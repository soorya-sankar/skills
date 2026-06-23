# SQL Patterns — Anti-Patterns & Rewrites

Reference this file when optimizing SQL queries. Each pattern includes the problem, why it hurts, and the correct rewrite. Patterns apply to Spark SQL, Databricks SQL, and standard SQL engines.

---

## 🔴 High Impact

### 1. SELECT * (column pruning)
**Problem:** Reads all columns from disk even if only a few are needed.
```sql
-- ❌ Bad
SELECT * FROM orders WHERE status = 'PENDING';

-- ✅ Good
SELECT order_id, customer_id, created_at, amount
FROM orders
WHERE status = 'PENDING';
```
**Why:** Columnar storage (Parquet, Delta) reads only the requested columns. `SELECT *` forces a full scan of every column on disk.

---

### 2. No partition filter
**Problem:** Querying a partitioned table without a partition column filter scans every partition.
```sql
-- ❌ Bad — scans all partitions to find user_id = 123
SELECT * FROM events WHERE user_id = 123;

-- ✅ Good — partition filter applied first, then row filter
SELECT event_id, event_type, user_id
FROM events
WHERE event_date >= '2024-01-01'    -- partition column goes first
  AND user_id = 123;
```
**Why:** Without the partition filter, Spark lists and reads every partition directory. Always filter on the partition key first.

---

### 3. Predicate pushdown — filter before joining
**Problem:** Filtering after a join processes far more rows than necessary.
```sql
-- ❌ Bad — joins all rows, then filters
SELECT o.order_id, c.country, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.status = 'COMPLETED'
  AND o.created_at >= '2024-01-01';

-- ✅ Good — filter orders down before the join
WITH recent_orders AS (
    SELECT order_id, customer_id, amount
    FROM orders
    WHERE status = 'COMPLETED'
      AND created_at >= '2024-01-01'  -- pushdown: filter first
)
SELECT ro.order_id, c.country, ro.amount
FROM recent_orders ro
JOIN customers c ON ro.customer_id = c.customer_id;
```
**Why:** The smaller the input to a join, the less data gets shuffled across the network. Filter as early as possible.

---

### 4. Correlated subquery
**Problem:** The subquery runs once per row of the outer query — N million rows = N million subquery executions.
```sql
-- ❌ Bad — subquery runs once per order row
SELECT o.order_id,
       (SELECT SUM(amount) FROM payments p WHERE p.order_id = o.order_id) AS total_paid
FROM orders o;

-- ✅ Good — aggregate once, then join
WITH payment_totals AS (
    SELECT order_id, SUM(amount) AS total_paid
    FROM payments
    GROUP BY order_id
)
SELECT o.order_id, pt.total_paid
FROM orders o
LEFT JOIN payment_totals pt ON o.order_id = pt.order_id;
```
**Why:** The CTE computes one aggregation pass. The correlated subquery re-runs for every row.

---

### 5. Window functions vs GROUP BY — use the right tool
**Problem:** Using GROUP BY when you need row-level detail alongside an aggregate forces a self-join.
```sql
-- ❌ Bad — GROUP BY loses row-level detail; requires self-join to get it back
SELECT o.order_id, o.amount, totals.customer_total
FROM orders o
JOIN (
    SELECT customer_id, SUM(amount) AS customer_total
    FROM orders
    GROUP BY customer_id
) totals ON o.customer_id = totals.customer_id;

-- ✅ Good — window function keeps all rows AND adds the aggregate
SELECT
    order_id,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id) AS customer_total,
    RANK()      OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rank_by_amount
FROM orders;
```
**Why:** Window functions compute the aggregate without collapsing rows. No self-join needed.

---

### 6. DISTINCT to fix bad joins (masking duplicates)
**Problem:** Using `DISTINCT` to hide fan-out duplicates from a one-to-many join instead of fixing the join.
```sql
-- ❌ Bad — DISTINCT forces a full sort/dedup, symptom not fixed
SELECT DISTINCT o.order_id, o.customer_id
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;

-- ✅ Good — use EXISTS if you only need to check presence
SELECT o.order_id, o.customer_id
FROM orders o
WHERE EXISTS (
    SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id
);
```

---

### 7. Aggregation before join — push aggregates down
**Problem:** Joining large tables then aggregating is more expensive than aggregating first.
```sql
-- ❌ Bad — joins all customer rows, then aggregates
SELECT c.country, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.country;

-- ✅ Good — aggregate orders first, then join the smaller result
WITH customer_totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
)
SELECT c.country, SUM(ct.total)
FROM customer_totals ct
JOIN customers c ON ct.customer_id = c.customer_id
GROUP BY c.country;
```

---

## 🟡 Medium Impact

### 8. NOT IN with NULLs — silent correctness bug
**Problem:** `NOT IN` returns zero rows if the subquery contains any NULL value. This is a correctness bug, not just a performance issue.
```sql
-- ❌ Bad — returns no rows if blacklist has any NULLs
SELECT * FROM orders
WHERE customer_id NOT IN (SELECT customer_id FROM blacklist);

-- ✅ Good — NOT EXISTS handles NULLs correctly
SELECT * FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM blacklist b
    WHERE b.customer_id = o.customer_id
);
```

---

### 9. Implicit type casting in WHERE
**Problem:** Wrapping the column in a function or cast prevents the engine from using partition pruning or indexes.
```sql
-- ❌ Bad — CAST on the column prevents pushdown
SELECT * FROM events WHERE CAST(event_date AS STRING) = '2024-01-15';

-- ❌ Also bad — function on column prevents pruning
SELECT * FROM events WHERE YEAR(created_at) = 2024;

-- ✅ Good — filter on the column directly
SELECT * FROM events WHERE event_date = DATE '2024-01-15';
SELECT * FROM events WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01';
```

---

### 10. OR instead of IN
**Problem:** `OR` on the same column is harder for optimizers to rewrite and prevents predicate simplification.
```sql
-- ❌ Bad
SELECT * FROM products
WHERE category = 'Electronics'
   OR category = 'Appliances'
   OR category = 'Furniture';

-- ✅ Good
SELECT product_id, product_name, price
FROM products
WHERE category IN ('Electronics', 'Appliances', 'Furniture');
```

---

### 11. ORDER BY without LIMIT on large tables
**Problem:** Forces a full sort of the entire dataset before returning any rows.
```sql
-- ❌ Bad — sorts everything
SELECT * FROM events ORDER BY created_at DESC;

-- ✅ Good — sort only what you need
SELECT event_id, event_type, created_at
FROM events
ORDER BY created_at DESC
LIMIT 1000;
```

---

### 12. CTEs vs subqueries in FROM — know the difference
CTEs improve readability and Spark can often reuse the result. However, some engines materialize CTEs even when referenced once, adding overhead. If a CTE is used exactly once and is simple, a subquery in `FROM` may be faster.

```sql
-- CTE (preferred for readability and reuse)
WITH filtered AS (
    SELECT * FROM orders WHERE status = 'ACTIVE'
)
SELECT * FROM filtered WHERE amount > 100;

-- Subquery in FROM (may be faster for single-use, engine-dependent)
SELECT * FROM (
    SELECT * FROM orders WHERE status = 'ACTIVE'
) sub
WHERE sub.amount > 100;
```
**Rule:** Use CTEs when a result is referenced 2+ times. For single-use, test both and check the query plan.

---

### 13. Bucketing — pre-partition for frequent joins (Spark SQL)
**Problem:** The same join runs repeatedly on the same large tables, shuffling both sides every time.
```sql
-- Pre-bucket tables that are frequently joined on the same key
-- Run once at table creation:
CREATE TABLE orders_bucketed
USING DELTA
CLUSTERED BY (customer_id) INTO 256 BUCKETS
AS SELECT * FROM orders;

CREATE TABLE customers_bucketed
USING DELTA
CLUSTERED BY (customer_id) INTO 256 BUCKETS
AS SELECT * FROM customers;

-- Subsequent joins skip the shuffle entirely
SELECT o.order_id, c.country
FROM orders_bucketed o
JOIN customers_bucketed c ON o.customer_id = c.customer_id;
```
**Use when:** The same join runs daily/hourly on stable, large tables. Not worth the setup cost for ad-hoc queries.

---

## 🟢 Best Practices

### 14. UNION ALL over UNION unless dedup is needed
`UNION` adds a sort and dedup stage. `UNION ALL` is almost always what you want.

### 15. LIKE with leading wildcard — avoid or restructure
`LIKE '%value'` scans every row — no pushdown possible. If this pattern is frequent, add a computed column for the extracted value.

### 16. Avoid scalar subqueries in SELECT list
```sql
-- ❌ Bad — executes once per row
SELECT o.order_id,
       (SELECT name FROM customers WHERE id = o.customer_id) AS customer_name
FROM orders o;

-- ✅ Good — one join instead
SELECT o.order_id, c.name AS customer_name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id;
```
