<!-- SQL anti-patterns and optimizations. Assumes columnar formats (Parquet, Delta), Spark SQL / Databricks. Validate patterns with your data and cluster config. -->

# SQL Patterns — Anti-Patterns & Rewrites

Reference this file when optimizing SQL queries. Each entry includes the problem, why it hurts performance, and the correct rewrite.

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
**Why:** Columnar formats (Parquet, Delta) only read requested columns; `SELECT *` forces a full scan.

---

### 2. No partition filter
**Problem:** Querying a partitioned table without filtering on the partition column scans all partitions.
```sql
-- ❌ Bad
SELECT * FROM events WHERE user_id = 123;

-- ✅ Good
SELECT event_id, event_type, user_id
FROM events
WHERE event_date >= '2024-01-01'   -- partition column
  AND user_id = 123;
```
**Why:** Without the partition filter, Spark/Hive lists and reads every partition directory. Always filter on the partition key first.

---

### 3. Correlated subquery
**Problem:** Runs the subquery once per row of the outer query.
```sql
-- ❌ Bad
SELECT o.order_id,
       (SELECT SUM(amount) FROM payments p WHERE p.order_id = o.order_id) AS total_paid
FROM orders o;

-- ✅ Good
WITH payment_totals AS (
    SELECT order_id, SUM(amount) AS total_paid
    FROM payments
    GROUP BY order_id
)
SELECT o.order_id, pt.total_paid
FROM orders o
LEFT JOIN payment_totals pt ON o.order_id = pt.order_id;
```
**Why:** CTEs compute once; correlated subqueries execute N times (where N = row count).

---

### 4. OR instead of IN / UNION
**Problem:** `OR` on the same column prevents index use and is harder to optimize.
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

### 5. DISTINCT to fix bad joins (masking duplicates)
**Problem:** Using `DISTINCT` to hide the symptom of a fan-out join instead of fixing the join.
```sql
-- ❌ Bad (fan-out from one-to-many join, fixed with DISTINCT)
SELECT DISTINCT o.order_id, o.customer_id
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;

-- ✅ Good (pre-aggregate or use EXISTS)
SELECT o.order_id, o.customer_id
FROM orders o
WHERE EXISTS (
    SELECT 1 FROM order_items oi WHERE oi.order_id = o.order_id
);
```
**Why:** `DISTINCT` adds a sort/dedup stage; fix the join cardinality instead.

---

## 🟡 Medium Impact

### 6. NOT IN with NULLs
**Problem:** `NOT IN` returns no rows if the subquery contains any NULL values.
```sql
-- ❌ Bad (silent bug if subquery has NULLs)
SELECT * FROM orders
WHERE customer_id NOT IN (SELECT customer_id FROM blacklist);

-- ✅ Good
SELECT * FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM blacklist b
    WHERE b.customer_id = o.customer_id
);
```

---

### 7. Implicit type casting in WHERE
**Problem:** Casting the column in the filter prevents partition/index pruning.
```sql
-- ❌ Bad
SELECT * FROM events WHERE CAST(event_date AS STRING) = '2024-01-15';

-- ✅ Good
SELECT * FROM events WHERE event_date = DATE '2024-01-15';
```

---

### 8. Aggregation before join (push down aggregates)
**Problem:** Joining large tables then aggregating is more expensive than aggregating first.
```sql
-- ❌ Bad
SELECT c.country, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.country;

-- ✅ Good
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

### 9. ORDER BY without LIMIT on large tables
**Problem:** Forces a full sort of the entire dataset.
```sql
-- ❌ Bad
SELECT * FROM events ORDER BY created_at DESC;

-- ✅ Good (if you only need recent records)
SELECT event_id, event_type, created_at
FROM events
ORDER BY created_at DESC
LIMIT 1000;
```

---

### 10. LIKE with leading wildcard
**Problem:** `LIKE '%value'` cannot use any index or predicate pushdown.
```sql
-- ❌ Bad
SELECT * FROM customers WHERE email LIKE '%@gmail.com';

-- ✅ Good (if possible, restructure data or use a computed column)
-- Or use full-text search / suffix index if available in your engine
SELECT * FROM customers WHERE email_domain = 'gmail.com';
```

---

## 🟢 Low Impact / Best Practice

### 11. Use CTEs for readability and reuse
Break complex queries into named steps. The optimizer can often reuse the CTE result.

### 12. Avoid functions on filtered columns
`WHERE YEAR(created_at) = 2024` → `WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01'`

### 13. UNION vs UNION ALL
Use `UNION ALL` unless you explicitly need deduplication. `UNION` adds a sort/dedup step.

### 14. Avoid SELECT in SELECT (scalar subqueries in SELECT list)
Move them into a JOIN or CTE — scalar subqueries in the SELECT list execute per row.
