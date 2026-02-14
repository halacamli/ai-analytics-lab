-- Goal: Revenue per active user per day
-- Note: This query is intentionally flawed for testing the reviewer

WITH daily_active AS (
  SELECT
    DATE(event_ts) AS dt,
    user_id
  FROM `project.dataset.events`
  WHERE event_name IN ('session_start', 'app_open')
    AND event_ts >= TIMESTAMP('2026-01-01')
    AND event_ts < TIMESTAMP('2026-02-01')
),

daily_purchases AS (
  SELECT
    user_id,
    DATE(purchase_ts) AS dt,
    amount
  FROM `project.dataset.purchases`
  WHERE purchase_ts >= TIMESTAMP('2026-01-01')
    AND purchase_ts < TIMESTAMP('2026-02-01')
),

users AS (
  SELECT
    user_id,
    country
  FROM `project.dataset.users`
)

SELECT
  a.dt,
  u.country,
  COUNT(DISTINCT a.user_id) AS dau,
  SUM(p.amount) AS revenue,
  SUM(p.amount) / COUNT(DISTINCT a.user_id) AS revenue_per_user
FROM daily_active a
LEFT JOIN daily_purchases p
  ON a.user_id = p.user_id           -- ❌ missing dt join -> duplicates revenue across days
JOIN users u
  ON a.user_id = u.user_id
WHERE u.country != 'Unknown'          -- ❌ this turns JOIN into an inner-like filter for some cases
GROUP BY 1, 2
ORDER BY 1, 2;
