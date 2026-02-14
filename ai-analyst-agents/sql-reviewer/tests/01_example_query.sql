WITH sessions AS (
  SELECT user_id, session_id, event_ts
  FROM `project.dataset.events`
  WHERE event_ts >= TIMESTAMP('2026-01-01')
),
purchases AS (
  SELECT user_id, purchase_ts, amount
  FROM `project.dataset.purchases`
)
SELECT
  s.user_id,
  COUNT(DISTINCT s.session_id) AS sessions,
  SUM(p.amount) AS revenue
FROM sessions s
LEFT JOIN purchases p
  ON s.user_id = p.user_id
WHERE p.purchase_ts >= s.event_ts
GROUP BY 1;
