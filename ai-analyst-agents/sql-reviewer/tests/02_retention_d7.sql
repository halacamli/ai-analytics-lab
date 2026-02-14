-- Goal: Revenue per active user per day
-- Context: Mobile game analytics
-- Assumptions: event_ts is UTC


WITH user_first_seen AS (
  SELECT
    user_id,
    DATE(MIN(event_ts)) AS first_seen_date
  FROM `project.dataset.events`
  GROUP BY 1
),

new_users AS (
  SELECT
    user_id,
    first_seen_date
  FROM user_first_seen
  WHERE first_seen_date BETWEEN DATE('2026-01-01') AND DATE('2026-01-31')
),

activity AS (
  SELECT
    e.user_id,
    DATE(e.event_ts) AS event_date
  FROM `project.dataset.events` e
  JOIN new_users n
    ON e.user_id = n.user_id
  WHERE DATE(e.event_ts) BETWEEN DATE('2026-01-01') AND DATE('2026-02-15')
  GROUP BY 1, 2
),

d7_flags AS (
  SELECT
    n.first_seen_date,
    n.user_id,
    MAX(CASE WHEN a.event_date = DATE_ADD(n.first_seen_date, INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS is_retained_d7
  FROM new_users n
  LEFT JOIN activity a
    ON a.user_id = n.user_id
  GROUP BY 1, 2
)

SELECT
  first_seen_date,
  COUNT(*) AS new_users,
  SUM(is_retained_d7) AS retained_users_d7,
  SAFE_DIVIDE(SUM(is_retained_d7), COUNT(*)) AS retention_d7
FROM d7_flags
GROUP BY 1
ORDER BY 1;
