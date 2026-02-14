## Summary (TL;DR)
- Verdict: Needs fixes
- Biggest risk: Incorrect retention calculation due to off-by-one day in D7 retention logic and potential missing activity on exact day 7.
- Quick win: Clarify and fix the D7 retention condition to correctly capture user activity on the 7th day after first_seen_date.

## A) Correctness & Edge Cases

- **D7 retention calculation off-by-one risk** (severity: High)  
  - Evidence: The condition `a.event_date = DATE_ADD(n.first_seen_date, INTERVAL 7 DAY)` checks for activity exactly 7 days after first_seen_date. Retention is often defined as activity on day 7 *inclusive*, i.e., 7 days after the first day (which is day 0). However, this depends on business definition. Also, if the user was active multiple times on day 7, MAX(CASE...) is fine, but if the user was active on day 6 or day 8, it won't count.  
  - Fix: Confirm with stakeholders if retention means activity on day 7 exactly or within a window (e.g., day 7 or day 7+1). If exact day 7, keep as is but clarify. If inclusive of day 7, consider `>=` and `<=` range or use `BETWEEN`. Also, consider timezone implications if event_ts is UTC but user timezone differs.

- **Join cardinality and duplication risk in `activity` CTE** (severity: Medium)  
  - Evidence: `activity` groups by user_id and event_date, so duplicates are removed. However, joining `events` to `new_users` on user_id only assumes user_id is unique in `new_users` (which it is). No duplication risk here.  
  - Fix: None needed.

- **Filter boundaries and timezones** (severity: Medium)  
  - Evidence: The query uses `DATE(event_ts)` which truncates timestamp to UTC date. Assumption states event_ts is UTC, so this is consistent. However, if users are in different timezones, this may misalign "day" boundaries for retention.  
  - Fix: Confirm if UTC day boundaries are acceptable for business logic.

- **Null handling and division by zero** (severity: Low)  
  - Evidence: SAFE_DIVIDE is used for retention calculation, which is good to avoid division by zero.  
  - Fix: None needed.

- **Missing users with no activity after first_seen_date** (severity: Medium)  
  - Evidence: `d7_flags` uses LEFT JOIN from `new_users` to `activity`, so users with no activity on day 7 get `is_retained_d7 = 0`. This is correct.  
  - Fix: None needed.

- **Date range filtering in `activity` CTE** (severity: Medium)  
  - Evidence: `activity` filters events between '2026-01-01' and '2026-02-15'. Since new_users are only from January, this covers at least 15 days after first_seen_date, which is enough for D7 retention.  
  - Fix: None needed.

## B) Performance & Cost (BigQuery)

- **Avoid SELECT *** (severity: Low)  
  - Evidence: The query explicitly selects columns, no SELECT * used. Good.

- **Partition pruning** (severity: Medium)  
  - Evidence: The base table `project.dataset.events` is filtered by event_ts date ranges in both `user_first_seen` and `activity` CTEs. However, `user_first_seen` scans all events without date filter, which can be expensive.  
  - Fix: If possible, add a date filter in `user_first_seen` to limit scanning, e.g., only events up to '2026-01-31' or a reasonable window before that. This reduces data scanned.

- **Early filtering** (severity: Medium)  
  - Evidence: Filtering in `user_first_seen` is done after aggregation, so no early filter.  
  - Fix: Add WHERE clause in `user_first_seen` to filter event_ts before aggregation.

- **No CROSS JOIN or unnecessary joins** (severity: Low)  
  - Evidence: Joins are appropriate and on keys.

- **Use of QUALIFY** (severity: Low)  
  - Evidence: Not applicable here.

## C) Readability & Maintainability

- **CTE naming** (severity: Low)  
  - Evidence: Names like `user_first_seen`, `new_users`, `activity`, `d7_flags` are descriptive and consistent. Good.

- **Consistent aliasing** (severity: Low)  
  - Evidence: Aliases `e`, `n`, `a` are used consistently.

- **Comments** (severity: Medium)  
  - Evidence: Only header comments exist. The logic inside CTEs could benefit from inline comments, especially for the D7 retention logic and date filters.

- **Explicit column naming** (severity: Low)  
  - Evidence: Good practice followed.

## D) Analytical Clarity

**What this query returns:**  
Daily counts of new users (first seen in January 2026) and their Day 7 retention rate, defined as the proportion of those users who had activity exactly 7 days after their first seen date.

**Primary metrics:**  
- `new_users`: count of users first seen on each date  
- `retained_users_d7`: count of those users active on day 7 after first_seen_date  
- `retention_d7`: ratio of retained_users_d7 to new_users

**Assumptions:**  
- event_ts timestamps are in UTC and day boundaries are UTC days  
- Retention is defined as activity exactly on day 7 after first_seen_date  
- Users are uniquely identified by user_id  
- Events table is large and partitioned by event_ts date

**Top risks:**  
1. Misinterpretation of retention day boundary (off-by-one or timezone issues)  
2. Scanning entire events table in `user_first_seen` without date filter, leading to high cost  
3. Potential missing activity if users have no events exactly on day 7 but are active nearby

**Questions to ask the stakeholder:**  
- How is Day 7 retention defined exactly? Is it activity on the 7th day after first_seen_date, or within a window?  
- Are UTC day boundaries acceptable for defining days, or should user local timezones be considered?  
- Is it acceptable to only consider users first seen in January 2026? Should the first_seen_date filter be dynamic?  
- Is the events table partitioned by event_ts date? Can we filter early to reduce scanned data?

## Suggested revised SQL (optional)

Assuming retention means activity on day 7 exactly, and we want to reduce scanned data in `user_first_seen`:

```sql
WITH user_first_seen AS (
  SELECT
    user_id,
    DATE(MIN(event_ts)) AS first_seen_date
  FROM `project.dataset.events`
  WHERE event_ts < '2026-02-01'  -- filter events up to end of Jan to reduce scan
  GROUP BY user_id
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
  GROUP BY e.user_id, event_date
),

d7_flags AS (
  SELECT
    n.first_seen_date,
    n.user_id,
    MAX(CASE WHEN a.event_date = DATE_ADD(n.first_seen_date, INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS is_retained_d7
  FROM new_users n
  LEFT JOIN activity a
    ON a.user_id = n.user_id
  GROUP BY n.first_seen_date, n.user_id
)

SELECT
  first_seen_date,
  COUNT(*) AS new_users,
  SUM(is_retained_d7) AS retained_users_d7,
  SAFE_DIVIDE(SUM(is_retained_d7), COUNT(*)) AS retention_d7
FROM d7_flags
GROUP BY first_seen_date
ORDER BY first_seen_date;
```

If retention definition changes, e.g., activity on day 7 or day 8, adjust the CASE condition accordingly:

```sql
MAX(CASE WHEN a.event_date BETWEEN DATE_ADD(n.first_seen_date, INTERVAL 7 DAY)
                               AND DATE_ADD(n.first_seen_date, INTERVAL 8 DAY)
         THEN 1 ELSE 0 END) AS is_retained_d7
```

Add comments inside CTEs to clarify logic for future maintainers.