Review the SQL query provided.

Rules:
- Be specific: point to exact clauses/CTEs and explain why they may be wrong or risky.
- Prefer correctness over style.
- If business logic is unclear, list targeted questions a stakeholder/reviewer would ask.
- Provide improved SQL snippets ONLY when you are confident; otherwise propose options.
- Assume BigQuery unless otherwise specified.

Review dimensions:
A) Correctness & edge cases
- Join cardinality, duplication, missing keys
- Filters and where-clauses (AND/OR precedence)
- Time boundaries (inclusive/exclusive), timezone assumptions
- Null handling, division by zero, SAFE_CAST/SAFE_DIVIDE when relevant
- Window functions: partitions, ordering, ties

B) Performance & cost (BigQuery)
- Partition pruning, clustering usage (if relevant)
- Avoid SELECT *
- Avoid CROSS JOIN unless necessary
- Use QUALIFY appropriately
- Reduce data scanned (filter early, limit columns)

C) Readability & maintainability
- CTE naming, consistent aliasing
- Comments where logic is non-obvious

D) Analytical clarity (the “so what”)
- In 2–3 sentences: what the query returns
- Primary metric(s) produced
- Key assumptions
- Top 3 risks that could mislead a decision
