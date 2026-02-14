## ✨ AI-powered SQL Review Tool

![SQL Reviewer Demo](ai-analyst-agents/sql-reviewer/sql_reviewer_demo.gif)

# AI SQL Reviewer Agent

An AI-powered tool that reviews SQL queries for correctness, performance, and analytical risks.

This project uses OpenAI models to act like a senior analytics reviewer and generate structured feedback for SQL queries.

---

## What it does

- Reviews SQL queries automatically
- Detects logical and analytical risks
- Suggests improvements
- Produces a structured `.review.md` file

---

## Providing Context (Recommended)

The reviewer can automatically use context written as SQL comments at the top of the file.

Adding structured comments helps the AI produce more accurate and less generic reviews.

Example:

```sql
-- Goal: Calculate D7 retention for new users
-- Context: Mobile game product analytics dataset
-- Assumptions: event_ts is stored in UTC

SELECT ...
```

The agent will read these comments and incorporate them into the analytical review automatically.

Recommended fields:
- Goal → What business question the query answers
- Context → Dataset or product area
- Assumptions → Timezone, filtering logic, or metric definitions

---

## How to run

### 1. Create virtual environment

```bash
cd ai-analyst-agents/sql-reviewer/cli
python3 -m venv .venv
source .venv/bin/activate 

``` 
### 2. Install dependencies
```bash
pip install -r requirements.txt
``` 

### 3. Add API key

Create a `.env` file inside the `cli` folder:

```env
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4.1-mini
```

### 4. Run reviewer
```bash
python reviewer.py --input ../tests
``` 

The tool will generate `.review.md` files next to SQL files.

---

## Example

Input:
```md
tests/01_example_query.sql
```

Output:
```md
tests/01_example_query.review.md
```

