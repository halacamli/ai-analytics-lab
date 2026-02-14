import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def extract_header_context(sql: str, max_lines: int = 25) -> str:
    """
    Extracts leading SQL comments (--) from the top of the file to use as context.
    Stops when the first non-comment, non-empty line appears.
    """
    lines = sql.splitlines()
    ctx_lines = []
    for line in lines[:max_lines]:
        stripped = line.strip()
        if stripped == "":
            # allow empty lines in the header
            continue
        if stripped.startswith("--"):
            ctx_lines.append(stripped[2:].strip())
            continue
        # stop at first real SQL line
        break
    return "\n".join(ctx_lines).strip()

def build_prompt(system_md: str, reviewer_md: str, output_schema_md: str, sql: str) -> tuple[str, str]:
    system = system_md.strip()

    header_ctx = extract_header_context(sql)
    context_block = ""
    if header_ctx:
        context_block = f"### Context from SQL file header\n{header_ctx}\n\n"

    user = f"""{reviewer_md.strip()}

{output_schema_md.strip()}

{context_block}### SQL
```sql
{sql.strip()}
```"""
    return system, user


def review_one_sql(client: OpenAI, model: str, system: str, user: str) -> str:
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content


def main():
    parser = argparse.ArgumentParser(description="AI SQL Reviewer Agent (CLI)")
    parser.add_argument("--input", required=True, help="Input .sql file or directory containing .sql files")
    parser.add_argument("--out", required=False, help="Output directory for .review.md files (default: alongside inputs)")
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"), help="OpenAI model name")
    parser.add_argument("--prompts", default="../prompts", help="Path to prompts directory (system.md, reviewer.md, output_schema.md)")
    args = parser.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing OPENAI_API_KEY. Set it in your environment or a .env file.")

    prompts_dir = Path(args.prompts).resolve()
    system_md = read_text(prompts_dir / "system.md")
    reviewer_md = read_text(prompts_dir / "reviewer.md")
    output_schema_md = read_text(prompts_dir / "output_schema.md")

    input_path = Path(args.input).resolve()
    out_dir = Path(args.out).resolve() if args.out else None

    client = OpenAI(api_key=api_key)

    sql_files = []
    if input_path.is_dir():
        sql_files = sorted(input_path.glob("*.sql"))
    elif input_path.is_file() and input_path.suffix.lower() == ".sql":
        sql_files = [input_path]
    else:
        raise SystemExit("Input must be a .sql file or a directory containing .sql files.")

    if not sql_files:
        raise SystemExit("No .sql files found.")

    for sql_path in sql_files:
        sql = read_text(sql_path)
        system, user = build_prompt(system_md, reviewer_md, output_schema_md, sql)

        print(f"Reviewing: {sql_path.name} ...")
        review_md = review_one_sql(client, args.model, system, user)

        if out_dir:
            out_path = out_dir / f"{sql_path.stem}.review.md"
        else:
            out_path = sql_path.with_suffix(".review.md")

        write_text(out_path, review_md)
        print(f"Saved: {out_path}")

    print("Done.")


if __name__ == "__main__":
    main()
