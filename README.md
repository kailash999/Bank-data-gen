# Bank-data-gen

Phase 1 pipeline for generating 40,000 agents, building beneficiary links, and loading both datasets into PostgreSQL.

## Pipeline scripts

- `scripts/00_create_db_and_tables.py`
- `scripts/01_build_batches.py`
- `scripts/02_submit_batches.py`
- `scripts/03_parse_responses.py`
- `scripts/04_validate_agents.py`
- `scripts/05_enrich_agents.py`
- `scripts/06_build_links.py`
- `scripts/07_load_agents_to_db.py`
- `scripts/08_load_links_to_db.py`

## Prompt templates

`scripts/01_build_batches.py` reads per-segment prompt templates from `prompts/<segment>.txt`.
Run the script from any working directory; template paths are resolved from the repository root.

## Configs

All runtime configuration is in `config/`.
Environment-backed secrets use `ENV:<VAR_NAME>` and are resolved at runtime.

## Database schema

Run `scripts/db/create_tables.sql` once before running load scripts.


Run scripts from repo root (for example: `python scripts/00_create_db_and_tables.py`).
