# Bank-data-gen

Phase 1 pipeline scripts for generating, validating, enriching, linking, and loading 40,000 synthetic banking agents.

## Scripts

1. `scripts/00_create_db_and_tables.py` – Create PostgreSQL database (if needed) and required tables/indexes.
2. `scripts/01_build_batches.py` – Build OpenAI Batch API JSONL requests and manifest.
3. `scripts/02_submit_batches.py` – Submit/poll/download OpenAI batch jobs.
4. `scripts/03_parse_responses.py` – Parse model responses into raw agent objects.
5. `scripts/04_validate_agents.py` – Validate agent schema/rules and segment counts.
6. `scripts/05_enrich_agents.py` – Add deterministic IDs and system-generated fields.
7. `scripts/06_build_links.py` – Build beneficiary graph with rule-based link generation.
8. `scripts/07_load_agents_to_db.py` – Bulk load enriched agents to PostgreSQL.
9. `scripts/08_load_links_to_db.py` – Bulk load link graph with FK validation.

## Expected configuration files

- `config/agent_segments.json`
- `config/city_pool.json`
- `config/openai.json`
- `config/bank_pool.json`
- `config/db.json`

## Notes

- Intermediate artifacts are written under `data/temp/`.
- Scripts are designed for safe restart from produced artifacts.
