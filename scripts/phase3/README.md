# Phase 3 Exports

Run in order:
1. `python scripts/phase3/01_export_transactions.py`
2. `python scripts/phase3/02_export_agents.py`
3. `python scripts/phase3/03_export_suspicious_subset.py`

Output directory: `data/exports/phase3/`

## Export-only field renames

Internal PostgreSQL fields stay unchanged. Renames are done only in `SELECT ... AS ...` aliases:

- `agent_id` -> `customer_id`
- `sender_agent_id` -> `sender_customer_id`
- `receiver_agent_id` -> `receiver_customer_id`
- `new_payee_agent_id` -> `new_payee_customer_id`
- `agent_risk_tier` -> `customer_risk_tier`
