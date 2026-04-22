# Phase 2 Scripts

Order:
1. `01_seed_mule_inflows.py`
2. `02a_build_session_schedule.py`
3. `02b_inject_ato_sessions.py`
4. `02c_generate_transactions.py`
5. `03_simulate_profile_changes.py`
6. `04_simulate_payee_additions.py`
7. `05_compute_features.py`
8. `06_compute_ato_scores.py`
9. `07_stamp_labels.py`

All scripts use:
- `config/db.json` for PostgreSQL connection
- `run_log` for run tracking
- `scripts/phase2/common.py` for shared helpers and schema guard rails

Run each script from repository root.
