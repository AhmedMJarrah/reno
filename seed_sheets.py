"""
seed_sheets.py — v1.0.0  (project: reno, volunteer portal)
Loads the tasks built by build_queues.py into the Google Sheet and writes the users sheet.

Safety:
  --init   first load. Refuses if the tasks sheet already has tasks (use --force to wipe and reload).
  --add    adds only task_ids that are not in the sheet yet (never touches existing tasks or answers).

Usage (CMD):
  py seed_sheets.py --init
  py seed_sheets.py --add --tasks outputs\\queues_20260923_112904\\tasks.csv
Config (.env): GOOGLE_SA_JSON, SHEET_NAME, VOLUNTEERS=v1,...,v5, ADMIN_USER=admin
Display names (optional, .env): DISPLAY_NAMES=v1:نولا,v2:لين
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from sheets_store import TASK_COLS, USER_COLS, Store

VERSION = "1.0.0"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
log = logging.getLogger("seed_sheets")


def main():
    from dotenv import load_dotenv
    load_dotenv()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--init", action="store_true")
    mode.add_argument("--add", action="store_true")
    ap.add_argument("--force", action="store_true", help="with --init: wipe existing tasks/answers first")
    ap.add_argument("--tasks", help="tasks.csv (default: latest outputs\\queues_*)")
    a = ap.parse_args()

    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                        handlers=[logging.FileHandler(f"logs/seed_sheets_{TS}.log", encoding="utf-8"), logging.StreamHandler()])
    path = Path(a.tasks) if a.tasks else sorted(Path("outputs").glob("queues_*"))[-1] / "tasks.csv"
    T = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    pcols = [c for c in T.columns if c.startswith("payload_")]
    log.info("seed_sheets v%s | tasks=%s (%d rows, %d payload cols)", VERSION, path, len(T), len(pcols))

    st = Store(os.environ["SHEET_NAME"], sa_file=os.environ["GOOGLE_SA_JSON"])
    st.ensure(len(pcols))
    ws = st.ws("tasks")
    existing = st.tasks()

    if a.init:
        if len(existing) and not a.force:
            raise SystemExit(f"tasks sheet already has {len(existing)} tasks. Use --add, or --init --force to wipe.")
        if a.force:
            log.warning("wiping tasks and answers")
            ws.clear()
            st.ws("answers").clear()
            st.ensure(len(pcols))
            st.ws("answers").update([["saved_at", "task_id", "queue", "pmk_ID", "username", "kind", "verdict", "answer_json"]], "A1")
        header = TASK_COLS + pcols
        ws.resize(rows=len(T) + 10, cols=len(header))
        ws.update([header] + T[header].values.tolist(), "A1", value_input_option="RAW")
        new = T
    else:
        new = T[~T.task_id.isin(existing.task_id)]
        header = ws.row_values(1)
        missing = [c for c in pcols if c not in header]
        if missing:
            raise SystemExit(f"sheet has fewer payload columns than the new tasks need: {missing}")
        if len(new):
            ws.append_rows(new.reindex(columns=header, fill_value="").values.tolist(), value_input_option="RAW")

    # users sheet (display names are optional)
    vols = [u.strip() for u in os.getenv("VOLUNTEERS", "v1,v2,v3,v4,v5").split(",") if u.strip()]
    names = dict(x.split(":", 1) for x in os.getenv("DISPLAY_NAMES", "").split(",") if ":" in x)
    admin = os.getenv("ADMIN_USER", "admin")
    users = [[u, names.get(u, u), "volunteer", "TRUE"] for u in vols] + [[admin, names.get(admin, "المشرف"), "admin", "TRUE"]]
    uw = st.ws("users")
    uw.clear()
    uw.update([USER_COLS] + users, "A1")

    st.audit("seed_sheets", "init" if a.init else "add", f"{len(new)} tasks from {path.name}")
    log.info("done: %d tasks written | per queue/user:\n%s", len(new),
             pd.crosstab(new.queue, new.assigned_to).to_string() if len(new) else "-")


if __name__ == "__main__":
    main()
