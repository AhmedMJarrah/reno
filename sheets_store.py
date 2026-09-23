"""
sheets_store.py — v1.0.0  (project: reno, volunteer portal)
The only module that talks to Google Sheets. Used by seed_sheets.py, portal_app.py and merge_back.py.

Spreadsheet layout (one spreadsheet, four worksheets):
  tasks      one row per task; payload split over payload_1..payload_N (a cell holds max 50,000 chars)
  answers    append-only; every save adds a row; the latest row per task_id is the valid answer
  users      username, display_name, role (volunteer/admin), active
  audit_log  every admin action (reassign, reopen) and every save, with timestamp and actor

Credentials: a service-account dict (Streamlit secrets) or a JSON key file path (scripts, .env GOOGLE_SA_JSON).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import gspread
import pandas as pd

TASK_COLS = ["task_id", "queue", "queue_title", "pmk_ID", "title", "assigned_to", "status", "updated_at"]
ANSWER_COLS = ["saved_at", "task_id", "queue", "pmk_ID", "username", "kind", "verdict", "answer_json"]
USER_COLS = ["username", "display_name", "role", "active"]
AUDIT_COLS = ["at", "actor", "action", "details"]
STATUSES = {"pending": "بانتظارك", "in_progress": "قيد العمل", "done": "منجزة"}


def now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


class Store:
    def __init__(self, sheet_name: str, sa_info: dict | None = None, sa_file: str | None = None):
        gc = gspread.service_account_from_dict(sa_info) if sa_info else gspread.service_account(filename=sa_file)
        self.sh = gc.open(sheet_name)

    # ---------------------------------------------------------------- structure
    def ws(self, name: str, header: list[str] | None = None, cols: int = 20):
        try:
            return self.sh.worksheet(name)
        except gspread.WorksheetNotFound:
            w = self.sh.add_worksheet(title=name, rows=1000, cols=max(cols, len(header or [])))
            if header:
                w.update([header], "A1")
            return w

    def ensure(self, payload_cols: int = 1):
        self.ws("tasks", TASK_COLS + [f"payload_{i + 1}" for i in range(payload_cols)], 30)
        self.ws("answers", ANSWER_COLS)
        self.ws("users", USER_COLS)
        self.ws("audit_log", AUDIT_COLS)

    # ---------------------------------------------------------------- reads
    def tasks(self) -> pd.DataFrame:
        vals = self.ws("tasks").get_all_values()
        if len(vals) < 2:
            return pd.DataFrame(columns=TASK_COLS)
        df = pd.DataFrame(vals[1:], columns=vals[0])
        df["_row"] = range(2, len(df) + 2)
        return df

    def answers(self) -> pd.DataFrame:
        vals = self.ws("answers").get_all_values()
        return pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame(columns=ANSWER_COLS)

    def users(self) -> pd.DataFrame:
        vals = self.ws("users").get_all_values()
        return pd.DataFrame(vals[1:], columns=vals[0]) if len(vals) > 1 else pd.DataFrame(columns=USER_COLS)

    @staticmethod
    def payload(row: pd.Series) -> dict:
        raw = "".join(str(row[c]) for c in sorted((c for c in row.index if str(c).startswith("payload_")),
                                                   key=lambda c: int(c.split("_")[1])))
        return json.loads(raw) if raw else {}

    # ---------------------------------------------------------------- writes
    def save_answer(self, task: pd.Series, username: str, kind: str, verdict: str, answer: dict):
        """kind: 'final' or 'draft'. Appends the answer, then updates the task status."""
        self.ws("answers").append_row(
            [now(), task.task_id, task.queue, task.pmk_ID, username, kind, verdict, json.dumps(answer, ensure_ascii=False)],
            value_input_option="RAW")
        self.set_status(task, "done" if kind == "final" else "in_progress")
        self.audit(username, f"save_{kind}", f"{task.task_id} verdict={verdict}")

    def set_status(self, task: pd.Series, status: str):
        w = self.ws("tasks")
        c_status = TASK_COLS.index("status") + 1
        w.update_cell(int(task._row), c_status, status)
        w.update_cell(int(task._row), c_status + 1, now())

    def reassign(self, rows: list[int], to_user: str, actor: str, note: str):
        w = self.ws("tasks")
        col = gspread.utils.rowcol_to_a1(1, TASK_COLS.index("assigned_to") + 1).rstrip("1")
        w.batch_update([{"range": f"{col}{r}", "values": [[to_user]]} for r in rows], value_input_option="RAW")
        self.audit(actor, "reassign", f"{len(rows)} tasks -> {to_user} | {note}")

    def audit(self, actor: str, action: str, details: str):
        self.ws("audit_log").append_row([now(), actor, action, details], value_input_option="RAW")
