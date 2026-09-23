"""
build_queues.py — v1.0.0  (project: reno, volunteer portal, phase 1)
Turns the reno outputs into self-contained volunteer tasks, split between the volunteers.

Phase-1 queues (priority order):
  reflection      الانعكاسات                 -> only the users in REFLECTION_USERS (e.g. v1,v2)
  end_date        تاريخ انتهاء السريان        -> all volunteers
  number_year     رقم/سنة التشريع بعد 1960    -> all volunteers (one task per group)
  articles        المواد الناقصة              -> all volunteers

Every task carries everything the volunteer needs (names, numbers, gazette issue/page/date,
the texts side by side) in a JSON payload, so the portal never has to look anything up.

Usage (CMD):
  py build_queues.py                      # latest outputs\\run_* folder
  py build_queues.py --run outputs\\run_20260923_111245
Config (.env):  VOLUNTEERS=v1,v2,v3,v4,v5   REFLECTION_USERS=v1,v2
Output: outputs\\queues_<timestamp>\\tasks.csv  (+ summary in logs\\)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

VERSION = "1.0.0"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
log = logging.getLogger("build_queues")
MAX_FIELD = 60000  # per text field
CHUNK = 45000      # Google Sheets cell limit is 50,000 chars
MAX_CHUNKS = 8

QUEUE_TITLES = {
    "reflection": "الانعكاسات",
    "end_date": "تاريخ انتهاء السريان",
    "number_year": "رقم وسنة التشريع (بعد 1960)",
    "articles": "المواد الناقصة",
}
KIND_AR = {
    "same_legislation_recorded_twice": "يبدو أن التشريع نفسه مسجل أكثر من مرة",
    "different_laws_one_has_wrong_number_or_year": "تشريعات مختلفة تحمل الرقم والسنة نفسيهما: رقم أو سنة أحدها خطأ",
    "amendment_carries_its_base_number_year": "تعديل يحمل رقم وسنة قانونه الأصلي",
    "same_modleg_but_different_laws": "سجلان مختلفان مربوطان بالتعديل نفسه في الديوان",
}


def read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def one(run: Path, pattern: str) -> Path:
    hits = sorted(run.glob(pattern))
    if not hits:
        raise FileNotFoundError(f"{pattern} not found in {run}")
    return hits[-1]


def cut(t: str) -> str:
    t = str(t or "")
    return t if len(t) <= MAX_FIELD else t[:MAX_FIELD] + "\n… [النص أطول من ذلك – راجع المصدر]"


def gazette(r) -> dict:
    return {"number": r.get("Magazine_Number", ""), "page": r.get("Magazine_Page", ""), "date": r.get("Magazine_Date", "")}


def split_even(task_ids: list[str], users: list[str]) -> dict[str, str]:
    """Round-robin -> counts differ by at most one; isolated (each task has exactly one owner)."""
    return {t: users[i % len(users)] for i, t in enumerate(task_ids)}


def build(run: Path, users: list[str], refl_users: list[str], out: Path) -> pd.DataFrame:
    K = read(one(run, "laws_kg_clean_*.csv")).set_index("pmk_ID", drop=False)
    data = json.loads(one(run, "master_clean_*.json").read_text(encoding="utf-8"))
    by_uid = {}
    for top in data:
        by_uid[top["leg_uid"]] = top
        for m in top.get("Mod_Legs", []):
            by_uid[m["leg_uid"]] = m

    def rec(p):
        u = K.at[p, "json_leg_uid"] if p in K.index else ""
        return by_uid.get(u, {}) if u else {}

    def arts(lst):
        return {str(a.get("article_number", "")).strip(): a.get("text", "") for a in (lst or [])}

    def previous_version(p):
        """Text of the law just before amendment p: previous amendment's snapshot, else the base text."""
        cid = K.at[p, "chain_id"]
        chain = K[K.chain_id == cid].sort_values("chain_position", key=lambda s: s.astype(int))
        before = [q for q in chain.pmk_ID if int(K.at[q, "chain_position"]) < int(K.at[p, "chain_position"])]
        for q in reversed(before):
            r = rec(q)
            if q != cid and r.get("Reflected_Articles"):
                return arts(r["Reflected_Articles"])
            if q == cid:
                return arts(r.get("Base_Articles"))
        return {}

    def meta(p) -> dict:
        r = K.loc[p]
        return {"pmk_ID": p, "name": r.Leg_Name, "type": "تعديل" if r.record_type == "amendment" else "قانون أصلي",
                "number": r.Leg_Number, "year": r.Year, "status": r.Status, "gazette": gazette(r),
                "active_date": r.Active_Date, "end_date": r.End_Date,
                "base_pmk_ID": r.parent_pmk_ID, "base_name": K.at[r.parent_pmk_ID, "Leg_Name"] if r.parent_pmk_ID in K.index else ""}

    tasks = []

    # ---- 1) reflections ------------------------------------------------------------------------
    q = read(one(run, "review_VOLUNTEERS_reflection_queue_*.csv"))
    for _, r in q.iterrows():
        p = r.pmk_ID
        own = rec(p).get("Base_Articles") or []
        prev, snap = previous_version(p), arts(rec(p).get("Reflected_Articles"))
        targets = [a for a in r.articles_to_check.split("|") if a] or []
        payload = {
            **meta(p),
            "problem": r.problem, "reviewer_note": r.reviewer_note,
            "amendment_articles": [{"n": str(a.get("article_number", "")), "text": cut(a.get("text", ""))} for a in own],
            "articles": [{"n": n, "previous": cut(prev.get(n, "")), "current": cut(snap.get(n, ""))} for n in targets],
            "position_in_chain": r.position_in_chain,
        }
        tasks.append({"queue": "reflection", "pmk_ID": p, "title": r.amendment_name, "payload": payload})

    # ---- 2) missing end date -------------------------------------------------------------------
    q = read(one(run, "review_VOLUNTEERS_missing_end_date_*.csv"))
    for _, r in q.iterrows():
        p = r.pmk_ID
        chain = K[K.chain_id == K.at[p, "chain_id"]].sort_values("chain_position", key=lambda s: s.astype(int))
        payload = {**meta(p), "suggested_end_date": r.suggested_end_date, "suggestion_basis": r.suggestion_basis,
                   "canceled_by": r.Canceled_By, "replaced_by": r.Replaced_By,
                   "chain": [{"pmk_ID": c.pmk_ID, "name": c.Leg_Name, "number": c.Leg_Number, "year": c.Year,
                              "gazette_date": c.Magazine_Date, "end_date": c.End_Date, "status": c.Status,
                              "this": c.pmk_ID == p} for c in chain.itertuples()][:40]}
        tasks.append({"queue": "end_date", "pmk_ID": p, "title": r.Leg_Name, "payload": payload})

    # ---- 3) number / year after 1960 (one task per group) --------------------------------------
    q = read(one(run, "review_dup_number_year_after_1960_URGENT_*.csv"))
    for (n, y), g in q.groupby(["Leg_Number", "Year"], sort=False):
        rows = [{"pmk_ID": r.pmk_ID, "name": r.Leg_Name, "type": "تعديل" if r.record_type == "amendment" else "قانون أصلي",
                 "gazette": {"number": r.Magazine_Number, "page": r.Magazine_Page, "date": r.Magazine_Date},
                 "diwan_number": r.diwan_number, "diwan_year": r.diwan_year, "own_title_year": r.own_title_year,
                 "base_name": K.at[r.chain_id, "Leg_Name"] if r.chain_id in K.index and r.chain_id != r.pmk_ID else ""}
                for r in g.itertuples()]
        kind = g.group_kind.iloc[0]
        payload = {"number": n, "year": y, "kind": kind, "kind_ar": KIND_AR.get(kind, kind), "rows": rows}
        tasks.append({"queue": "number_year", "pmk_ID": "|".join(g.pmk_ID), "title": f"الرقم {n} لسنة {y}", "payload": payload})

    # ---- 4) missing articles -------------------------------------------------------------------
    q = read(one(run, "review_VOLUNTEERS_missing_articles_*.csv"))
    for _, r in q.iterrows():
        tasks.append({"queue": "articles", "pmk_ID": r.pmk_ID, "title": r.Leg_Name, "payload": meta(r.pmk_ID)})

    # ---- ids + assignment ------------------------------------------------------------------------
    T = pd.DataFrame(tasks)
    T["task_id"] = [f"{qn[:3].upper()}-{i + 1:04d}" for qn, i in zip(T.queue, T.groupby("queue").cumcount())]
    T["assigned_to"] = ""
    for qn, g in T.groupby("queue"):
        who = refl_users if qn == "reflection" else users
        amap = split_even(list(g.task_id), who)
        T.loc[g.index, "assigned_to"] = g.task_id.map(amap)
    T["queue_title"] = T.queue.map(QUEUE_TITLES)
    T["payload"] = T.payload.map(lambda d: json.dumps(d, ensure_ascii=False))
    # a Sheets cell holds 50,000 chars -> long payloads are split over payload_1..payload_N
    chunks = T.payload.map(lambda s: [s[i:i + CHUNK] for i in range(0, len(s), CHUNK)] or [""])
    n_cols = int(chunks.map(len).max())
    if n_cols > MAX_CHUNKS:
        raise ValueError(f"payload needs {n_cols} cells, more than {MAX_CHUNKS}")
    for i in range(n_cols):
        T[f"payload_{i + 1}"] = chunks.map(lambda c, i=i: c[i] if i < len(c) else "")
    T["status"], T["updated_at"] = "pending", ""
    T = T[["task_id", "queue", "queue_title", "pmk_ID", "title", "assigned_to", "status", "updated_at"]
          + [f"payload_{i + 1}" for i in range(n_cols)]]
    out.mkdir(parents=True, exist_ok=True)
    T.to_csv(out / "tasks.csv", index=False, encoding="utf-8-sig")
    log.info("tasks written: %s", out / "tasks.csv")
    log.info("per queue / user:\n%s", pd.crosstab(T.queue, T.assigned_to, margins=True).to_string())
    return T


def main():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", help="reno outputs\\run_* folder (default: latest)")
    a = ap.parse_args()
    run = Path(a.run) if a.run else sorted(Path("outputs").glob("run_*"))[-1]
    users = [u.strip() for u in os.getenv("VOLUNTEERS", "v1,v2,v3,v4,v5").split(",") if u.strip()]
    refl = [u.strip() for u in os.getenv("REFLECTION_USERS", "v1,v2").split(",") if u.strip()]
    if not set(refl) <= set(users):
        raise SystemExit("REFLECTION_USERS must be a subset of VOLUNTEERS")
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                        handlers=[logging.FileHandler(f"logs/build_queues_{TS}.log", encoding="utf-8"), logging.StreamHandler()])
    log.info("build_queues v%s | run=%s | volunteers=%s | reflection=%s", VERSION, run, users, refl)
    build(run, users, refl, Path("outputs") / f"queues_{TS}")


if __name__ == "__main__":
    main()
