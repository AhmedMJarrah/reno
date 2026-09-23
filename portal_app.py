"""
portal_app.py — v1.1.0  (project: reno, volunteer portal, phase 1)
One Streamlit app, role-based: volunteers (v1..v5) see only their own tasks; the admin sees progress,
reassigns tasks and reads the answers. Backend: Google Sheets through sheets_store.py.

Run locally (CMD):   streamlit run portal_app.py

Theme (put next to this file, as .streamlit/config.toml — see chat for the exact content):
    [theme] base="light", primaryColor sky-blue, light backgrounds. Without it Streamlit falls
    back to its default (dark on some browsers) theme and the custom CSS below has to fight it.

Secrets (.streamlit/secrets.toml locally, "Secrets" on Streamlit Cloud):

    [app]
    sheet_name = "reno_volunteers"

    [auth]
    volunteer_password = "..."
    admin_password = "..."

    [links]
    gazette = "..."      # official gazette archive link
    diwan = "..."        # Diwan (legislation bureau) link

    [gcp_service_account]
    type = "service_account"
    ... (all fields of the service-account JSON key)

Login: v3/v4/v5 type their username as-is. The two lead reviewers can instead type their own
name (see LOGIN_ALIASES below) — no need to remember "v1"/"v2".
"""
from __future__ import annotations

import hmac
import json
import os
from datetime import date, datetime

import pandas as pd
import streamlit as st

from guides import guide
from sheets_store import STATUSES, Store

VERSION = "1.1.0"
QUEUES = {"reflection": "الانعكاسات", "end_date": "تاريخ انتهاء السريان",
          "number_year": "رقم وسنة التشريع", "articles": "المواد الناقصة"}
QUEUE_ICONS = {"reflection": "🔄", "end_date": "📅", "number_year": "🔢", "articles": "📄"}
PROBLEMS = {"new_text_missing_in_snapshot": "النص الجديد الذي جاء به التعديل غير موجود في النسخة الحالية",
            "identical_to_previous_version": "النسخة الحالية مطابقة للنسخة السابقة رغم وجود تعديل",
            "amended_articles_unchanged_in_snapshot": "المواد التي يذكرها التعديل لم تتغير في النسخة الحالية",
            "snapshot_may_belong_to_other_law": "النسخة الحالية قد تكون نص قانون آخر",
            "redo_amendment_text_was_wrong": "نص التعديل كان خطأ واستُبدل من الديوان؛ يلزم إعادة الانعكاس"}
V_REFL = {"ok": "الانعكاس سليم", "fixed": "صححت النص", "undecided": "لا أستطيع الحسم"}
V_END = {"dated": "غير ساري – حددت تاريخ انتهاء السريان", "active": "التشريع ساري فعلاً (الحالة خطأ)", "undecided": "لا أستطيع الحسم"}
V_ROW = {"correct": "صحيح", "wrong": "خطأ – أكتب الصحيح", "duplicate": "مكرر لسجل آخر في المجموعة", "undecided": "لا أستطيع الحسم"}
V_ART = {"entered": "أدخلت المواد", "no_text": "لا توجد مواد نصية في المصدر", "not_found": "لم أجد التشريع"}
VERDICT_AR = {**V_REFL, **V_END, **V_ROW, **V_ART, "answered": "تمت الإجابة"}
KIND_AR = {"final": "نهائية", "draft": "مسودة"}
STATUS_STYLE = {"pending": ("#eef2f7", "#64748b"), "in_progress": ("#fff4e5", "#b45309"), "done": ("#e8f8ef", "#15803d")}
ART_COLORS = ["#2f8fd6", "#059669", "#7c3aed", "#d97706"]

# the two lead reviewers can log in with their own name instead of v1/v2
LOGIN_ALIASES = {"نلا": "v1", "نولا": "v1", "نوله": "v1", "لين": "v2", "لينا": "v2"}

st.set_page_config(page_title="بوابة تدقيق التشريعات", page_icon="⚖️", layout="wide")
st.markdown("""
<style>
/* -------- RTL base -------- */
html, body, [class*="css"] { direction: rtl; }
.stMarkdown, .stMarkdown p, .stMarkdown li, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4,
.stTextInput, .stTextArea, .stSelectbox, .stRadio, .stCaption, .stAlert, .stMetric, .stDataFrame {
    direction: rtl; text-align: right;
}
.stTextArea textarea, .stTextInput input { direction: rtl; text-align: right; }

/* -------- type scale -------- */
html { font-size: 17px; }
.stMarkdown p, .stMarkdown li { font-size: 1.05rem; line-height: 1.95; }
h1, h2, h3, h4 { font-weight: 700; }

/* -------- sidebar -------- */
section[data-testid="stSidebar"] {
    direction: rtl;
    background: linear-gradient(180deg, #eaf4fc 0%, #f8fcfe 55%);
    border-left: 1px solid #d7e9f7;
}

/* pill-style radio (category picker) */
div[role="radiogroup"] > label {
    background: #ffffff; border: 1px solid #d7e6f2; border-radius: 12px;
    padding: 10px 14px; margin-bottom: 8px; width: 100%; transition: all .15s ease;
}
div[role="radiogroup"] > label:hover { border-color: #2f8fd6; }
div[role="radiogroup"] > label:has(input:checked) { background: #2f8fd6 !important; border-color: #2f8fd6; }
div[role="radiogroup"] > label:has(input:checked) p { color: #ffffff !important; font-weight: 700; }

/* -------- chips / badges (atomic labels only — safe under RTL) -------- */
.chip { display: inline-block; padding: 4px 14px; border-radius: 999px; font-size: .85rem; font-weight: 700; }
.abadge { display: inline-block; padding: 4px 14px; border-radius: 999px; font-size: .92rem;
          font-weight: 700; color: #fff; margin-bottom: .5rem; }

/* -------- buttons / containers -------- */
.stButton > button, .stFormSubmitButton > button { border-radius: 10px; font-weight: 600; }
div[data-testid="stVerticalBlockBorderWrapper"] { border-radius: 14px !important; }
[data-testid="stMetricValue"] { font-size: 1.5rem; }
</style>""", unsafe_allow_html=True)


# ============================================================================ backend
@st.cache_resource(show_spinner=False)
def store() -> Store:
    name = st.secrets.get("app", {}).get("sheet_name") or os.getenv("SHEET_NAME", "reno_volunteers")
    if "gcp_service_account" in st.secrets:
        return Store(name, sa_info=dict(st.secrets["gcp_service_account"]))
    return Store(name, sa_file=os.getenv("GOOGLE_SA_JSON", "secrets/service_account.json"))


@st.cache_data(ttl=20, show_spinner=False)
def load(_s: Store, what: str) -> pd.DataFrame:
    df = getattr(_s, what)()
    st.session_state["last_sync"] = datetime.now().strftime("%H:%M:%S")
    return df


def refresh():
    load.clear()


def latest_answers() -> pd.DataFrame:
    a = load(store(), "answers")
    return a.sort_values("saved_at").groupby("task_id").tail(1).set_index("task_id") if len(a) else a


def links() -> dict:
    return dict(st.secrets.get("links", {}))


# ============================================================================ login
def login():
    st.markdown("""<div style='text-align:center;padding:2.2rem 0 1.2rem'>
<div style='font-size:3rem'>⚖️</div>
<h1 style='margin:.3rem 0'>بوابة تدقيق التشريعات</h1>
<p style='color:#5b7387'>مشروع reno — مراجعة بيانات التشريعات الأردنية</p></div>""", unsafe_allow_html=True)
    users = load(store(), "users")
    users = users[users.active.str.upper() == "TRUE"].set_index("username")
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        with st.form("login"):
            st.markdown("#### تسجيل الدخول")
            raw = st.text_input("اسمك", placeholder="مثال: نلا")
            p = st.text_input("كلمة السر", type="password")
            ok = st.form_submit_button("دخول", type="primary", width="stretch")
        if ok:
            key = raw.strip()
            u = LOGIN_ALIASES.get(key) or next((idx for idx in users.index if idx.lower() == key.lower()), None)
            if not u:
                st.error("لم أتعرف على هذا الاسم. تأكد من كتابته بشكل صحيح.")
            elif not p:
                st.error("اكتب كلمة السر.")
            else:
                role = users.at[u, "role"]
                pass_key = "admin_password" if role == "admin" else "volunteer_password"
                if hmac.compare_digest(p, str(st.secrets["auth"][pass_key])):
                    st.session_state.update(user=u, role=role, name=users.at[u, "display_name"])
                    st.rerun()
                else:
                    st.error("كلمة السر غير صحيحة")


# ============================================================================ shared widgets
def top_header(subtitle: str):
    st.markdown(f"""<div style='display:flex;align-items:center;gap:.6rem;margin-bottom:.4rem'>
<span style='font-size:1.8rem'>⚖️</span>
<div><div style='font-weight:800;font-size:1.3rem'>بوابة تدقيق التشريعات</div>
<div style='color:#5b7387;font-size:.85rem'>{subtitle}</div></div></div>""", unsafe_allow_html=True)


def status_chip(status: str) -> str:
    bg, fg = STATUS_STYLE.get(status, ("#eef2f7", "#64748b"))
    return f"<span class='chip' style='background:{bg};color:{fg}'>{STATUSES.get(status, status)}</span>"


def article_badge(n, i: int, extra: str = ""):
    color = ART_COLORS[i % len(ART_COLORS)]
    st.markdown(f"<span class='abadge' style='background:{color}'>المادة {n}{extra}</span>", unsafe_allow_html=True)


def card(p: dict, extra: str = ""):
    g = p.get("gazette", {})
    with st.container(border=True):
        st.markdown(f"##### {p.get('name', '')}")
        st.caption(p.get("type", ""))
        c1, c2, c3 = st.columns(3)
        c1.metric("الرقم", p.get("number") or "—")
        c2.metric("السنة", p.get("year") or "—")
        c3.metric("الحالة", p.get("status") or "—")
        st.markdown(f"**الجريدة الرسمية:** العدد {g.get('number') or '—'} · صفحة {g.get('page') or '—'} · بتاريخ {g.get('date') or '—'}")
        if p.get("base_name"):
            st.markdown(f"**القانون الأصلي:** {p['base_name']}")
        if extra:
            st.info(extra)


def ro(label: str, text: str, key: str, h: int = 220):
    st.text_area(label, text or "— لا يوجد نص —", height=h, disabled=True, key=key)


def prefill(task_id: str) -> dict:
    la = latest_answers()
    if len(la) and task_id in la.index:
        try:
            return json.loads(la.at[task_id, "answer_json"])
        except (TypeError, ValueError):
            return {}
    return {}


def save(task: pd.Series, kind: str, verdict: str, answer: dict):
    try:
        store().save_answer(task, st.session_state.user, kind, verdict, answer)
    except Exception as e:  # network / quota: tell the volunteer clearly, keep the form content
        st.error(f"تعذّر الحفظ، لم يضِع شيء مما كتبت. أعد المحاولة بعد لحظات. التفاصيل: {e}")
        return
    refresh()
    if kind == "final":
        st.session_state.pop(f"cur_{task.queue}", None)
        st.session_state["flash"] = f"✅ حُفظت المهمة {task.task_id}. هذه مهمتك التالية."
    else:
        st.session_state["flash"] = f"💾 حُفظت مسودة {task.task_id}."
    st.rerun()


# ============================================================================ queue forms
def form_reflection(task, p, prev):
    if p.get("reviewer_note"):
        st.warning(f"ملاحظة المراجع: {p['reviewer_note']}")
    st.caption(f"سبب وصول المهمة: {PROBLEMS.get(p.get('problem'), p.get('problem'))} — ترتيب التعديل في السلسلة: {p.get('position_in_chain')}")
    with st.expander("📜 نص التعديل كما نُشر", expanded=True):
        for i, a in enumerate(p.get("amendment_articles", [])):
            article_badge(a["n"], i)
            st.write(a["text"])
    full = st.text_area("نص التعديل الكامل من الجريدة (فقط إذا كان النص أعلاه ناقصاً)", prev.get("full_amendment_text", ""),
                        key=f"full_{task.task_id}", height=120)
    k_extra = f"extra_{task.task_id}"
    if k_extra not in st.session_state:
        st.session_state[k_extra] = [a for a in prev.get("articles", []) if a.get("added")]
    arts = [{"n": a["n"], "previous": a["previous"], "current": a["current"], "added": False} for a in p.get("articles", [])]
    arts += [{"n": a["n"], "previous": "", "current": "", "added": True} for a in st.session_state[k_extra]]
    saved = {a["n"]: a for a in prev.get("articles", [])}
    out = []
    for i, a in enumerate(arts):
        with st.container(border=True):
            article_badge(a["n"], i, " (مادة مضافة)" if a["added"] else "")
            c1, c2, c3 = st.columns(3)
            with c1:
                ro("النسخة السابقة (قبل التعديل)", a["previous"], f"p_{task.task_id}_{i}")
            with c2:
                ro("النسخة الحالية عندنا", a["current"], f"c_{task.task_id}_{i}")
            with c3:
                n = st.text_input("رقم المادة", saved.get(a["n"], {}).get("n", a["n"]), key=f"n_{task.task_id}_{i}") if a["added"] else a["n"]
                txt = st.text_area("النص الصحيح (عدّل هنا)", saved.get(a["n"], {}).get("text", a["current"]), height=220,
                                   key=f"t_{task.task_id}_{i}")
                rep = st.checkbox("المادة ملغاة بموجب هذا التعديل", saved.get(a["n"], {}).get("repealed", False), key=f"r_{task.task_id}_{i}")
        out.append({"n": n, "text": "" if rep else txt, "repealed": rep, "added": a["added"],
                    "changed": rep or a["added"] or txt.strip() != (a["current"] or "").strip()})
    if st.button("➕ إضافة مادة جديدة أضافها التعديل", key=f"add_{task.task_id}"):
        st.session_state[k_extra].append({"n": "", "added": True})
        st.rerun()
    st.divider()
    verdict = st.radio("الإجابة", list(V_REFL), format_func=V_REFL.get, horizontal=True,
                       index=list(V_REFL).index(prev.get("verdict", "ok")) if prev.get("verdict") in V_REFL else 0, key=f"v_{task.task_id}")
    ev = st.text_input("الدليل (العدد والصفحة إن رجعت للجريدة)", prev.get("evidence", ""), key=f"e_{task.task_id}")
    note = st.text_area("ملاحظات", prev.get("note", ""), key=f"o_{task.task_id}", height=80)
    if st.button("حفظ وإنهاء المهمة", type="primary", key=f"s_{task.task_id}", width="stretch"):
        errs = []
        if verdict == "fixed" and not any(a["changed"] for a in out):
            errs.append("اخترت «صححت النص» لكنك لم تغيّر أي مادة.")
        if verdict == "ok" and any(a["changed"] for a in out):
            errs.append("اخترت «الانعكاس سليم» لكنك غيّرت نص مادة. اختر «صححت النص» أو أعد النص كما كان.")
        if verdict == "undecided" and not note.strip():
            errs.append("اكتب في الملاحظات سبب عدم الحسم.")
        if any(a["added"] and not a["n"].strip() for a in out):
            errs.append("اكتب رقم كل مادة مضافة.")
        if errs:
            for e in errs:
                st.error(e)
            return
        save(task, "final", verdict, {"verdict": verdict, "articles": [a for a in out if a["changed"] or verdict == "fixed"],
                                      "full_amendment_text": full, "evidence": ev, "note": note})


def form_end_date(task, p, prev):
    c1, c2 = st.columns(2)
    c1.markdown(f"**ألغي بموجب:** {p.get('canceled_by') or '— غير مسجل —'}")
    c2.markdown(f"**حلّ محله:** {p.get('replaced_by') or '— غير مسجل —'}")
    if p.get("suggested_end_date"):
        st.info(f"تاريخ مقترح: **{p['suggested_end_date']}** — {p.get('suggestion_basis', '')}. تحقق منه قبل اعتماده.")
    with st.expander("🔗 سلسلة القانون (القانون الأصلي وتعديلاته)"):
        ch = pd.DataFrame(p.get("chain", []))
        if len(ch):
            ch["▶"] = ch.pop("this").map({True: "◀ هذا السجل", False: ""})
            st.dataframe(ch.rename(columns={"name": "الاسم", "number": "الرقم", "year": "السنة", "gazette_date": "تاريخ الجريدة",
                                            "end_date": "انتهاء السريان", "status": "الحالة"}).drop(columns=["pmk_ID"]),
                         hide_index=True, width="stretch")
    verdict = st.radio("الإجابة", list(V_END), format_func=V_END.get,
                       index=list(V_END).index(prev["verdict"]) if prev.get("verdict") in V_END else 0, key=f"v_{task.task_id}")
    d0 = date.fromisoformat(prev["end_date"]) if prev.get("end_date") else None
    d = st.date_input("تاريخ انتهاء السريان", d0, min_value=date(1900, 1, 1), max_value=date.today(),
                      format="DD/MM/YYYY", key=f"d_{task.task_id}", disabled=verdict != "dated")
    by = st.text_input("التشريع الذي أنهى السريان (الاسم، الرقم، السنة، والمادة إن وجدت)", prev.get("ended_by", ""),
                       key=f"b_{task.task_id}", disabled=verdict != "dated")
    ev = st.text_input("الدليل: رقم عدد الجريدة والصفحة", prev.get("evidence", ""), key=f"e_{task.task_id}")
    note = st.text_area("ملاحظات", prev.get("note", ""), key=f"o_{task.task_id}", height=80)
    if st.button("حفظ وإنهاء المهمة", type="primary", key=f"s_{task.task_id}", width="stretch"):
        errs = []
        if verdict == "dated" and (not d or not by.strip() or not ev.strip()):
            errs.append("مع «حددت التاريخ» يلزم: التاريخ، والتشريع المنهي، والدليل.")
        if verdict == "active" and not ev.strip():
            errs.append("اكتب الدليل على أن التشريع ما زال سارياً.")
        if verdict == "undecided" and not note.strip():
            errs.append("اكتب في الملاحظات سبب عدم الحسم وما بحثت فيه.")
        if errs:
            for e in errs:
                st.error(e)
            return
        save(task, "final", verdict, {"verdict": verdict, "end_date": d.isoformat() if (d and verdict == "dated") else "",
                                      "ended_by": by if verdict == "dated" else "", "evidence": ev, "note": note})


def form_number_year(task, p, prev):
    st.info(f"**{p.get('kind_ar', '')}** — كل السجلات أدناه مسجلة برقم **{p.get('number')}** لسنة **{p.get('year')}**.")
    rows, saved, out = p.get("rows", []), prev.get("rows", {}), {}
    labels = {r["pmk_ID"]: f"{r['pmk_ID']} — {r['name']}" for r in rows}
    for r in rows:
        pid, sv = r["pmk_ID"], saved.get(r["pmk_ID"], {})
        g = r["gazette"]
        with st.container(border=True):
            st.markdown(f"**{r['name']}**")
            st.caption(f"{r['type']} · المعرّف {pid}")
            c1, c2 = st.columns(2)
            c1.markdown(f"**الجريدة:** العدد {g.get('number') or '—'} · صفحة {g.get('page') or '—'} · بتاريخ {g.get('date') or '—'}")
            c2.markdown(f"**في الديوان:** {r.get('diwan_number') or '—'} / {r.get('diwan_year') or '—'}"
                        f" &nbsp;·&nbsp; **سنة العنوان:** {r.get('own_title_year') or '—'}", unsafe_allow_html=True)
            if r.get("base_name"):
                st.markdown(f"**يعدّل:** {r['base_name']}")
            c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
            v = c1.selectbox("الحكم", list(V_ROW), format_func=V_ROW.get, key=f"rv_{task.task_id}_{pid}",
                             index=list(V_ROW).index(sv["verdict"]) if sv.get("verdict") in V_ROW else 0)
            num = c2.text_input("الرقم الصحيح", sv.get("number", ""), key=f"rn_{task.task_id}_{pid}", disabled=v != "wrong")
            yr = c3.text_input("السنة الصحيحة", sv.get("year", ""), key=f"ry_{task.task_id}_{pid}", disabled=v != "wrong")
            others = [x for x in labels if x != pid]
            dup = c4.selectbox("مكرر لـ", others, format_func=labels.get, key=f"rd_{task.task_id}_{pid}", disabled=v != "duplicate") if others else ""
        out[pid] = {"verdict": v, "number": num if v == "wrong" else "", "year": yr if v == "wrong" else "",
                    "duplicate_of": dup if v == "duplicate" else ""}
    ev = st.text_input("الدليل: أرقام الأعداد والصفحات التي فتحتها", prev.get("evidence", ""), key=f"e_{task.task_id}")
    note = st.text_area("ملاحظات", prev.get("note", ""), key=f"o_{task.task_id}", height=80)
    if st.button("حفظ وإنهاء المهمة", type="primary", key=f"s_{task.task_id}", width="stretch"):
        errs = []
        for pid, a in out.items():
            if a["verdict"] == "wrong" and not (a["number"].strip().isdigit() and len(a["year"].strip()) == 4 and a["year"].strip().isdigit()):
                errs.append(f"السجل {pid}: اكتب رقماً صحيحاً وسنة من أربعة أرقام.")
        if any(a["verdict"] != "undecided" for a in out.values()) and not ev.strip():
            errs.append("اكتب الدليل (الأعداد والصفحات).")
        if all(a["verdict"] == "correct" for a in out.values()):
            errs.append("لا يمكن أن تكون كل السجلات صحيحة: هي تحمل الرقم والسنة نفسيهما. راجع الدليل.")
        if any(a["verdict"] == "undecided" for a in out.values()) and not note.strip():
            errs.append("اكتب في الملاحظات سبب عدم الحسم.")
        if errs:
            for e in errs:
                st.error(e)
            return
        verdict = "undecided" if all(a["verdict"] == "undecided" for a in out.values()) else "answered"
        save(task, "final", verdict, {"verdict": verdict, "rows": out, "evidence": ev, "note": note})


def form_articles(task, p, prev):
    k = f"arts_{task.task_id}"
    if k not in st.session_state:
        st.session_state[k] = prev.get("articles") or [{"n": "1", "text": ""}]
    arts = st.session_state[k]
    verdict = st.radio("الإجابة", list(V_ART), format_func=V_ART.get, horizontal=True,
                       index=list(V_ART).index(prev["verdict"]) if prev.get("verdict") in V_ART else 0, key=f"v_{task.task_id}")
    if verdict == "entered":
        st.caption(f"عدد المواد المدخلة: {len(arts)}. انسخ النص كما هو دون «المادة (…)» في أوله.")
        for i, a in enumerate(arts):
            with st.container(border=True):
                article_badge(a["n"] or "؟", i)
                c1, c2, c3 = st.columns([1, 6, 0.6])
                a["n"] = c1.text_input("رقم المادة", a["n"], key=f"an_{task.task_id}_{i}")
                a["text"] = c2.text_area("نص المادة", a["text"], key=f"at_{task.task_id}_{i}", height=140)
                if c3.button("🗑️", key=f"del_{task.task_id}_{i}", help="حذف هذه المادة"):
                    arts.pop(i)
                    st.rerun()
        if st.button("➕ إضافة مادة", key=f"add_{task.task_id}"):
            last = arts[-1]["n"] if arts else "0"
            arts.append({"n": str(int(last) + 1) if str(last).isdigit() else "", "text": ""})
            st.rerun()
    src = st.text_input("المصدر: رقم العدد والصفحات التي نسخت منها", prev.get("source", ""), key=f"src_{task.task_id}")
    note = st.text_area("ملاحظات", prev.get("note", ""), key=f"o_{task.task_id}", height=80)
    c1, c2 = st.columns(2)
    body = {"verdict": verdict, "articles": arts if verdict == "entered" else [], "source": src, "note": note}
    if c1.button("💾 حفظ مؤقت", key=f"dr_{task.task_id}", width="stretch"):
        save(task, "draft", verdict, body)
    if c2.button("حفظ وإنهاء المهمة", type="primary", key=f"s_{task.task_id}", width="stretch"):
        errs = []
        if verdict == "entered":
            nums = [a["n"].strip() for a in arts]
            if not arts or any(not a["n"].strip() or not a["text"].strip() for a in arts):
                errs.append("كل مادة تحتاج رقماً ونصاً. احذف المواد الفارغة.")
            if len(set(nums)) != len(nums):
                errs.append("يوجد رقم مادة مكرر.")
            if any(a["text"].strip().startswith(("المادة", "مادة")) for a in arts):
                errs.append("احذف «المادة (…)» من أول النص؛ الرقم له خانة مستقلة.")
        if not src.strip() and verdict != "not_found":
            errs.append("اكتب المصدر (العدد والصفحات).")
        if verdict != "entered" and not note.strip():
            errs.append("اشرح في الملاحظات.")
        if errs:
            for e in errs:
                st.error(e)
            return
        save(task, "final", verdict, body)


FORMS = {"reflection": form_reflection, "end_date": form_end_date, "number_year": form_number_year, "articles": form_articles}


# ============================================================================ volunteer page
def volunteer():
    s = store()
    T = load(s, "tasks")
    mine = T[T.assigned_to == st.session_state.user]
    with st.sidebar:
        st.markdown(f"""<div style='text-align:center;padding:.4rem 0 1rem'>
<div style='width:56px;height:56px;border-radius:50%;background:#2f8fd6;color:#fff;
display:flex;align-items:center;justify-content:center;font-size:1.4rem;font-weight:700;margin:0 auto .5rem'>
{(st.session_state.name or '?')[:1]}</div>
<div style='font-weight:700;font-size:1.1rem'>{st.session_state.name}</div>
<div style='color:#5b7387;font-size:.85rem'>متطوع مراجعة</div></div>""", unsafe_allow_html=True)
        qs = [q for q in QUEUES if q in set(mine.queue)]
        if not qs:
            st.info("لا توجد مهمات مخصصة لك حالياً.")
            return
        st.caption(f"إجمالي مهماتك: {(mine.status == 'done').sum()} من {len(mine)}")
        st.caption("📂 الفئة")
        q = st.radio("الفئة", qs, format_func=lambda x: f"{QUEUE_ICONS[x]}  {QUEUES[x]}", key="queue", label_visibility="collapsed")
        m_sel = mine[mine.queue == q]
        d = (m_sel.status == "done").sum()
        st.caption(f"أنجزت {d} من {len(m_sel)} في هذه الفئة")
        st.progress(d / len(m_sel) if len(m_sel) else 0.0)
        st.divider()
        c1, c2 = st.columns(2)
        if c1.button("🔄 تحديث", width="stretch"):
            refresh()
            st.rerun()
        if c2.button("خروج", width="stretch"):
            st.session_state.clear()
            st.rerun()
        st.caption(f"آخر مزامنة: {st.session_state.get('last_sync', '—')} · v{VERSION}")
    top_header("مشروع reno — مراجعة بيانات التشريعات الأردنية")
    if msg := st.session_state.pop("flash", None):
        st.success(msg)
    t_task, t_guide, t_list, t_start = st.tabs(["📝 المهمة", "📘 الشرح", "📋 مهماتي", "👋 ابدأ هنا"])
    m = mine[mine.queue == q].sort_values("task_id")
    with t_guide:
        st.markdown(guide(q, links()))
    with t_start:
        st.markdown(guide("start", links()))
    with t_list:
        view = m[["task_id", "title", "status", "updated_at"]].assign(status=m.status.map(STATUSES))
        st.dataframe(view.rename(columns={"task_id": "المهمة", "title": "التشريع", "status": "الحالة", "updated_at": "آخر تحديث"}),
                     hide_index=True, width="stretch")
        pick = st.selectbox("افتح مهمة", m.task_id.tolist(), key=f"pick_{q}")
        if st.button("فتح", key=f"open_{q}"):
            st.session_state[f"cur_{q}"] = pick
            st.rerun()
    with t_task:
        cur = st.session_state.get(f"cur_{q}")
        if not cur:
            nxt = pd.concat([m[m.status == "in_progress"], m[m.status == "pending"]])
            if not len(nxt):
                st.balloons()
                st.success("أنهيت كل مهماتك في هذه الفئة. شكراً لك! يمكنك مراجعة إجاباتك من «مهماتي».")
                return
            cur = nxt.task_id.iloc[0]
        task = m[m.task_id == cur].iloc[0]
        p = s.payload(task)
        st.subheader(f"{task.task_id} — {task.title}")
        st.markdown(status_chip(task.status), unsafe_allow_html=True)
        if q != "number_year":
            card(p)
        FORMS[q](task, p, prefill(task.task_id))


# ============================================================================ admin page
def admin():
    s = store()
    T, U = load(s, "tasks"), load(s, "users")
    vols = U[U.role == "volunteer"].username.tolist()
    names = U.set_index("username").display_name.to_dict()
    with st.sidebar:
        st.markdown(f"""<div style='text-align:center;padding:.4rem 0 1rem'>
<div style='width:56px;height:56px;border-radius:50%;background:#7c3aed;color:#fff;
display:flex;align-items:center;justify-content:center;font-size:1.4rem;font-weight:700;margin:0 auto .5rem'>
{(st.session_state.name or '?')[:1]}</div>
<div style='font-weight:700;font-size:1.1rem'>{st.session_state.name}</div>
<div style='color:#5b7387;font-size:.85rem'>المشرف</div></div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        if c1.button("🔄 تحديث", width="stretch"):
            refresh()
            st.rerun()
        if c2.button("خروج", width="stretch"):
            st.session_state.clear()
            st.rerun()
        st.caption(f"آخر مزامنة: {st.session_state.get('last_sync', '—')} · v{VERSION}")
    top_header("لوحة المشرف")
    if msg := st.session_state.pop("flash", None):
        st.success(msg)
    t1, t2, t3, t4 = st.tabs(["📊 التقدم", "🔁 إعادة التوزيع", "🗂️ الإجابات", "🔌 الاتصال والسجل"])
    with t1:
        done = (T.status == "done").sum()
        st.metric("الإنجاز الكلي", f"{done} / {len(T)}", f"{done / len(T):.0%}" if len(T) else None)
        for q, title in QUEUES.items():
            tq = T[T.queue == q]
            if not len(tq):
                continue
            st.markdown(f"#### {QUEUE_ICONS[q]} {title}")
            tab = pd.crosstab(tq.assigned_to.map(lambda u: f"{u} ({names.get(u, u)})"), tq.status.map(STATUSES))
            tab["المجموع"] = tab.sum(axis=1)
            st.dataframe(tab, width="stretch")
    with t2:
        st.caption("تُنقل فقط المهمات «بانتظارك» (لم تُبدأ)، حتى لا تضيع مسودة أحد.")
        q = st.selectbox("الفئة", [x for x in QUEUES if x in set(T.queue)], format_func=QUEUES.get, key="rq")
        tq = T[T.queue == q]
        pend = tq[tq.status == "pending"].groupby("assigned_to").size()
        left = tq[tq.status != "done"].groupby("assigned_to").size()
        free = [v for v in vols if left.get(v, 0) == 0]
        if free:
            st.success("أنهى كل مهماته في هذه الفئة: " + "، ".join(f"{v} ({names.get(v, v)})" for v in free))
        c1, c2, c3 = st.columns(3)
        src = c1.selectbox("من", [v for v in vols if pend.get(v, 0)], format_func=lambda v: f"{v} ({names.get(v, v)}) — {pend.get(v, 0)} بانتظار")
        dst = c2.selectbox("إلى", [v for v in vols if v != src], format_func=lambda v: f"{v} ({names.get(v, v)})")
        n = c3.number_input("عدد المهمات", 1, int(pend.get(src, 1)) if src else 1, max(1, int(pend.get(src, 0)) // 2) if src else 1)
        if st.button("نقل المهمات", type="primary", disabled=not src):
            rows = tq[(tq.assigned_to == src) & (tq.status == "pending")].sort_values("task_id", ascending=False).head(int(n))
            s.reassign(rows._row.astype(int).tolist(), dst, st.session_state.user, f"{q}: {src} -> {dst}")
            refresh()
            st.session_state["flash"] = f"نُقلت {len(rows)} مهمة من {src} إلى {dst}."
            st.rerun()
    with t3:
        la = latest_answers()
        if not len(la):
            st.info("لا توجد إجابات بعد.")
        else:
            qa = st.selectbox("الفئة", ["الكل"] + list(QUEUES), format_func=lambda x: QUEUES.get(x, x), key="aq")
            view = la.reset_index()
            if qa != "الكل":
                view = view[view.queue == qa]
            view = view.assign(queue=view.queue.map(QUEUES), kind=view.kind.map(KIND_AR),
                               verdict=view.verdict.map(lambda v: VERDICT_AR.get(v, v)))
            st.dataframe(view[["task_id", "queue", "username", "kind", "verdict", "saved_at"]]
                        .rename(columns={"task_id": "المهمة", "queue": "الفئة", "username": "المستخدم",
                                          "kind": "النوع", "verdict": "القرار", "saved_at": "وقت الحفظ"}),
                        hide_index=True, width="stretch")
            tid = st.selectbox("عرض إجابة", view.task_id.tolist(), key="ashow")
            if tid:
                st.json(json.loads(la.at[tid, "answer_json"]))
            st.download_button("⬇️ تنزيل كل الإجابات (CSV)", load(s, "answers").to_csv(index=False).encode("utf-8-sig"),
                               "answers.csv", "text/csv")
    with t4:
        try:
            st.success(f"متصل بالجدول: {s.sh.title} — أوراق العمل: {', '.join(w.title for w in s.sh.worksheets())}")
        except Exception as e:
            st.error(f"لا يوجد اتصال: {e}")
        log_df = pd.DataFrame(s.ws("audit_log").get_all_records()[-50:][::-1])
        if len(log_df):
            st.dataframe(log_df.rename(columns={"at": "الوقت", "actor": "المستخدم", "action": "الإجراء", "details": "التفاصيل"}),
                        hide_index=True, width="stretch")
        else:
            st.caption("لا يوجد سجل بعد.")


# ============================================================================ main
def main():
    try:
        store()
    except Exception as e:
        st.error(f"تعذّر الاتصال بـ Google Sheets: {e}")
        st.stop()
    if "user" not in st.session_state:
        login()
    elif st.session_state.role == "admin":
        admin()
    else:
        volunteer()


main()
