"""
reno_pipeline.py — v2.4.0  (project: reno)
Produces a clean, knowledge-graph-ready laws CSV + a corrected master JSON + review/volunteer files.

v2.5.0 (evidence: pmk 6038 قانون معدل لقانون الجنسية 1948, pmk 6056; full-corpus before/after diff):
  - ONE deterministic target resolver (resolve_targets) for every path - targets, new-text probes,
    operations - replacing a digits-only regex that silently missed: numbers written as words
    ("المادة السابعة", "الحادية والعشرون"), a base law cited by name ("من قانون الجنسية لسنة 1928",
    checked against the base law's name), "(3 - و)", "(54 مكررة)", "المادة5", lists closed by "منه".
  - operative articles are found by CONTENT (operation verb, not the naming clause), no longer by
    position: article 1 was always skipped, and in old laws it is often the only real change.
  - no more arbitrary fallback: an unresolved target is sent to a volunteer as
    "target_article_unresolved" / "amendment_has_no_operative_text" instead of "the first 5 changed
    articles"; each target now carries its evidence (clause, how matched, confirmed by the new text's
    own number) through build_queues to the portal.
  - result on 1849 amendments: 1448 -> 1709 with identified targets; rows_out unchanged (4396);
    the 18 target ids no longer produced were each inspected: mentions inside the new wording
    ("المادة 33 من هذا القانون"), other laws, quoted text, own headings, or ambiguous renumbering.
    6038 and 1555 left the queue after content verification (new wording present in the snapshot);
    4 new queue entries (targets now found whose snapshot article never changed) are real checks.
v2.4.0 (evidence: pmk 3219 قانون الإعسار, corpus-wide scan, see code comments where noted):
  - parse_amended_articles: dropped the "منه/منها" ("...of it") fallback whenever the referenced
    number is also one of the amendment's own article numbers - it was a self-reference, not a
    pointer into the base law (proven false positive: قانون الإعسار's own article 92 says
    "المادة (91) منه", meaning its OWN article 91, not قانون التجارة's). Affects 6/1499 amendments
    corpus-wide, all from a wrong guess to an honest "no confident target".
  - New range-repeal check: a full replacement law can repeal a whole block of the base law's
    articles in one dedicated article ("تلغى ... المواد من (290) ولغاية (477) من قانون التجارة").
    When found, the pipeline checks every article in that range against the current reflected
    snapshot: if all are already empty, the row is confirmed "ok" automatically (no volunteer
    queue entry); if some still carry text, only THOSE exception article numbers are queued for
    review - never the full range, and never the amendment's unrelated other articles.

Inputs (paths in .env):
  MASTER_JSON_PATH           master JSON (names, nesting, articles, reflections)
  LAWS_CSV_PATH              laws_with_rebuilt_chains_clean_*.csv
  DIWAN_LAWS_PATH            UD_leg_Laws export (base_laws.csv)
  DIWAN_AMENDMENTS_PATH      UD_leg_Legislative_Amendments export (amend_laws.csv)
  DIWAN_AMEND_ARTICLES_PATH  amendment articles export (amend_art.csv)
  DIWAN_LAW_ARTICLES_PATH    UD_leg_Article exports for laws, several allowed separated by ';' (117.csv;art.csv)
  DIWAN_ARTICLE_VERSIONS_PATH UD_leg_Article WHERE ModLeg IS NOT NULL (not_null.csv) - article text after each amendment

Rules (decided with Ahmed):
  - Identity = (source_table, pmk_ID); (Law_Number, Year) is never a join/dedup key.
  - Status = Diwan code (1 ساري / 2 غير ساري); any End_Date => غير ساري.
  - Names = the Diwan's own names (UD_leg_Laws.Law_Name / UD_leg_Legislative_Amendments.amendment), only tatweel and
    repeated spaces removed; records the Diwan lacks get the Diwan pattern (no year, no 'وتعديلاته',
    'قانون معدل لقانون X'; budget laws keep their fiscal year; old-style تعديل/ذيل titles kept).
  - Parent of each amendment is chosen by scored evidence: Diwan linked list (UD_leg_Laws.fnk_leg_Laws10765),
    JSON nesting, CSV chain_id_v2, the amendment's article 1 ("ويقرأ مع القانون رقم N لسنة Y"), name match,
    and date sanity (an amendment cannot precede its base). Weak decisions are kept but flagged.
  - Duplicates are removed only when content is verified (same ModLeg+gazette, or same law: year+gazette+name+text).
  - Article text proven to belong to another legislation is replaced from the Diwan when the Diwan text matches.
  - Reflection article 1 overwritten by the amendment's own naming article is restored from the previous version.
  - Everything else goes to review/volunteer files — no guessing.

Usage (CMD):  py reno_pipeline.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from reflection_decisions import MANUAL as MANUAL_REFLECTIONS  # case-by-case decisions (same folder)
except ImportError:  # pipeline still runs; those cases stay in the volunteer queue
    MANUAL_REFLECTIONS = {}

VERSION = "2.5.0"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
log = logging.getLogger("reno_pipeline")
ISSUE_DATES: dict = {}

# ----------------------------------------------------------------------------- helpers
CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")  # e.g. a Leg_Name made of 65,000 NUL bytes
AR_DIAC = re.compile(r"[ً-ْـ]")


def norm_ar(s: str) -> str:
    """Normalise Arabic for comparison only (never written to output)."""
    s = AR_DIAC.sub("", str(s or ""))
    s = re.sub("[إأآا]", "ا", s)
    s = s.replace("ى", "ي").replace("ة", "ه").replace("ؤ", "و").replace("ئ", "ي")
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def squash(s: str) -> str:
    return re.sub(r"\s+", "", norm_ar(s))


def jaccard(a: str, b: str) -> float:
    A, B = set(norm_ar(a).split()), set(norm_ar(b).split())
    return len(A & B) / len(A | B) if A and B else 0.0


def to_iso(v) -> str:
    """Accept M/D/YYYY (CSV), DD-MM-YYYY (JSON) or YYYY-MM-DD; return YYYY-MM-DD or ''."""
    v = str(v or "").strip()
    if not v:
        return ""
    for fmt in ("%m/%d/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(v[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def clean_num(v) -> str:
    v = str(v or "").strip()
    return "" if v in ("", "0", "nan", "None") else v


def valid_year(v) -> bool:
    return bool(re.fullmatch(r"(19|20)\d\d", str(v or "").strip()))


def as_int(v, default=10**9) -> int:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return default


WRAPPER = re.compile(r"^\s*قانون\s+(?:مؤقت\s+)?(?:معدل\s+)?(?:مؤقت\s+)?رقم\s*\d+\s*لسنة\s*\d{4}\s*\((.+)\)\s*(وتعديلاته)?\s*$")


def strip_wrapper(name: str) -> str | None:
    m = WRAPPER.match(str(name or ""))
    if not m:
        return None
    return m.group(1).strip() + (" وتعديلاته" if m.group(2) else "")


# "ويقرأ مع القانون رقم 11 لسنة 1994" / "ويقرا مع قانون الجمارك والمكوس رقم (1) لسنة 1962"
READ_WITH = re.compile(r"ويقر[أا]\s+مع\s+.{0,160}?رقم\s*\(?\s*(\d+)\s*\)?\s*لسنة\s*\(?\s*(\d{4})", re.S)
# older style title: "قانون تعديل قانون السكك الحديدية رقم (1) لسنة 1932"
AMEND_TITLE = re.compile(r"(?:تعديل|معدل\s+ل)\s*قانون.{0,120}?رقم\s*\(?\s*(\d+)\s*\)?\s*لسنة\s*\(?\s*(\d{4})", re.S)
ART_REF = re.compile(r"الماد(?:ة|تين|تان|تان|ه)\s*\(?\s*(\d+)")
STRICT_ORIGINAL_LAW = re.compile(r"القانون\s+ال[اأ]صلي")
LOOSE_ORIGINAL_LAW = re.compile(r"منه|منها")
# a full replacement law (e.g. قانون الإعسار, pmk 3219) repeals a block of the base law's
# articles in one dedicated article near its end, e.g.:
#   "تلغى أحكام ... الواردة في المواد من (290) ولغاية (477) من قانون التجارة رقم (12) لسنة 1966"
# verified against the real corpus: 3 matches total, 0 false positives (v2.4.0).
RANGE_REPEAL = re.compile(
    r"(?:يلغ[ىي]|تلغ[ىي]).{0,60}?المواد\s*من\s*\(?\s*(\d+)\s*\)?\s*(?:و?لغاي[ةه]|الى|إلى|حتى)\s*\(?\s*(\d+)\s*\)?", re.S)


def naming_article(articles: list[dict]) -> str:
    """Article 1 (or 2 when article 1 is a preamble like 'عملا بالمادة 39 ...')."""
    for a in articles[:2]:
        t = str(a.get("text", ""))
        if "يسمى" in t or "يسمي" in t:
            return t
    return str(articles[0].get("text", "")) if articles else ""


def parse_parent_ref(articles: list[dict]) -> tuple[str, str] | None:
    text = naming_article(articles)[:700]
    m = READ_WITH.search(text) or AMEND_TITLE.search(text)
    return (m.group(1), m.group(2)) if m else None


TITLE = re.compile(r"يسم[ىي]\s+هذا\s+(القانون|النظام|الذيل|التعديل)?\s*(?:الم[ؤو]قت)?\s*[\(\"«]?\s*([^()\n]{3,250})")
TITLE_END = re.compile(r"\s(?:ويعمل|ويقر[أا]|المشار|ويحل|ويسري)|لسنة\s*\d")
STOP = {"قانون", "القانون", "لقانون", "لسنه", "سنه", "معدل", "المعدل", "مؤقت", "المؤقت", "رقم", "و", "ل", "لل",
        "تعديل", "وتعديلاته", "في", "من", "على", "الى", "ويعمل", "به", "هذا", "يسمى", "موقت", "الموقت"}


def stem(t: str) -> str:
    t = t[1:] if t.startswith("و") and len(t) > 4 else t
    for pre in ("بال", "لل", "ال"):
        if t.startswith(pre) and len(t) - len(pre) >= 3:
            return t[len(pre):]
    return t


def content_tokens(s: str) -> set[str]:
    return {stem(t) for t in norm_ar(s).split() if t not in STOP and not t.isdigit() and len(t) > 1}


def articles_check(articles: list[dict], names: list[str]) -> tuple[str, float]:
    """Does the law's own naming article match its record name? Catches article text
    attached to the wrong legislation (verified on real cases, e.g. a 'نظام' text in a law)."""
    if not articles:
        return "no_articles", 0.0
    m = TITLE.search(naming_article(articles)[:600])
    if not m:
        return "no_title_in_article1", 0.0
    if m.group(1) == "النظام":
        return "articles_title_mismatch", 0.0
    title = TITLE_END.split(m.group(2))[0]
    T = content_tokens(title)
    N = set().union(*(content_tokens(n) for n in names))
    score = len(T & N) / len(T) if T else 1.0
    return ("ok" if score >= 0.5 else "articles_title_mismatch"), round(score, 2)


# ----------------------------------------------------------------------------- target resolver (v2.5.0)
# ONE deterministic resolver for "which article(s) of the BASE law does this amendment touch?",
# used by every path (targets, new-text probes, operations) so they can never disagree again.
# Replaces three silent failures proven on pmk 6038 (قانون معدل لقانون الجنسية 1948):
#   1) numbers written as words ("المادة السابعة") were invisible to the digits-only ART_REF;
#   2) a base law cited by NAME ("من قانون الجنسية لسنة 1928") was not accepted - only the
#      literal "القانون الأصلي"/"منه" were;
#   3) article 1 was always skipped by POSITION as "the naming clause" - in old laws it is often the
#      operative article (6038's only real change was in its article 1).
# Every accepted target carries its evidence (the clause + how the base law was identified), and a
# target is "confirmed" when the replacement text itself starts with the same number ("7. يجوز").
_AN = str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ى": "ي", "ة": "ه", "ـ": ""})
_UNITS = {"الاولي": 1, "الحاديه": 1, "الثانيه": 2, "الثالثه": 3, "الرابعه": 4, "الخامسه": 5, "السادسه": 6,
          "السابعه": 7, "الثامنه": 8, "التاسعه": 9, "العاشره": 10}
_TENS = {"العشرين": 20, "العشرون": 20, "الثلاثين": 30, "الثلاثون": 30, "الاربعين": 40, "الاربعون": 40,
         "الخمسين": 50, "الخمسون": 50, "الستين": 60, "الستون": 60, "السبعين": 70, "السبعون": 70,
         "الثمانين": 80, "الثمانون": 80, "التسعين": 90, "التسعون": 90, "المائه": 100, "المئه": 100}
ARTWORD = re.compile(r"(?:ال)?(?:ماده|مادتين|مادتان|مواد)(?=[\s(\d])")
HEADING = re.compile(r"(?:^|\n)\s*[-–]?\s*(?:ال)?ماده\s*\(?\s*(\d{1,4})\s*\)?\s*[:\-–]")
OP_VERB = re.compile(r"(?<![\w])[وف]?(?:تعدل|يعدل|تلغي|يلغي|تضاف|يضاف|يستعاض|تستبدل|يستبدل|تحذف|يحذف|تشطب|يشطب|يعاد)(?![\w])")
LAW_WORD = re.compile(r"^\s*من\s+(?:ال)?(?:قانون|نظام|ذيل|قرار|لائحه)\s*(.{0,120})")
NEW_T_N = re.compile(r"(?:(?:كما|بما)\s+(?:يلي|ياتي)|(?:بال|علي\s+ال)(?:نص|صوره|نحو)\s+(?:التالي|الاتي|التاليه|الاتيه)|كالاتي|كالتالي)[^:\n]{0,40}[:.]?\s*")
NAMING = re.compile(r"يسمي\s+هذا\s+(?:القانون|النظام|الذيل|التعديل|القرار)")
BASE_ALIAS = re.compile(r"^\s*(?:من\s+)?(?:القانون\s+(?:الاصلي|الرئيسي)|النظام\s+الاصلي|منه|منها|من\s+(?:\S+\s+){1,4}?(?:الانف|المذكور|المشار))")


def _ar(s: str) -> str:
    return AR_DIAC.sub("", str(s)).translate(_AN)


def _read_number(t: str, i: int) -> tuple[int | None, int]:
    """Read one article number at t[i:] - digits or an Arabic ordinal (1..199) - plus an optional
    sub-clause marker like '(3 - و)' / '(3/أ)'. Returns (number, end_index) or (None, i)."""
    m = re.match(r"\s*\(?\s*(\d{1,4})\s*(?:[-/]\s*[ا-ي]\s*)?(?:مكرر[هة]?(?:\s+[^\s()]+)?\s*)?\)?", t[i:])
    if m:
        return int(m.group(1)), i + m.end()
    m = re.match(r"\s*([^\s()]+)(?:\s+(عشره|عشر))?(?:\s+و\s?([^\s()]+))?", t[i:])
    if not m:
        return None, i
    w1, teen, w2 = m.group(1), m.group(2), m.group(3)
    if w1 in _UNITS and (w1 != "الحاديه" or teen or w2):
        n, end = _UNITS[w1], m.end(1)
        if teen:
            n, end = n + 10, m.end(2)
        elif w2 and ("ال" + w2.removeprefix("ال")) in _TENS:
            n, end = n + _TENS["ال" + w2.removeprefix("ال")], m.end(3)
        return n, i + end
    if w1 in _TENS:
        return _TENS[w1], i + m.end(1)
    return None, i


def operative(articles: list[dict]) -> list[dict]:
    """The amendment's own articles that actually change something (by CONTENT, not position):
    has an operation verb and is not the naming clause. Enforcement/execution clauses drop out."""
    out = []
    for a in articles:
        t = _ar(a.get("text", ""))
        if NAMING.search(t[:150]):
            continue
        if OP_VERB.search(t) or STRICT_ORIGINAL_LAW.search(t):
            out.append(a)
    return out


def resolve_targets(text: str, own_nums: set[str], base_names: list[str] | None) -> list[dict]:
    """Base-law articles referenced by one operative clause, each with its evidence.
    base_names=None -> accept any "من قانون ..." (the operations engine's historical behaviour)."""
    t = _ar(text)
    base_core = set().union(*(core(n) for n in base_names)) if base_names else set()
    out, seen, pending = [], set(), []
    for m in ARTWORD.finditer(t):
        nums, j = [], m.end()
        n, j2 = _read_number(t, j)
        while n is not None:
            nums.append(n)
            j = j2
            sep = re.match(r"\s*(?:،|,|و)\s*", t[j:])
            if not sep:
                break
            n, j2 = _read_number(t, j + sep.end())
        if not nums:
            continue
        tail = t[j: j + 160]
        nxt = ARTWORD.search(tail)
        near = tail[: min(90, nxt.start() if nxt else 90)]   # up to the next article mention
        law = LAW_WORD.match(tail)
        if BASE_ALIAS.match(tail) or re.search(r"القانون\s+(?:الاصلي|الرئيسي)|الوارد\w*\s+(?:فيه|فيها)(?!\w)|(?<!\w)(?:اليه|اليها)(?!\w)"
                                              r"|^\s*من\s+القانون(?=\s+(?:حسبما|كما|وتعديلاته)|\s*[.,،:)])", near):
            via = "القانون الأصلي/منه"
            if "منه" in tail[:8] and not STRICT_ORIGINAL_LAW.search(tail) and any(str(x) in own_nums for x in nums):
                continue  # self-reference guard (v2.4.0, pmk 3219)
        elif law:
            cited = core(re.split(r"\s(?:لسنه|رقم|كما|بما|علي|بال|الواقعه|المؤرخ)(?!\w)|[:\n(]", law.group(1))[0])
            if base_names is not None and (not cited or len(cited & base_core) / len(cited) < 0.5):
                pending = []
                continue  # an article of ANOTHER law - never a target
            via = "اسم القانون الأصلي"
        elif OP_VERB.search(t[max(0, m.start() - 25): m.start()]) and not re.search(r"قانون|نظام|ذيل|قرار", tail[:60]):
            via = "سياق التعديل (دون ذكر القانون)"
        else:
            pending.append((nums, m.start()))  # may be part of a list closed by "... منه"
            continue
        for pn, ps in pending:  # same sentence, no break before this base-resolved mention
            if not re.search(r"[.:\n]", t[ps: m.start()]):
                for x in pn:
                    if str(x) not in seen:
                        seen.add(str(x))
                        out.append({"n": str(x), "via": "ضمن قائمة مواد تنتهي بذكر القانون الأصلي",
                                    "clause": str(text).strip()[:700], "confirmed": False})
        pending = []
        body = NEW_T_N.search(t[j:])
        lead = re.match(r"\s*[-–]*\s*\(?\s*(\d{1,4})\s*\)?\s*(?:[.\-–:]|\s)", t[j + body.end():]) if body else None
        for x in nums:
            if str(x) not in seen:
                seen.add(str(x))
                out.append({"n": str(x), "via": via, "clause": str(text).strip()[:700],
                            "confirmed": bool(lead and int(lead.group(1)) == x)})
    # an operative clause that writes out new article text: its "المادة N :" headings are base-law
    # articles being (re)written, even when the instruction line itself names no number
    for h in HEADING.finditer(t):
        if h.group(1) not in seen and OP_VERB.search(t[: h.start()]):
            seen.add(h.group(1))
            out.append({"n": h.group(1), "via": "عنوان مادة في النص الجديد", "clause": str(text).strip()[:700], "confirmed": True})
    return out


def parse_amended_articles(articles: list[dict], base_names: list[str] | None = None) -> list[str]:
    """Article numbers of the ORIGINAL law this amendment touches (see resolve_targets)."""
    return [d["n"] for d in target_evidence(articles, base_names)]


def target_evidence(articles: list[dict], base_names: list[str] | None = None) -> list[dict]:
    own_nums = {str(a.get("article_number", "")).strip() for a in articles}
    ev, seen = [], set()
    for a in operative(articles):
        for d in resolve_targets(a.get("text", ""), own_nums, base_names):
            if d["n"] not in seen:
                seen.add(d["n"])
                ev.append(d)
    return sorted(ev, key=lambda d: as_int(d["n"]))


def art_map(lst) -> dict[str, str]:
    return {str(a.get("article_number", "")).strip(): squash(a.get("text", "")) for a in (lst or [])}



# ----------------------------------------------------------------------------- extra helpers
AMEND_STOP = {"ذيل", "الذيل", "لذيل", "اضافي", "الاضافي", "بشان", "شان", "الغاء", "الماده", "المادتين", "بتعديل",
              "يعدل", "المعدله", "ثاني", "الثاني", "ثالث", "مشروع", "المؤرخ", "بعض", "مواد", "فقره", "الفقره",
              "لائحه", "قانونيه", "موضوع", "موضوعه", "ذيلا", "قانونا", "والمعدل", "وتعديلات"}


def core(name: str) -> set[str]:
    return {t for t in content_tokens(name) if t not in {stem(x) for x in AMEND_STOP} and t not in AMEND_STOP}


def overlap(a: set, b: set) -> float:
    return len(a & b) / min(len(a), len(b)) if a and b else 0.0


def to_json_date(iso: str) -> str:
    return datetime.strptime(iso, "%Y-%m-%d").strftime("%d-%m-%Y") if iso else ""


def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    return df.apply(lambda s: s.str.strip().replace("NULL", ""))


NEW_TEXT = re.compile(r"(?:بما يلي|بما يأتي|بما ياتي|كما يلي|كما يأتي|كما ياتي|بالصورة التالية|بالنص التالي|بالنص الاتي|التالي نصها|التالية نصها|بالعبارة التالية|بالعبارتين التاليتين|الفقرة التالية|الفقرتين التاليتين|المادة التالية|البند التالي|البندين التاليين|التعريف التالي|التعريفين التاليين)[^:：]{0,80}[:：]\s*[-–]*\s*")
NEXT_STEP = re.compile(r"\n\s*(?:ثانيا|ثالثا|رابعا|خامسا|سادسا|سابعا|ثامنا|تاسعا|عاشرا)\s*[:：\-]")
ART_HEAD = re.compile(r"^\s*(?:ال)?ماد[ةه]\s*\(?\s*\d+\s*(?:مكرر[ةه]?)?\s*\)?\s*[:：\-–]*\s*")


def new_text_blocks(articles: list[dict]) -> list[list[str]]:
    """Replacement/added wording an amendment introduces, as a few squashed probes per block."""
    out = []
    for a in operative(articles):
        t = str(a.get("text", ""))
        for m in NEW_TEXT.finditer(t):
            seg = t[m.end():]
            nxt = NEXT_STEP.search(seg)
            seg = seg[: nxt.start()] if nxt else seg
            seg = re.sub(r"\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{4}", " ", seg)  # drop signing dates
            body = squash(ART_HEAD.sub("", seg))
            probes = [body[o:o + 40] for o in (4, 40, 80) if len(body[o:o + 40]) == 40] or ([body[2:]] if len(body) >= 22 else [])
            if probes:
                out.append(probes)
    return out


def diwan_articles(df: pd.DataFrame, key: str, val: str) -> list[dict]:
    g = df[df[key] == val].copy()
    g["_n"] = pd.to_numeric(g.Article_Number, errors="coerce")
    g = g.sort_values(["_n", "pmk_ID"], key=lambda s: s if s.name == "_n" else s.map(as_int))
    return [{"article_number": n, "title": f"- المادة {n}", "text": re.sub(r"<br\s*/?>", "\n", t).strip()}
            for n, t in zip(g.Article_Number, g.Article)]


# ----------------------------------------------------------------------------- reflection resolver (v2.1)
ORD = {"الاولى": 1, "الأولى": 1, "الثانية": 2, "الثالثة": 3, "الرابعة": 4, "الخامسة": 5, "السادسة": 6, "السابعة": 7,
       "الثامنة": 8, "التاسعة": 9, "العاشرة": 10, "الحادية عشرة": 11, "الثانية عشرة": 12, "الثالثة عشرة": 13,
       "الرابعة عشرة": 14, "الخامسة عشرة": 15, "السادسة عشرة": 16, "السابعة عشرة": 17, "الثامنة عشرة": 18,
       "التاسعة عشرة": 19, "العشرين": 20}
ORD_RX = "|".join(sorted(map(re.escape, ORD), key=len, reverse=True))
TARGET = re.compile(r"(?:ال)?ماد[ةه]\s*\(?\s*(\d+|" + ORD_RX + r")\s*\)?\s*(?:مكرر[ةه]?\s*)?(?:من\s+(?:القانون\s+ال[اأ]صلي|قانون)|منه)")
Q = r"[\(«\"“]\s*(.+?)\s*[\)»\"”]"
OP_REPLACE = re.compile(r"(?:ب?حذف|ب?[اإ]لغاء|ب?شطب)\s+(?:ال)?(?:عبارة|عبارتي|كلمة|كلمتي|رقم|الرقم)\s*" + Q +
                        r"(.{0,200}?)(?:و?يستعاض\s+عنها|و?يستعاض\s+عنه|والاستعاض[ةه]\s+عنها|والاستعاض[ةه]\s+عنه|واستبدالها|ووضع)\s*(?:ب?(?:ال)?(?:عبارة|كلمة|رقم))?\s*(?:التالي[ةه]\s*)?" + Q, re.S)
OP_REPLACE2 = re.compile(r"يستعاض\s+عن\s+(?:ال)?(?:عبارة|كلمة)\s*" + Q + r"([^()«»\"“”]{0,160}?)ب(?:ال)?(?:عبارة|كلمة)\s*" + Q, re.S)
OP_REPLACE3 = re.compile(r"ب?استبدال\s+(?:ال)?(?:عبارة|كلمة|رقم)\s*" + Q + r"([^()«»\"“”]{0,160}?)ب(?:ال)?(?:عبارة|كلمة|رقم)\s*" + Q, re.S)
OP_INSERT = re.compile(r"ب?[اإ]ضاف[ةه]\s+(?:ال)?(?:عبارة|كلمة)\s*" + Q + r"\s*(?:الى\s+ما\s+)?(بعد|قبل)\s+(?:ال)?(?:عبارة|كلمة)\s*" + Q, re.S)
OP_DELETE = re.compile(r"(?:ب?حذف|ب?شطب|ب?[اإ]لغاء)\s+(?:ال)?(?:عبارة|كلمة)\s*" + Q + r"(?!.{0,200}?(?:يستعاض|الاستعاض|استبدال))", re.S)
FULL_ARTICLE = re.compile(r"(?:يلغى|تلغى)\s+(?:نص\s+)?(?:ال)?ماد[ةه]\s*\(?\s*(\d+|" + ORD_RX + r")\s*\)?\s*(?:مكرر[ةه]?\s*)?من\s+القانون\s+ال[اأ]صلي\s*و?يستعاض\s+عنه[اـ]?\s*(?:بالنص\s+التالي|بما\s+يلي|بما\s+ي[اأ]تي)\s*[:：]?\s*(.+)", re.S)
SCHEDULE = re.compile(r"(?:ال)?جدول")
CANCEL_NAME = re.compile(r"الغاء|إلغاء|بطلان|بطلانه")
SIGN = re.compile(r"\n\s*\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{4}.*$|\n\s*(?:رئيس الوزراء|الحسين بن طلال|عبدالله الثاني|فيصل بن الحسين).*$", re.S)


def art_num(tok: str) -> str:
    tok = tok.strip()
    return str(ORD.get(tok, tok)) if not tok.isdigit() else tok


def word_rx(phrase: str) -> str:
    cls = {"ا": "[اأإآ]", "أ": "[اأإآ]", "إ": "[اأإآ]", "آ": "[اأإآ]", "ى": "[يى]", "ي": "[يى]", "ة": "[ةه]", "ه": "[ةه]"}
    words = [w for w in re.split(r"\s+", AR_DIAC.sub("", phrase.strip())) if w]
    return r"\s*".join("".join(cls.get(ch, re.escape(ch)) + "[ً-ْـ]*" for ch in w) for w in words)


def find_all(text: str, phrase: str) -> list:
    return list(re.finditer(word_rx(phrase), text)) if phrase.strip() else []


def instructions(own: list[dict]) -> list[tuple[str, str]]:
    """(target_article, instruction_text) for each operative article of the amendment."""
    out = []
    for a in operative(own):
        t = SIGN.sub("", str(a.get("text", "")))
        ts = resolve_targets(t, set(), None)
        if ts:
            out.append((ts[0]["n"], t))
        else:
            m = TARGET.search(t)
            out.append((art_num(m.group(1)) if m else "", t))
    return out


CLAUSE_SPLIT = re.compile(r"\n|(?=(?:اولا|أولا|ثانيا|ثالثا|رابعا|خامسا|سادسا)\s*[:：\-])")


def near_present(text: str, phrase: str) -> bool:
    """phrase (or a 1-typo variant of it) already in text."""
    import difflib
    p, t = squash(phrase), squash(text)
    if not p:
        return False
    if p in t:
        return True
    n = len(p)
    return any(difflib.SequenceMatcher(None, p, t[i:i + n]).ratio() >= 0.9 for i in range(0, max(1, len(t) - n + 1), 3))


def apply_ops(target_text: str, instr: str, everywhere: bool) -> tuple[str | None, list[str], str]:
    """Apply phrase-level operations clause by clause; returns (new_text, new_phrases, note)."""
    t, news, did = target_text, [], 0
    for clause in [c for c in CLAUSE_SPLIT.split(instr) if c.strip()]:
        done = False
        for rx in (OP_REPLACE, OP_REPLACE2, OP_REPLACE3):
            for m in rx.finditer(clause):
                old, new = m.group(1), m.group(3)
                if near_present(t, new) and not find_all(t, old):
                    done = True
                    continue  # already applied
                hits = find_all(t, old)
                if not hits or (len(hits) > 1 and not everywhere):
                    return None, [], f"old phrase found {len(hits)}x: {old[:40]}"
                t = re.sub(word_rx(old), lambda _m: new, t)
                news.append(new)
                did += 1
                done = True
        for m in OP_INSERT.finditer(clause):
            new, where, anchor = m.group(1), m.group(2), m.group(3)
            done = True
            if near_present(t, new):
                continue  # already applied
            hits = find_all(t, anchor)
            if len(hits) != 1:
                return None, [], f"anchor found {len(hits)}x: {anchor[:40]}"
            h = hits[0]
            t = t[:h.end()] + " " + new + t[h.end():] if where == "بعد" else t[:h.start()] + new + " " + t[h.start():]
            news.append(new)
            did += 1
        if not done:
            for m in OP_DELETE.finditer(clause):
                old = m.group(1)
                hits = find_all(t, old)
                if not hits:
                    continue  # already deleted
                if len(hits) > 1 and not everywhere:
                    return None, [], f"deleted phrase found {len(hits)}x"
                t = re.sub(r"\s*" + word_rx(old), "", t)
                did += 1
    return (t, news, "ok") if did else (None, [], "no phrase operation recognised")


def clean_diwan(t: str) -> str:
    t = re.sub(r"<br\s*/?>", "\n", t or "").strip()
    t = re.sub(r"^.{0,80}?(?:ال)?ماد[ةه]\s*\(?\s*\d+\s*(?:مكرر[ةه]?)?\s*\)?\s*[-:：–]*\s*", "", t, count=1, flags=re.S)
    return t.strip()


def resolve_reflection(own, prev_raw, snap_raw, diwan_v, name, redo=False):
    """Try to settle a flagged reflection. Returns (method, {article: new_text}, note) or None."""
    instr = instructions(own)
    whole = " ".join(t for _, t in instr)
    snap_sq = {k: squash(v.get("text", "")) for k, v in snap_raw.items()}
    snap_all = "".join(snap_sq.values())
    # 1) the amendment's new wording is already in the snapshot -> reflection was fine
    phrases = [m.group(3) for rx in (OP_REPLACE, OP_REPLACE2, OP_REPLACE3) for m in rx.finditer(whole)] + \
              [m.group(1) for m in OP_INSERT.finditer(whole)]
    phrases = [p for p in phrases if len(squash(p)) >= 4]
    def present(p):
        n = next((n for n, t in instr if p in t), "")
        return squash(p) in (snap_sq.get(n, "") if n else snap_all)
    if phrases and all(present(p) for p in phrases):
        return "ok_new_wording_already_present", {}, ", ".join(p[:30] for p in phrases)
    # 1b) generic: every operative clause's new wording (text after ':' / quoted addition) already present
    def new_segments(t):
        segs = []
        for c in [c for c in re.split(r"(?=(?:اولا|أولا|ثانيا|ثالثا|رابعا|خامسا|سادسا)\s*[:：\-])", t) if c.strip()]:
            if re.search(r"[اإ]لغاء|حذف|شطب", c) and not re.search(r"يستعاض|الاستعاض|[اإ]ضاف", c):
                continue
            after = re.split(r"[:：]", c, maxsplit=1)
            if len(after) < 2:
                after = re.split(r"التالي[ةه]?\s+(?:الى\s+[اآ]خرها|اليها|اليه)?\s*[\.\-–]?", c, maxsplit=1)
            cand = after[1] if len(after) == 2 else " ".join(re.findall(r"\(([^()]{12,})\)", c))
            sq = squash(ART_HEAD.sub("", cand))
            if len(sq) >= 20:
                segs.append(sq)
        return segs
    op_clauses = [(n, t) for n, t in instr if not re.search(r"مكلف(?:ون|ان)\s+بت(?:نفيذ|طبيق)", t)]
    segs = [(n, s) for n, t in op_clauses for s in new_segments(t)]
    if segs and all(any(s[i:i + 30] in snap_all for i in (2, len(s) // 2) if len(s[i:i + 30]) >= 20) for _, s in segs):
        return "ok_new_wording_already_present", {}, "generic segments"
    # 2) cancellation / void laws carry no article text change
    if CANCEL_NAME.search(name) and not any(n for n, _ in instr):
        return "ok_cancellation_no_text_change", {}, ""
    # 3) schedule-only amendments (الجدول الملحق) do not edit articles
    ops = [(n, t) for n, t in instr if t.strip() and not re.search(r"مكلف(?:ون|ان)\s+بت(?:نفيذ|طبيق)", t)]
    if ops and all(not n and SCHEDULE.search(t) for n, t in ops):
        return "ok_schedule_change_not_an_article", {}, ""
    if redo and diwan_v:
        return "taken_from_diwan_version", {n: clean_diwan(x) for n, x in diwan_v.items()}, "amendment text was wrong; Diwan versions used"
    edits, notes = {}, []
    # (d) one phrase replaced wherever it occurs in the law
    for n, t in instr:
        g = re.search(r"(?:يستعاض\s+عن|تستبدل\s+ب?)\s*(?:ال)?(?:عبارة|كلمة)\s*" + Q + r"\s*(?:اينما|أينما|حيثما)\s+وردت[^()]{0,120}?ب(?:ال)?(?:عبارة|كلمة)\s*" + Q, t, re.S)
        if g and not re.search(r"سياق", t):
            old, new = g.group(1), g.group(2)
            for k2, a2 in prev_raw.items():
                tx = a2.get("text", "")
                if find_all(tx, old):
                    edits[k2] = re.sub(word_rx(old), lambda _m: new, tx)
            if edits:
                return "global_replacement_applied", edits, f"{old} -> {new}"
    for n, t in instr:
        # (e) append a paragraph/sentence at the end of an article
        ap = re.search(r"(?:تضاف|يضاف|ب?[اإ]ضاف[ةه])\s+(?:ما\s+يلي|الفقر[ةه]\s+التالي[ةه]|العبار[ةه]\s+التالي[ةه]|البند\s+التالي)[^:：]{0,60}?(?:الى\s+)?[اآ]خر(?:ها|\s+الماد[ةه])[^:：]{0,40}[:：]\s*(.+)", t, re.S)
        if ap and n in prev_raw:
            add = ap.group(1).strip().strip("()-– ").strip()
            base_t = prev_raw[n].get("text", "")
            if near_present(base_t, add[:80]):
                continue
            edits[n] = base_t.rstrip() + ("\n" if re.match(r"^[\(]?[اأبجدهوزحطيكل\d][\.\-–)]", add) else " ") + add
            continue
        # (f) a brand-new article N, without renumbering
        na = re.search(r"[اإ]ضاف[ةه]\s+الماد[ةه]\s*\(?\s*(\d+)\s*(?:مكرر[ةه]?)?\s*\)?\s*التالي[ةه][^:：]{0,80}[:：]\s*(.+)", t, re.S)
        if na and not re.search(r"ترقيم", t):
            nn = na.group(1) + (" مكررة" if "مكرر" in t[na.start():na.start(2)] else "")
            if nn not in snap_raw:
                edits[nn] = ART_HEAD.sub("", na.group(2).strip()).strip()
            continue
    if edits:
        return "applied_by_rule_append_or_new_article", edits, ""
    for n, t in instr:
        if not n or n not in prev_raw:
            continue
        base = prev_raw[n].get("text", "")
        m = FULL_ARTICLE.search(t)
        if m and art_num(m.group(1)) == n and not re.search(r"الفقر[ةه]|البند", t[:m.start(2)]):
            body = ART_HEAD.sub("", m.group(2).strip()).strip()
            body = re.sub(r"^\(?\s*" + n + r"\s*[\.\-–:]\s*", "", body).strip()
            if body.startswith("(") and body.endswith(")"):
                body = body[1:-1].strip()
            edits[n] = body
            continue
        new_text, news, note = apply_ops(edits.get(n, base), t, bool(re.search(r"[اأ]ينما\s+وردت|في\s+كل\s+من", t)))
        if new_text is not None:
            if all(squash(x) in squash(new_text) for x in news):
                edits[n] = new_text
        else:
            notes.append(f"art {n}: {note}")
    if edits and not notes:
        import difflib
        if diwan_v and all(n in diwan_v and difflib.SequenceMatcher(
                None, norm_ar(txt).split(), norm_ar(clean_diwan(diwan_v[n])).split(), autojunk=False).ratio() >= 0.9
                for n, txt in edits.items()):
            return "applied_by_rule_confirmed_by_diwan", edits, ""
        return "proposed_by_rule_needs_check", {}, json.dumps(edits, ensure_ascii=False)[:3000]
    # 4) Diwan version of the article carries the amendment's new wording -> take it
    if diwan_v:
        probes = [pr for b in new_text_blocks(own) for pr in b] + [squash(p)[:40] for p in phrases]
        take = {n: clean_diwan(txt) for n, txt in diwan_v.items()
                if probes and any(pr in squash(clean_diwan(txt)) for pr in probes)}
        if take:
            return "taken_from_diwan_version", take, ""
    return None



def apply_manual(dec: dict, prev_raw: dict, snap_raw: dict, diwan_v: dict | None):
    """Apply one case-by-case decision. Returns (status, {art: text}, note); status in ok/edited/vol/failed."""
    a = dec.get("a")
    if a == "ok":
        return "ok", {}, dec.get("note", "")
    if a == "vol":
        return "vol", {}, dec.get("note", "")
    work = {k: v.get("text", "") for k, v in (prev_raw if a == "prev" else snap_raw).items()}
    for op in dec.get("ops", []):
        kind = op[0]
        if kind == "global":
            for old, new in op[1]:
                for k in work:
                    work[k] = re.sub(word_rx(old), lambda _m, n=new: n, work[k])
            continue
        art = op[1]
        if kind == "restore":
            if art not in prev_raw:
                return "failed", {}, f"restore: art {art} not in previous version"
            work[art] = prev_raw[art].get("text", "")
            continue
        if kind == "diwan":
            txt = clean_diwan((diwan_v or {}).get(art, ""))
            if not txt or squash(op[2]) not in squash(txt):
                return "failed", {}, f"diwan art {art} missing or lacks new wording"
            work[art] = txt
            continue
        if art not in work:
            return "failed", {}, f"art {art} not in text"
        t = work[art]
        if kind == "replace":
            old, new = op[2], op[3]
            hits = find_all(t, old)
            if not hits:
                if near_present(t, new):
                    continue
                return "failed", {}, f"old wording not found in art {art}"
            if len(hits) > 1:
                return "failed", {}, f"old wording found {len(hits)}x in art {art}"
            work[art] = re.sub(word_rx(old), lambda _m: new, t)
        elif kind == "delete":
            hits = find_all(t, op[2])
            if len(hits) > 1:
                return "failed", {}, f"phrase found {len(hits)}x in art {art}"
            if hits:
                work[art] = re.sub(r"\s*" + word_rx(op[2]), "", t, count=1)
        elif kind == "insert_after":
            if near_present(t, op[3]) and find_all(t, op[2] + " " + op[3]):
                continue
            hits = find_all(t, op[2])
            if len(hits) != 1:
                return "failed", {}, f"anchor found {len(hits)}x in art {art}"
            h = hits[0]
            work[art] = t[:h.end()] + " " + op[3] + t[h.end():]
        elif kind == "append":
            if not near_present(t, op[2][:80]):
                work[art] = t.rstrip() + "\n" + op[2]
    if dec.get("must_contain") and squash(dec["must_contain"]) not in "".join(squash(x) for x in work.values()):
        return "failed", {}, "required new wording not present"
    base = {k: v.get("text", "") for k, v in snap_raw.items()}
    edits = {k: v for k, v in work.items() if base.get(k) != v}
    if a == "prev":
        edits["__replace_all__"] = "1"
    return ("edited" if edits else "ok"), edits, dec.get("note", "")



# ----------------------------------------------------------------------------- names in Diwan style (v2.2)
def cosmetic(name: str) -> str:
    """Diwan wording kept verbatim; only tatweel and repeated spaces removed (decided 2026-09-23)."""
    return re.sub(r"\s+", " ", str(name or "").replace("ـ", "")).strip()


YEAR_TAIL = re.compile(r"\s*(?:لسن[ةه]\s*)?\(?\s*\d{4}\s*(?:[-/]\s*\d{2,4})?\s*\)?\s*(?:المالي[ةه])?\s*$")
FISCAL = re.compile(r"الموازن[ةه]|الميزاني[ةه]|السن[ةه]\s+المالي[ةه]")


def diwan_style(name: str) -> str:
    """Bring a non-Diwan name to the Diwan pattern (decided 2026-09-23):
    no 'وتعديلاته', no trailing year (except budget laws, where the fiscal year is part of the Diwan name),
    'قانون X [المؤقت] المعدل لسنة Y' -> 'قانون [مؤقت] معدل لقانون X'; old-style titles (تعديل/ذيل) kept."""
    n = cosmetic(name)
    n = re.sub(r"\s*وتعديلاته\s*$", "", n).strip(" .:،")
    w = re.match(r"^قانون\s+(?:م[ؤو]قت\s+)?(?:معدل\s+)?(?:رقم\s*\(?\d+\)?\s*)?لسن[ةه]\s*\d{4}\s*\((.+)\)\s*$", n)
    if w:  # wrapper "قانون [رقم N] لسنة Y (الاسم)" -> الاسم
        n = w.group(1).strip()
    fiscal = bool(FISCAL.search(n))
    m = re.match(r"^(?:ال)?قانون\s+(.+?)\s+((?:ال)?م[ؤو]قت\s+)?المعدل(\s+(?:ال)?م[ؤو]قت)?(?:\s+لسن[ةه]\s*\d{4})?\s*$", n)
    if m and not re.match(r"^(?:ال)?قانون\s+(?:معدل|تعديل|ذيل)", n):
        temp = bool(m.group(2) or m.group(3))
        n = ("قانون مؤقت معدل لقانون " if temp else "قانون معدل لقانون ") + m.group(1).strip()
    m = re.match(r"^القانون\s+المعدل\s+لقانون\s+(.+)$", n)
    if m:
        n = "قانون معدل لقانون " + m.group(1)
    if not fiscal:
        n = YEAR_TAIL.sub("", n).strip(" .:،")
    return n or cosmetic(name)


# ----------------------------------------------------------------------------- load
def load_json(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    flat, full = [], {}
    for i, top in enumerate(data):
        items = [(top, "top", None, -1)] + [(m, "nest", top, j) for j, m in enumerate(top.get("Mod_Legs") or [])]
        for rec, lvl, par, j in items:
            uid = rec["leg_uid"]
            full[uid] = rec
            flat.append({
                "json_uid": uid, "json_level": lvl, "json_top_idx": i, "json_order": j,
                "json_parent_uid": par["leg_uid"] if par else "",
                "source_table": rec.get("source_table", ""), "source_pmk_ID": str(rec.get("source_pmk_ID", "")),
                "j_name": CTRL.sub("", str(rec.get("Leg_Name", "") or "")).strip(),
                "j_name_source": rec.get("Leg_Name_source", ""),
                "j_number": clean_num(rec.get("Leg_Number")), "j_year": str(rec.get("Year", "")).strip(),
                "j_number_src": rec.get("Leg_Number_source", ""), "j_status": rec.get("Status", ""),
                "j_mag_no": clean_num(rec.get("Magazine_Number")), "j_mag_page": clean_num(rec.get("Magazine_Page")),
                "j_mag_date": to_iso(rec.get("Magazine_Date")),
                "j_issue": to_iso(rec.get("Issue_Date")), "j_active": to_iso(rec.get("Active_Date")),
                "j_end": to_iso(rec.get("End_Date")), "j_art_count": str(rec.get("Article_Count", "") or ""),
                "j_has_refl": str(rec.get("has_reflection", "")).lower() == "true",
                "j_canceled_by": rec.get("Canceled_By", "") or "", "j_replaced_by": rec.get("Replaced_By", "") or "",
                "j_replaced_for": rec.get("Replaced_For", "") or "",
            })
    log.info("JSON: %d top-level, %d total records", len(data), len(flat))
    return data, pd.DataFrame(flat), full


def diwan_chains(B: pd.DataFrame) -> dict[str, str]:
    """Diwan stores each law's history as a linked list in UD_leg_Laws:
    base --fnk_leg_Laws10765--> carrier 1 --> carrier 2 ... (carrier = row with ModLeg)."""
    nxt = {p: v for p, v in zip(B.pmk_ID, B.fnk_leg_Laws10765) if v}
    carrier = {p: bool(m) for p, m in zip(B.pmk_ID, B.ModLeg)}
    parent = {}
    for b in B.pmk_ID:
        if carrier[b]:
            continue
        q, seen = nxt.get(b), {b}
        while q and q not in seen and carrier.get(q):
            parent.setdefault(q, b)
            seen.add(q)
            q = nxt.get(q)
    return parent


# ----------------------------------------------------------------------------- 1. link JSON -> CSV
def link(J: pd.DataFrame, C: pd.DataFrame, dpar: dict) -> tuple[pd.DataFrame, list]:
    by_pmk = set(C.pmk_ID)
    by_modleg = defaultdict(list)
    for p, ml in zip(C.pmk_ID, C.ModLeg):
        if ml:
            by_modleg[ml].append(p)
    J = J.copy()
    J["csv_pmk"], J["link_method"] = "", ""
    used: dict[str, str] = {}
    dropped = []  # JSON top records that duplicate a base already represented
    prio = {"UD_leg_Legislative_Amendments": 0}
    order = J.assign(_p=J.source_table.map(prio).fillna(1) + J.json_level.map({"nest": 0, "top": 0.5}))
    pending = []
    for idx in order.sort_values("_p").index:
        r = J.loc[idx]
        sp = r.source_pmk_ID
        if r.source_table == "UD_leg_Legislative_Amendments":
            cands, method = by_modleg.get(sp, []), "modleg_namespace"
        else:
            cands, method = ([sp] if sp in by_pmk else []), "pmk_namespace"
        if not cands:
            cands, method = by_modleg.get(sp, []), "modleg_fallback"
        free = [p for p in cands if p not in used]
        if free:
            J.at[idx, "csv_pmk"], J.at[idx, "link_method"] = free[0], method
            used[free[0]] = r.json_uid
        elif cands and r.json_level == "top":
            pending.append((idx, cands[0]))
    # pass 2: a top record sitting on an amendment's carrier row really is the Diwan base of that chain
    for idx, row_pmk in pending:
        r = J.loc[idx]
        base = dpar.get(row_pmk, "")
        if base and base not in used:
            J.at[idx, "csv_pmk"], J.at[idx, "link_method"] = base, "top_relinked_to_diwan_base"
            used[base] = r.json_uid
        elif base:
            dropped.append({"json_uid": r.json_uid, "into_pmk": base, "reason": "top record duplicates Diwan base " + base})
    return J, dropped


# ----------------------------------------------------------------------------- main build
def build(cfg: dict, out: Path):
    data, J, FULL = load_json(cfg["json"])
    n_in = len(FULL)
    C = read_csv(cfg["csv"])
    assert C.pmk_ID.is_unique, "CSV pmk_ID is not unique"
    B = read_csv(cfg["diwan_laws"]); B = B[B.isDelete == "0"]
    A = read_csv(cfg["diwan_amend"]); A = A[A.isDelete == "0"].set_index("pmk_ID", drop=False)
    AA = read_csv(cfg["diwan_amend_art"]); AA = AA[AA.isDelete == "0"]
    # DIWAN_LAW_ARTICLES_PATH may list several exports separated by ';' (e.g. 117.csv;art.csv)
    LA = pd.concat([read_csv(Path(x.strip())) for x in str(cfg["diwan_law_art"]).split(";") if x.strip()], ignore_index=True)
    LA = LA.drop_duplicates("pmk_ID")
    LA = LA[(LA.isDelete == "0") & (LA.ModLeg == "")]
    log.info("CSV %d rows | Diwan: laws %d, amendments %d, amendment articles %d, law articles %d",
             len(C), len(B), len(A), len(AA), len(LA))
    VV = read_csv(cfg["diwan_versions"])
    VV = VV[(VV.isDelete == "0") & (VV.fnk_leg_Laws10689 != "") & (VV.ModLeg != "")]
    diwan_versions = {ml: dict(zip(g.Article_Number, g.Article)) for ml, g in VV.groupby("ModLeg")}
    log.info("Diwan article versions (laws): %d rows for %d amendments", len(VV), len(diwan_versions))
    dpar = diwan_chains(B)
    in_diwan_base = set(B[B.ModLeg == ""].pmk_ID)

    J, json_dropped = link(J, C, dpar)
    rep: dict[str, pd.DataFrame] = {}
    Jl = J[J.csv_pmk != ""].set_index("csv_pmk")
    rep["json_unlinked"] = J[(J.csv_pmk == "") & ~J.json_uid.isin([d["json_uid"] for d in json_dropped])]
    log.info("Linked %d JSON records | %d JSON top records are duplicates of a Diwan base | %d unlinked",
             len(Jl), len(json_dropped), len(rep["json_unlinked"]))

    D = C.merge(Jl, left_on="pmk_ID", right_index=True, how="left")
    D["in_json"] = D.json_uid.notna()
    D = D.fillna("")
    flags: dict[str, list[str]] = defaultdict(list)

    def flag(p, f):
        if f not in flags[p]:
            flags[p].append(f)

    # ------------------------------------------------------------------ 2. fields
    conflicts = []

    def pick(row, field, csv_v, json_v, json_wins: bool):
        if row.in_json and csv_v and json_v and csv_v != json_v:
            conflicts.append({"pmk_ID": row.pmk_ID, "field": field, "csv": csv_v, "json": json_v,
                              "chosen": json_v if json_wins else csv_v})
        return (json_v or csv_v) if json_wins else (csv_v or json_v)

    global ISSUE_DATES
    ISSUE_DATES = defaultdict(lambda: defaultdict(int))
    for r in D.itertuples(index=False):
        nc, nj, dc, dj = clean_num(r.Magazine_Number), r.j_mag_no, to_iso(r.Magazine_Date), r.j_mag_date
        if (nc == nj or not nj) and (dc == dj or not dj) and nc and dc:
            ISSUE_DATES[nc][dc] += 1
        elif not nc and nj and dj:
            ISSUE_DATES[nj][dj] += 1

    B_names = {p: n for p, n, ml in zip(B.pmk_ID, B.Law_Name, B.ModLeg) if not ml}
    name_conflicts = []
    recs = []
    for r in D.itertuples(index=False):
        p = r.pmk_ID
        dn, dsrc = "", ""
        if r.ModLeg and r.ModLeg in A.index and A.at[r.ModLeg, "amendment"]:
            dn, dsrc = cosmetic(A.at[r.ModLeg, "amendment"]), "diwan:UD_leg_Legislative_Amendments.amendment"
        elif not r.ModLeg and p in B_names and B_names[p]:
            dn, dsrc = cosmetic(B_names[p]), "diwan:UD_leg_Laws.Law_Name"
        ours = r.j_name or r.Law_Name
        if dn and ours and overlap(core(dn), core(ours)) < 0.3 and overlap(content_tokens(dn), content_tokens(ours)) < 0.3:
            # the Diwan row this record points to names a different law -> keep our name, flag it
            name_conflicts.append({"pmk_ID": p, "ModLeg": r.ModLeg, "our_name": ours, "diwan_name": dn})
            flag(p, "diwan_name_is_a_different_law")
            dn = ""
        if dn:
            name, nsrc = dn, dsrc
        elif r.in_json and r.j_name:
            name, nsrc = diwan_style(r.j_name), "diwan_style_from:json"
        else:
            if r.in_json:
                flag(p, "json_name_corrupted_or_empty_used_csv_rule")
            if not r.ModLeg and r.Law_Name_original and not WRAPPER.match(r.Law_Name_original):
                name, nsrc = r.Law_Name_original, "csv_diwan_short_name"
            else:
                s = strip_wrapper(r.Law_Name)
                name, nsrc = (s, "fallback_wrapper_stripped") if s else (r.Law_Name, "csv_law_name_asis")
            name, nsrc = diwan_style(name), "diwan_style_from:" + nsrc
        if nsrc.startswith("diwan_style") and name.count("(") != name.count(")"):
            flag(p, "name_incomplete_in_source")
        number = pick(r, "Leg_Number", clean_num(r.Law_Number), r.j_number, bool(r.j_number_src))
        cy, jy = r.Year.strip(), r.j_year
        if r.in_json and cy != jy:
            conflicts.append({"pmk_ID": p, "field": "Year", "csv": cy, "json": jy, "chosen": cy if valid_year(cy) else jy})
        year = cy if valid_year(cy) else jy
        if not valid_year(year):
            flag(p, "invalid_year")
        # gazette/dates: JSON wins (measured: issue-date consensus 142 vs 37, article-1 wording 1099 vs 11,
        # Canceled_By date 121 vs 2); gazette number/date pair chosen by issue->date consensus
        nc, nj, dc, dj = clean_num(r.Magazine_Number), r.j_mag_no, to_iso(r.Magazine_Date), r.j_mag_date
        combos = [(n, t) for n in (nj, nc) if n for t in (dj, dc) if t]
        best = max(combos, key=lambda x: ISSUE_DATES[x[0]][x[1]], default=(nj or nc, dj or dc))
        if not combos or ISSUE_DATES[best[0]][best[1]] == 0:
            best = (nj or nc, dj or dc)
        mag_no = pick(r, "Magazine_Number", nc, nj, best[0] == nj)
        mag_dt = pick(r, "Magazine_Date", dc, dj, best[1] == dj)
        mag_pg = pick(r, "Magazine_Page", clean_num(r.Magazine_Page_Number), r.j_mag_page, True)
        active = pick(r, "Active_Date", to_iso(r.Active_Date_final) or to_iso(r.Active_Date), r.j_active, True)
        end = pick(r, "End_Date", to_iso(r.End_Date_final) or to_iso(r.end_date), r.j_end, True)
        # status: Diwan code; RULE (decided 2026-09-23): any End_Date => غير ساري
        code = r.Status.strip()
        status = {"1": "ساري", "2": "غير ساري"}.get(code, "") or r.j_status
        if not code:
            flag(p, "status_missing_in_diwan_csv")
        status_rule = ""
        if end and status != "غير ساري":
            status, status_rule = "غير ساري", "end_date_implies_inactive"
        if r.in_json and r.j_status and r.j_status != status:
            conflicts.append({"pmk_ID": p, "field": "Status", "csv": status, "json": r.j_status, "chosen": status})
        if status == "غير ساري" and not end:
            flag(p, "status_inactive_without_end_date")
        if active and end and end < active:
            flag(p, "end_before_active")
        recs.append({
            "pmk_ID": p, "ModLeg": r.ModLeg, "json_leg_uid": r.json_uid,
            "Leg_Name": name, "name_source": nsrc, "Law_Name_csv_original": r.Law_Name, "_json_name": r.j_name,
            "Leg_Number": number, "Year": year, "is_temporary": "مؤقت" in (r.Law_Name + r.Law_Name_original + name),
            "Status": status, "Status_code": {"ساري": "1", "غير ساري": "2"}.get(status, ""), "status_rule": status_rule,
            "Issue_Date": r.j_issue, "Magazine_Number": mag_no, "Magazine_Page": mag_pg, "Magazine_Date": mag_dt,
            "Active_Date": active, "End_Date": end, "Article_Count": r.j_art_count,
            "Canceled_By": r.j_canceled_by, "Replaced_By": r.j_replaced_by, "Replaced_For": r.j_replaced_for or r.Replaced_For,
            "entity": r.entity_final, "parent_ministry": r.parent_ministry, "entity_type": r.type, "scope": r.scope,
            "in_json": r.in_json, "row_origin": r.row_origin,
            "_lvl": r.json_level, "_uid": r.json_uid, "_parent_uid": r.json_parent_uid, "_order": r.json_order,
            "_top_idx": r.json_top_idx, "_chain": r.chain_id_v2, "_pos": as_int(r.chain_position_v2),
            "_has_refl": r.j_has_refl, "_nums": {clean_num(r.Law_Number), r.j_number} - {""}, "_years": {cy, jy} - {""},
        })
    R = pd.DataFrame(recs).set_index("pmk_ID", drop=False)
    rep["field_conflicts_csv_vs_json"] = pd.DataFrame(conflicts)
    rep["names_diwan_points_to_other_law"] = pd.DataFrame(name_conflicts)
    log.info("Status set to غير ساري by the End_Date rule: %d", (R.status_rule != "").sum())

    # ------------------------------------------------------------------ 3a. duplicate rows: same Diwan amendment
    removed = []
    for ml, g in R[R.ModLeg != ""].groupby("ModLeg"):
        if len(g) < 2:
            continue
        keep = g[g.in_json]
        if len(keep) == 1:
            k = keep.iloc[0]
            for p, row in g.drop(k.pmk_ID).iterrows():
                if (row.Magazine_Number, row.Magazine_Page) == (k.Magazine_Number, k.Magazine_Page):
                    removed.append({"removed_pmk_ID": p, "kept_pmk_ID": k.pmk_ID, "Leg_Name": row.Leg_Name,
                                    "evidence": f"same ModLeg {ml} + same gazette {k.Magazine_Number}/{k.Magazine_Page}; kept row is in JSON"})
                else:
                    flag(p, f"same_modleg_as_{k.pmk_ID}_different_gazette")
        else:
            for p in g.pmk_ID:
                flag(p, "duplicate_modleg_unresolved")
    R = R.drop([x["removed_pmk_ID"] for x in removed])
    alias: dict[str, str] = {x["removed_pmk_ID"]: x["kept_pmk_ID"] for x in removed}

    # ------------------------------------------------------------------ 3b. duplicate BASE laws (same law twice)
    def arts_of(p):
        u = R.at[p, "_uid"] if p in R.index else ""
        return (FULL[u].get("Base_Articles") or []) if u else []

    baseish = R[(R.ModLeg == "") & (R._lvl != "nest")]
    merged = []
    for (y, mag), g in baseish[baseish.Magazine_Number != ""].groupby(["Year", "Magazine_Number"]):
        if len(g) < 2:
            continue
        ids = list(g.pmk_ID)
        for i, a in enumerate(ids):
            for b in ids[i + 1:]:
                if a in alias or b in alias:
                    continue
                ta, tb = content_tokens(R.at[a, "Leg_Name"]), content_tokens(R.at[b, "Leg_Name"])
                if not ta or not tb or len(ta & tb) / len(ta | tb) < 0.8:
                    continue  # names must be essentially identical
                pa, pb = R.at[a, "Magazine_Page"], R.at[b, "Magazine_Page"]
                if pa and pb and pa != pb:
                    continue  # different page in the same issue -> could be two laws
                aa, ab = arts_of(a), arts_of(b)
                if aa and ab and jaccard(aa[0].get("text", ""), ab[0].get("text", "")) < 0.5:
                    continue  # both have text and the texts differ -> not proven the same law
                rank = lambda p: ((p in in_diwan_base), R.at[p, "in_json"], len(arts_of(p)), -as_int(p))
                keep, drop = (a, b) if rank(a) >= rank(b) else (b, a)
                alias[drop] = keep
                merged.append({"dropped_pmk_ID": drop, "kept_pmk_ID": keep, "Leg_Name_kept": R.at[keep, "Leg_Name"],
                               "Leg_Name_dropped": R.at[drop, "Leg_Name"], "Year": y, "Magazine_Number": mag,
                               "pages": f"{R.at[keep, 'Magazine_Page']} / {R.at[drop, 'Magazine_Page']}",
                               "kept_in_diwan": keep in in_diwan_base, "dropped_origin": R.at[drop, "row_origin"],
                               "articles_moved_to_kept": bool(ab if drop == b else aa) and not arts_of(keep)})
    for d in json_dropped:
        alias.setdefault("json:" + d["json_uid"], d["into_pmk"])
    removed += [{"removed_pmk_ID": m["dropped_pmk_ID"], "kept_pmk_ID": m["kept_pmk_ID"], "Leg_Name": m["Leg_Name_dropped"],
                 "evidence": f"same law: same year {m['Year']} + gazette {m['Magazine_Number']} + same name"} for m in merged]
    rep["removed_duplicate_rows"] = pd.DataFrame(removed)
    rep["merged_duplicate_base_laws"] = pd.DataFrame(merged)
    merged_articles = {m["kept_pmk_ID"]: m["dropped_pmk_ID"] for m in merged if m["articles_moved_to_kept"]}
    R_dropped = R.loc[[m["dropped_pmk_ID"] for m in merged]]
    R = R.drop(R_dropped.index)
    log.info("Removed %d duplicate amendment rows and merged %d duplicate base laws", len(removed) - len(merged), len(merged))

    def resolve(p):
        seen = set()
        while p in alias and p not in seen:
            seen.add(p)
            p = alias[p]
        return p

    uid2pmk = {u: p for p, u in zip(R.pmk_ID, R._uid) if u}
    for p, u in zip(R_dropped.pmk_ID, R_dropped._uid):
        if u:
            uid2pmk[u] = resolve(p)
    for d in json_dropped:
        k = uid2pmk[d["json_uid"]] = resolve(d["into_pmk"])
        if k in R.index and not arts_of(k) and FULL[d["json_uid"]].get("Base_Articles"):
            merged_articles[k] = "json:" + d["json_uid"]

    # ------------------------------------------------------------------ 4. article text: check + Diwan replacement
    R["articles_check"], R["articles_source"] = "not_in_json", ""
    replaced, art_review, new_articles, filled = [], [], {}, []
    for p, u in zip(R.pmk_ID, R._uid):
        if not u:
            continue
        names = [R.at[p, "Leg_Name"], R.at[p, "Law_Name_csv_original"]]
        arts = FULL[u].get("Base_Articles") or []
        if not arts and p in merged_articles:
            src = merged_articles[p]
            src_uid = src[5:] if src.startswith("json:") else R_dropped.at[src, "_uid"]
            arts = arts_of_dropped = FULL[src_uid].get("Base_Articles") or []
            new_articles[p] = arts_of_dropped
            R.at[p, "articles_source"] = "moved_from_merged_duplicate"
        if not arts and "diwan_name_is_a_different_law" not in flags[p]:
            # no article text at all -> take the Diwan's own articles for this exact record (v2.3)
            ml = R.at[p, "ModLeg"]
            d_arts = diwan_articles(AA, "fnk_leg_Legislative_Amendments10747", ml) if ml else []
            d_arts = d_arts or diwan_articles(LA, "fnk_leg_Laws10689", p)
            if d_arts and articles_check(d_arts, names)[0] in ("ok", "no_title_in_article1"):
                arts = new_articles[p] = d_arts
                R.at[p, "articles_source"] = "diwan_filled_missing_articles"
                filled.append({"pmk_ID": p, "ModLeg": ml, "Leg_Name": R.at[p, "Leg_Name"], "articles": len(d_arts),
                               "article1": naming_article(d_arts)[:200]})
        chk, _ = articles_check(arts, names)
        if chk == "articles_title_mismatch" and "diwan_name_is_a_different_law" in flags[p]:
            flag(p, "articles_title_mismatch")
            art_review.append({"pmk_ID": p, "ModLeg": R.at[p, "ModLeg"], "record_type_hint": "",
                               "Leg_Name": R.at[p, "Leg_Name"], "json_article1": naming_article(arts)[:250],
                               "diwan_article1": "", "diwan_law_name": "", "diwan_text_check": "skipped: Diwan row is another law"})
        elif chk == "articles_title_mismatch":
            ml = R.at[p, "ModLeg"]
            d_arts = diwan_articles(AA, "fnk_leg_Legislative_Amendments10747", ml) if ml else []
            d_arts = d_arts or diwan_articles(LA, "fnk_leg_Laws10689", p)
            dchk, _ = articles_check(d_arts, names) if d_arts else ("no_articles", 0)
            if d_arts and (dchk == "ok" or (ml and dchk == "no_title_in_article1")):
                new_articles[p] = d_arts
                chk = "replaced_from_diwan"
                R.at[p, "articles_source"] = "diwan_amendment_articles" if ml else "diwan_law_articles"
                replaced.append({"pmk_ID": p, "ModLeg": ml, "Leg_Name": R.at[p, "Leg_Name"],
                                 "old_article1": naming_article(arts)[:200], "new_article1": naming_article(d_arts)[:200],
                                 "diwan_title_check": dchk})
            else:
                flag(p, "articles_title_mismatch")
                art_review.append({"pmk_ID": p, "ModLeg": ml, "record_type_hint": "amendment" if ml else "base",
                                   "Leg_Name": R.at[p, "Leg_Name"], "json_article1": naming_article(arts)[:250],
                                   "diwan_article1": naming_article(d_arts)[:250] if d_arts else "",
                                   "diwan_law_name": B.set_index("pmk_ID").Law_Name.get(p, ""),
                                   "diwan_text_check": dchk})
        R.at[p, "articles_check"] = chk
    rep["articles_replaced_from_diwan"] = pd.DataFrame(replaced)
    rep["articles_filled_from_diwan"] = pd.DataFrame(filled)
    rep["articles_wrong_needs_review"] = pd.DataFrame(art_review)
    art_now = lambda p: new_articles.get(p) or arts_of(p)

    # ------------------------------------------------------------------ 5. sequence (evidence scoring)
    heads = C[C.chain_position_v2 == "0"].set_index("chain_id_v2").pmk_ID.to_dict()
    is_base = lambda p: p in R.index and R.at[p, "ModLeg"] == "" and R.at[p, "_lvl"] != "nest"
    bases = [p for p in R.pmk_ID if is_base(p)]
    base_core = {b: core(R.at[b, "Leg_Name"]) for b in bases}
    base_by_numyear = defaultdict(set)
    for b in bases:
        for n in R.at[b, "_nums"] | {R.at[b, "Leg_Number"]}:
            for y in R.at[b, "_years"] | {R.at[b, "Year"]}:
                base_by_numyear[(n, y)].add(b)
    tok_index = defaultdict(set)
    for b, cs in base_core.items():
        for t in cs:
            tok_index[t].add(b)

    def date_of(p):
        return R.at[p, "Magazine_Date"] or R.at[p, "Active_Date"] or (R.at[p, "Year"] + "-12-31" if valid_year(R.at[p, "Year"]) else "")

    def to_base(p):
        p = resolve(p)
        if p in R.index and not is_base(p) and p in dpar:
            p = resolve(dpar[p])
        return p if is_base(p) else ""

    parent, evidence, conf, seq_rows = {}, {}, {}, []
    for p, row in R.iterrows():
        diwan_carrier = p in dpar
        amendment_signal = row._lvl == "nest" or bool(row.ModLeg) or diwan_carrier or (row._lvl == "" and row._pos > 0)
        if not amendment_signal:
            continue
        votes = defaultdict(list)
        pj = to_base(uid2pmk.get(row._parent_uid, "")) if row._lvl == "nest" else ""
        pc = to_base(heads.get(row._chain, "")) if heads.get(row._chain, "") != p else ""
        pd_ = to_base(dpar.get(p, ""))
        for src, c in (("json", pj), ("csv", pc), ("diwan", pd_)):
            if c and c != p:
                votes[c].append(src)
        rf = parse_parent_ref(art_now(p)) if R.at[p, "articles_check"] in ("ok", "replaced_from_diwan") else None
        if rf:
            for c in base_by_numyear.get(rf, ()):
                if c != p:
                    votes[c].append("article1_text")
        ac = core(row.Leg_Name)
        if ac:
            cand = defaultdict(int)
            for t in ac:
                for b in tok_index.get(t, ()):
                    cand[b] += 1
            for b, n in cand.items():
                if b != p and base_core[b] and n / len(base_core[b] | ac) >= 0.6:
                    votes[b].append("name")
        if not votes:
            flag(p, "orphan_amendment_no_parent_evidence")
            seq_rows.append({"pmk_ID": p, "Leg_Name": row.Leg_Name, "chosen_parent": "", "evidence": "", "confidence": "none"})
            continue
        pdate = date_of(p)
        scored = []
        for c, srcs in votes.items():
            if "name" not in srcs and ac and base_core.get(c) and len(ac & base_core[c]) / len(ac | base_core[c]) >= 0.6:
                srcs.append("name")
            name_ok = bool(ac and base_core.get(c) and overlap(ac, base_core[c]) >= 0.5)
            # the article-1 number/year is strong only when the named law is also the right subject
            w = {"json": 1, "csv": 1, "diwan": 1.5, "article1_text": 3 if name_ok else 0.5, "name": 1}
            s = sum(w[x] for x in srcs)
            cdate = date_of(c)
            if pdate and cdate and pdate < cdate:
                s -= 10
                srcs.append("DATE_BEFORE_BASE")
            scored.append((s, c, srcs))
        # successive laws with the same name: prefer the latest one in force before the amendment
        same_name = [x for x in scored if "name" in x[2] and "DATE_BEFORE_BASE" not in x[2]]
        if len(same_name) > 1:
            latest = max(same_name, key=lambda x: date_of(x[1]))
            scored = [(s + 0.5 if c == latest[1] else s, c, srcs) for s, c, srcs in scored]
        scored.sort(key=lambda x: (-x[0], as_int(x[1])))
        s1, c1, srcs1 = scored[0]
        s2 = scored[1][0] if len(scored) > 1 else -99
        strong = bool({"article1_text", "name"} & set(srcs1)) and bool({"json", "csv", "diwan"} & set(srcs1))
        n_src = len({"json", "csv", "diwan"} & set(srcs1))
        c_ = "high" if s1 - s2 >= 1 and (strong or n_src >= 2) else "medium" if s1 - s2 >= 0.5 else "low"
        if s1 < 0:
            c_ = "low"
        parent[p], evidence[p], conf[p] = c1, "+".join(sorted(set(srcs1))), c_
        if c_ == "low":
            flag(p, "parent_low_confidence")
        if "DATE_BEFORE_BASE" in srcs1:
            flag(p, "amendment_published_before_its_base")
        seq_rows.append({"pmk_ID": p, "Leg_Name": row.Leg_Name, "json_parent": pj, "csv_parent": pc, "diwan_parent": pd_,
                         "article1_ref": f"{rf[0]}/{rf[1]}" if rf else "", "chosen_parent": c1,
                         "chosen_parent_name": R.at[c1, "Leg_Name"], "evidence": evidence[p], "confidence": c_,
                         "runner_up": f"{scored[1][1]} ({R.at[scored[1][1], 'Leg_Name'][:40]}) {'+'.join(scored[1][2])}" if len(scored) > 1 else "",
                         "changed_vs_json": bool(pj) and pj != c1})
    rep["sequence_decisions"] = pd.DataFrame(seq_rows)

    for p in list(parent):  # a chosen parent that is itself an amendment -> climb to its root base
        seen, q = {p}, parent[p]
        while q in parent and q not in seen:
            seen.add(q)
            q = parent[q]
        if q != parent[p]:
            flag(p, "parent_was_amendment_climbed_to_root")
            parent[p] = q
    R["record_type"] = ["amendment" if p in parent or "orphan_amendment_no_parent_evidence" in flags[p] else "base" for p in R.pmk_ID]
    R["parent_pmk_ID"] = [parent.get(p, "") for p in R.pmk_ID]
    R["chain_evidence"] = [evidence.get(p, "base" if t == "base" else "none") for p, t in zip(R.pmk_ID, R.record_type)]
    R["chain_confidence"] = [conf.get(p, "" if t == "base" else "none") for p, t in zip(R.pmk_ID, R.record_type)]
    R["chain_id"] = [parent.get(p, p) for p in R.pmk_ID]

    def order_key(p):
        return (date_of(p) or "9999", as_int(R.at[p, "Magazine_Number"]), as_int(R.at[p, "Leg_Number"]), as_int(p))

    R["chain_position"], R["chain_length"], R["prev_pmk_ID"], R["next_pmk_ID"] = 0, 1, "", ""
    chains = {}
    for cid, g in R.groupby("chain_id"):
        seq = ([cid] if cid in R.index else []) + sorted([p for p in g.pmk_ID if p != cid], key=order_key)
        chains[cid] = seq
        for i, p in enumerate(seq):
            R.at[p, "chain_position"], R.at[p, "chain_length"] = i, len(seq)
            R.at[p, "prev_pmk_ID"] = seq[i - 1] if i else ""
            R.at[p, "next_pmk_ID"] = seq[i + 1] if i + 1 < len(seq) else ""
            if i and not R.at[p, "Magazine_Date"] and not R.at[p, "Active_Date"]:
                flag(p, "order_by_year_only_no_dates")

    # ------------------------------------------------------------------ 6. reflections: fix article 1 + checks
    R["has_reflection"], R["amended_articles"], R["reflection_check"] = False, "", "not_applicable"
    new_refl, refl_rows, vol_rows, fixed, auto_res, manual_notes = {}, [], [], [], [], {}
    for cid, seq in chains.items():
        corr: dict[str, tuple[str, str]] = {}  # article -> (wrong text squashed, corrected text) to carry forward
        prev_raw = {str(a.get("article_number", "")).strip(): a for a in art_now(cid)} if cid in R.index else {}
        base_a1 = ART_HEAD.sub("", prev_raw.get("1", {}).get("text", "")).strip()
        for p in seq[1:]:
            u = R.at[p, "_uid"]
            if not u:
                R.at[p, "reflection_check"] = "not_in_json"
                continue
            rec = FULL[u]
            own = art_now(p)
            base_names = [R.at[cid, "Leg_Name"], R.at[p, "Leg_Name"]] if cid in R.index else None
            tev = target_evidence(own, base_names)
            amended = [d["n"] for d in tev]
            R.at[p, "amended_articles"] = "|".join(amended)
            has = bool(R.at[p, "_has_refl"])
            R.at[p, "has_reflection"] = has
            snap_list = [dict(a) for a in (rec.get("Reflected_Articles") or [])]
            if not has:
                R.at[p, "reflection_check"] = "no_reflection_declared"
                continue
            if not snap_list:
                chk = "declared_but_snapshot_missing"
            else:
                snap_raw = {str(a.get("article_number", "")).strip(): a for a in snap_list}
                s1 = squash(snap_raw.get("1", {}).get("text", ""))
                own_a1 = squash(naming_article(own))
                if s1 and "1" not in amended and (s1 == own_a1 or squash("ويقرا مع") in s1) and "1" in prev_raw:
                    # FIX (approved): restore article 1 from the previous version, touch nothing else
                    for a in snap_list:
                        if str(a.get("article_number", "")).strip() == "1":
                            fixed.append({"pmk_ID": p, "chain_id": cid, "wrong_article1": a.get("text", "")[:200],
                                          "restored_article1": prev_raw["1"].get("text", "")[:200]})
                            a["text"] = prev_raw["1"].get("text", "")
                    new_refl[p] = snap_list
                    snap_raw = {str(a.get("article_number", "")).strip(): a for a in snap_list}
                # carry forward corrections made on earlier amendments of this chain
                touched = {n for n, _ in instructions(own) if n}
                for n in list(corr):
                    if n in touched:
                        corr.pop(n)
                    elif n in snap_raw and squash(snap_raw[n].get("text", "")) == corr[n][0]:
                        snap_raw[n]["text"] = corr[n][1]
                        new_refl[p] = snap_list
                snap = {k: squash(v.get("text", "")) for k, v in snap_raw.items()}
                prev = {k: squash(v.get("text", "")) for k, v in prev_raw.items()}
                changed = {k for k in set(snap) | set(prev) if snap.get(k) != prev.get(k)}
                hit = [a for a in amended if a in changed]
                # word-level similarity of article 1 (fixed in v2.3.1: was computed on squashed strings)
                sim = jaccard(ART_HEAD.sub("", snap_raw.get("1", {}).get("text", "")), base_a1) if base_a1 else 1.0
                # range-repeal check (v2.4.0): a full replacement law can repeal a whole block of
                # the base law's articles in one dedicated article (e.g. "تلغى ... من (290) ولغاية
                # (477)"). Checked first and, when it fires, takes priority over the generic
                # phrase-matching checks below: those assume a short surgical amendment and produce
                # noise on a long replacement law (proven on pmk 3219 - see parse_amended_articles).
                range_lo_hi = None
                for a in own[1:]:
                    t = str(a.get("text", ""))
                    m = RANGE_REPEAL.search(t)
                    # exclude: (a) range REPLACED with new text ("يستعاض") - not a repeal;
                    # (b) range RENUMBERED ("ترقيم", e.g. pmk 3089: "تلغى المادة 4 ... ويعاد ترقيم
                    # المواد من 5 الى 15 ... لتصبح من 4 الى 14") - the articles still exist, just
                    # renumbered, so they are never "empty" and would wrongly show as exceptions.
                    if m and not re.search(r"يستعاض|ترقيم", t):
                        lo, hi = int(m.group(1)), int(m.group(2))
                        if 0 < hi - lo < 500:
                            range_lo_hi = (lo, hi)
                            break
                # strongest check: is the NEW wording written by the amendment present in the snapshot?
                blocks = new_text_blocks(own)
                snap_all = "".join(snap.values())
                found = sum(any(pr in snap_all for pr in b) for b in blocks)
                if range_lo_hi:
                    lo, hi = range_lo_hi
                    range_exceptions = [str(n) for n in range(lo, hi + 1) if snap.get(str(n), "").strip()]
                    if range_exceptions:
                        chk = "range_repeal_has_exceptions"
                        amended = range_exceptions  # only the exceptions need a volunteer's eyes
                        manual_notes[p] = (f"إلغاء جماعي: المواد من {lo} إلى {hi} من القانون الأصلي يُفترض إلغاؤها بهذا "
                                          f"التعديل، لكن {len(range_exceptions)} مادة منها ما زالت تظهر بنص عندنا: "
                                          f"{'، '.join(range_exceptions[:15])}{' ...' if len(range_exceptions) > 15 else ''}.")
                    else:
                        chk = "ok"  # all articles in the repealed range are already empty - confirmed, no queue entry
                elif blocks and found >= 1:
                    chk = "fixed_article1_ok" if p in new_refl else "ok"
                elif blocks:
                    chk = "new_text_missing_in_snapshot"
                elif prev and not changed:
                    chk = "identical_to_previous_version"
                elif base_a1 and sim < 0.3:
                    chk = "snapshot_may_belong_to_other_law"
                elif amended and not hit and prev:
                    chk = "amended_articles_unchanged_in_snapshot"
                else:
                    chk = "fixed_article1_ok" if p in new_refl else "ok"
                if chk not in ("ok", "fixed_article1_ok", "range_repeal_has_exceptions") and R.at[p, "articles_source"] == "diwan_amendment_articles":
                    chk = "redo_amendment_text_was_wrong"
                # range-repeal rows skip the generic phrase-matching engine below: it looks for
                # surgical replace/insert/delete operations, which a repeal-a-block article has
                # none of, and running it over the amendment's other (unrelated) own articles could
                # spuriously "resolve" this row using a phrase edit that has nothing to do with it.
                if chk not in ("ok", "fixed_article1_ok", "range_repeal_has_exceptions"):
                    res = resolve_reflection(own, prev_raw, snap_raw, diwan_versions.get(R.at[p, "ModLeg"]), R.at[p, "Leg_Name"],
                                             redo=R.at[p, "articles_source"] == "diwan_amendment_articles")
                    if res:
                        method, edits, note = res
                        for n, txt in edits.items():
                            old = snap_raw[n].get("text", "") if n in snap_raw else ""
                            if n in snap_raw:
                                snap_raw[n]["text"] = txt
                            else:
                                obj = {"text": txt, "title": f"- المادة {n}", "article_number": n}
                                snap_list.append(obj)
                                snap_raw[n] = obj
                            corr[n] = (squash(old), txt)
                            auto_res.append({"pmk_ID": p, "chain_id": cid, "problem": chk, "method": method, "article": n,
                                             "before": old[:1500], "after": txt[:1500]})
                        if edits:
                            new_refl[p] = snap_list
                        else:
                            auto_res.append({"pmk_ID": p, "chain_id": cid, "problem": chk, "method": method, "article": "",
                                             "before": note, "after": ""})
                        chk = "resolved_" + method if method != "proposed_by_rule_needs_check" else chk
                if (chk not in ("ok", "fixed_article1_ok", "range_repeal_has_exceptions") and not chk.startswith("resolved_")
                        and p in MANUAL_REFLECTIONS):
                    st, edits, note = apply_manual(MANUAL_REFLECTIONS[p], prev_raw, snap_raw, diwan_versions.get(R.at[p, "ModLeg"]))
                    if st == "edited":
                        old_texts = {k: v.get("text", "") for k, v in snap_raw.items()}
                        if edits.pop("__replace_all__", None):
                            # snapshot belonged to another law: rebuild from the previous version
                            snap_list = [dict(a, text=edits.get(k, a.get("text", ""))) for k, a in prev_raw.items()]
                            snap_raw = {str(a.get("article_number", "")).strip(): a for a in snap_list}
                            edits = {k: snap_raw[k]["text"] for k in snap_raw}
                        for n, txt in edits.items():
                            old = old_texts.get(n, "")
                            if n in snap_raw:
                                snap_raw[n]["text"] = txt
                            else:
                                obj = {"text": txt, "title": f"- المادة {n}", "article_number": n}
                                snap_list.append(obj)
                                snap_raw[n] = obj
                            if squash(old) != squash(txt):
                                corr[n] = (squash(old), txt)
                                auto_res.append({"pmk_ID": p, "chain_id": cid, "problem": chk, "method": "manual_decision",
                                                 "article": n, "before": old[:1500], "after": txt[:1500]})
                        new_refl[p] = snap_list
                        chk = "resolved_manual_edit"
                    elif st == "ok":
                        auto_res.append({"pmk_ID": p, "chain_id": cid, "problem": chk, "method": "manual_ok",
                                         "article": "", "before": note, "after": ""})
                        chk = "resolved_manual_ok"
                    else:
                        manual_notes[p] = note
                refl_rows.append({"pmk_ID": p, "chain_id": cid, "position": R.at[p, "chain_position"],
                                  "Leg_Name": R.at[p, "Leg_Name"], "amended_articles": "|".join(amended),
                                  "changed_articles": "|".join(sorted(changed, key=as_int)), "check": chk})
                if chk not in ("ok", "fixed_article1_ok") and not chk.startswith("resolved_"):
                    # v2.5.0: no arbitrary fallback. The old "first 5 changed articles" showed volunteers
                    # articles unrelated to the amendment (pmk 6056 -> 1..5 while it only touches art. 3).
                    targets = amended
                    if not targets:
                        chk = "target_article_unresolved" if operative(own) else "amendment_has_no_operative_text"
                    vol_rows.append({
                        "pmk_ID": p, "amendment_name": R.at[p, "Leg_Name"], "base_pmk_ID": cid,
                        "base_name": R.at[cid, "Leg_Name"] if cid in R.index else "", "position_in_chain": R.at[p, "chain_position"],
                        "problem": chk, "reviewer_note": manual_notes.get(p, ""), "articles_to_check": "|".join(targets),
                        "amendment_text": "\n".join(f"[{a.get('article_number')}] {a.get('text', '')}" for a in own)[:6000],
                        "target_evidence": json.dumps([d for d in tev if d["n"] in targets], ensure_ascii=False),
                        "previous_version_of_articles": "\n".join(f"[{k}] {prev_raw[k].get('text', '')}" for k in targets if k in prev_raw)[:6000],
                        "current_snapshot_of_articles": "\n".join(f"[{k}] {snap_raw[k].get('text', '')}" for k in targets if k in snap_raw)[:6000],
                    })
                prev_raw = snap_raw
            if chk in ("declared_but_snapshot_missing", "redo_amendment_text_was_wrong"):
                vol_rows.append({"pmk_ID": p, "amendment_name": R.at[p, "Leg_Name"], "base_pmk_ID": cid,
                                 "base_name": R.at[cid, "Leg_Name"] if cid in R.index else "",
                                 "position_in_chain": R.at[p, "chain_position"], "problem": chk,
                                 "articles_to_check": "|".join(amended),
                                 "amendment_text": "\n".join(f"[{a.get('article_number')}] {a.get('text', '')}" for a in own)[:6000],
                                 "previous_version_of_articles": "", "current_snapshot_of_articles": ""})
            R.at[p, "reflection_check"] = chk
            if chk not in ("ok", "fixed_article1_ok") and not chk.startswith("resolved_"):
                flag(p, "reflection_" + chk)
    rep["reflection_article1_fixed"] = pd.DataFrame(fixed)
    rep["reflection_auto_resolved"] = pd.DataFrame(auto_res)
    rep["reflection_checks"] = pd.DataFrame(refl_rows)
    rep["VOLUNTEERS_reflection_queue"] = pd.DataFrame(vol_rows).drop_duplicates("pmk_ID") if vol_rows else pd.DataFrame()
    log.info("Article-1 reflection fixes applied: %d | reflection volunteer queue: %d", len(fixed), len(vol_rows))

    # ------------------------------------------------------------------ 7. number+year duplicates (verified content)
    Bi = B.set_index("pmk_ID")
    dd = R[(R.Leg_Number != "") & R.Year.map(valid_year)]
    dd = dd[dd.duplicated(["Leg_Number", "Year"], keep=False)]
    rows = []
    for (n, y), g in dd.groupby(["Leg_Number", "Year"]):
        issues = set(g.Magazine_Number) - {""}
        chs = g.chain_id.tolist()
        if len(issues) == 1 and "" not in set(g.Magazine_Number) and len(set(chs)) == len(chs) \
                and (g.record_type == "base").sum() <= 1 and len(set(g.ModLeg) - {""}) == (g.ModLeg != "").sum():
            kind, legit = "one_act_touching_several_laws", True
        else:
            legit = False
            toks = [content_tokens(x) for x in g.Leg_Name]
            sims = [overlap(a, b) for i, a in enumerate(toks) for b in toks[i + 1:]]
            same_name = bool(sims) and min(sims) >= 0.5
            same_page = len(set(zip(g.Magazine_Number, g.Magazine_Page))) == 1
            same_modleg = (g.ModLeg != "").all() and g.ModLeg.nunique() == 1
            if len(set(chs)) == 1 and set(g.record_type) == {"base", "amendment"} and not same_page:
                kind = "amendment_carries_its_base_number_year"
            elif len(issues) <= 1 and same_name and (same_page or same_modleg or (g.ModLeg == "").any()):
                kind = "same_legislation_recorded_twice"
            elif same_modleg:
                kind = "same_modleg_but_different_laws"
            else:
                kind = "different_laws_one_has_wrong_number_or_year"
        for p, r in g.iterrows():
            dn, dy = ((A.at[r.ModLeg, "amendments_Number"], A.at[r.ModLeg, "Year"]) if r.ModLeg in A.index
                      else (Bi.Law_Number.get(p, ""), Bi.Year.get(p, "")))
            m = re.search(r"لسنة\s*\(?\s*(\d{4})", naming_article(art_now(p))[:300]) if r.articles_check in ("ok", "replaced_from_diwan") else None
            ty, gy = (m.group(1) if m else ""), r.Magazine_Date[:4]
            rows.append({"Leg_Number": n, "Year": y, "group_kind": kind, "legitimate": legit, "pmk_ID": p, "ModLeg": r.ModLeg,
                         "record_type": r.record_type, "Leg_Name": r.Leg_Name, "Magazine_Number": r.Magazine_Number,
                         "Magazine_Page": r.Magazine_Page, "Magazine_Date": r.Magazine_Date, "chain_id": r.chain_id,
                         "in_json": r.in_json, "row_origin": r.row_origin, "in_diwan": bool(dn or dy),
                         "diwan_number": dn, "diwan_year": dy, "gazette_year": gy, "own_title_year": ty,
                         "year_evidence": "gazette+title disagree with Year" if ty and ty == gy and ty != y else ""})
            if not legit:
                flag(p, "number_year_duplicate_pre1960" if int(y) <= 1960 else "number_year_duplicate_post1960")
    ny = pd.DataFrame(rows)
    if not ny.empty:
        bad = ny[~ny.legitimate]
        rep["dup_number_year_1960_and_before"] = bad[bad.Year.astype(int) <= 1960]
        rep["dup_number_year_after_1960_URGENT"] = bad[bad.Year.astype(int) > 1960]
        rep["number_year_shared_legitimately_info"] = ny[ny.legitimate]
    pb = R[R.record_type == "base"]
    grp = defaultdict(list)
    for p in pb.pmk_ID:
        key = (R.at[p, "Year"], " ".join(sorted(core(R.at[p, "Leg_Name"]))))
        if key[1]:
            grp[key].append(p)
    rep["possible_duplicate_base_laws_not_merged"] = pd.DataFrame(
        [{"Year": y, "pmk_ID": p, "Leg_Name": R.at[p, "Leg_Name"], "Magazine_Number": R.at[p, "Magazine_Number"],
          "Magazine_Page": R.at[p, "Magazine_Page"], "chain_length": R.at[p, "chain_length"], "row_origin": R.at[p, "row_origin"],
          "in_diwan": p in in_diwan_base, "in_json": R.at[p, "in_json"]}
         for (y, _), ps in grp.items() if len(ps) > 1 for p in ps])
    # inactive without End_Date -> volunteer queue, with the base law's End_Date as a SUGGESTION only
    # (measured: 871 of 1132 dated amendments share their base's End_Date; the rest end earlier)
    ne = R[R.pmk_ID.map(lambda x: "status_inactive_without_end_date" in flags[x])]
    rep["VOLUNTEERS_missing_end_date"] = pd.DataFrame([{
        "pmk_ID": p, "record_type": r.record_type, "Leg_Name": r.Leg_Name, "Leg_Number": r.Leg_Number, "Year": r.Year,
        "base_pmk_ID": r.parent_pmk_ID, "base_name": R.at[r.parent_pmk_ID, "Leg_Name"] if r.parent_pmk_ID in R.index else "",
        "suggested_end_date": R.at[r.parent_pmk_ID, "End_Date"] if r.parent_pmk_ID in R.index else "",
        "suggestion_basis": "base law End_Date (holds for ~77% of dated amendments) - verify" if r.parent_pmk_ID in R.index and R.at[r.parent_pmk_ID, "End_Date"] else "",
        "Canceled_By": r.Canceled_By, "Replaced_By": r.Replaced_By} for p, r in ne.iterrows()])
    rep["names_changed_in_json"] = pd.DataFrame(
        [{"pmk_ID": p, "record_type": r.record_type, "old_json_name": r._json_name, "new_name": r.Leg_Name, "source": r.name_source}
         for p, r in R.iterrows() if r._json_name and r._json_name != r.Leg_Name])
    no_art = [p for p in R.pmk_ID if not art_now(p)]
    rep["VOLUNTEERS_missing_articles"] = pd.DataFrame([{
        "pmk_ID": p, "record_type": R.at[p, "record_type"], "Leg_Name": R.at[p, "Leg_Name"], "Leg_Number": R.at[p, "Leg_Number"],
        "Year": R.at[p, "Year"], "Status": R.at[p, "Status"], "Magazine_Number": R.at[p, "Magazine_Number"],
        "Magazine_Page": R.at[p, "Magazine_Page"], "Magazine_Date": R.at[p, "Magazine_Date"], "in_json": R.at[p, "in_json"],
        "base_pmk_ID": R.at[p, "parent_pmk_ID"]} for p in no_art])
    rep["csv_rows_not_in_json"] = R[~R.in_json][["pmk_ID", "ModLeg", "Leg_Name", "Leg_Number", "Year", "record_type", "chain_id", "row_origin"]]

    # ------------------------------------------------------------------ 8. write JSON (same record structure)
    def updated(p, as_nest: bool):
        rec = FULL[R.at[p, "_uid"]]
        rec["Status"] = R.at[p, "Status"]
        if R.at[p, "End_Date"] and not rec.get("End_Date"):
            rec["End_Date"] = to_json_date(R.at[p, "End_Date"])
        rec["Leg_Name"] = R.at[p, "Leg_Name"]
        rec["Leg_Name_source"] = R.at[p, "name_source"]
        if p in new_articles:
            rec["Base_Articles"] = new_articles[p]
            rec["Article_Count"] = str(len(new_articles[p]))
        if p in new_refl:
            rec["Reflected_Articles"] = new_refl[p]
        if as_nest:
            rec.pop("Mod_Legs", None)
            rec.setdefault("is_amendment", "True")
            rec.setdefault("has_reflection", bool(rec.get("Reflected_Articles")))
        return rec

    tops, created = [], []
    for cid, seq in chains.items():
        if cid not in R.index:
            continue
        if not R.at[cid, "_uid"]:
            if not any(R.at[p, "_uid"] for p in seq[1:]):
                continue
            created.append({"pmk_ID": cid, "Leg_Name": R.at[cid, "Leg_Name"], "amendments_in_json": sum(bool(R.at[p, "_uid"]) for p in seq[1:])})
            FULL["laws:" + cid] = {
                "Leg_Name": R.at[cid, "Leg_Name"], "Publication": "", "Leg_Number": R.at[cid, "Leg_Number"], "Year": R.at[cid, "Year"],
                "Article_Count": "0", "Replaced_For": R.at[cid, "Replaced_For"], "Canceled_By": R.at[cid, "Canceled_By"],
                "Magazine_Number": R.at[cid, "Magazine_Number"], "Magazine_Page": R.at[cid, "Magazine_Page"],
                "Magazine_Date": to_json_date(R.at[cid, "Magazine_Date"]), "Issue_Date": to_json_date(R.at[cid, "Issue_Date"]),
                "Active_Date": to_json_date(R.at[cid, "Active_Date"]), "End_Date": to_json_date(R.at[cid, "End_Date"]),
                "Replaced_By": R.at[cid, "Replaced_By"], "Status": R.at[cid, "Status"], "URL": "", "Base_Articles": [],
                "Mod_Legs": [], "pmk_ID": cid, "source_table": "UD_leg_Laws", "source_pmk_ID": cid, "leg_uid": "laws:" + cid,
                "id_also_used_as_modleg": False, "Leg_Name_original": R.at[cid, "Law_Name_csv_original"],
                "Leg_Name_source": "created_from_csv_to_hold_its_amendments"}
            R.at[cid, "_uid"] = "laws:" + cid
            R.at[cid, "json_leg_uid"] = "laws:" + cid
        top = updated(cid, False)
        top["Mod_Legs"] = [updated(p, True) for p in seq[1:] if R.at[p, "_uid"]]
        tops.append((as_int(R.at[cid, "_top_idx"], 10**9), as_int(cid), top))
    rep["json_base_records_created"] = pd.DataFrame(created)
    new_json = [t for _, _, t in sorted(tops, key=lambda x: (x[0], x[1]))]
    n_out = len(new_json) + sum(len(t["Mod_Legs"]) for t in new_json)
    uids = [t["leg_uid"] for t in new_json] + [m["leg_uid"] for t in new_json for m in t["Mod_Legs"]]
    dupu = pd.Series(uids)[pd.Series(uids).duplicated()].tolist()
    assert not dupu, f"duplicate leg_uid in output JSON: {dupu[:10]}"
    kept = set(uids)
    rep["json_records_not_written"] = pd.DataFrame(
        [{"json_uid": u, "Leg_Name": CTRL.sub("", str(FULL[u].get("Leg_Name", "")))[:120],
          "reason": ("duplicate of Diwan base " + next((d["into_pmk"] for d in json_dropped if d["json_uid"] == u), ""))
          if u in {d["json_uid"] for d in json_dropped} else "row removed/merged as duplicate or amendment without base in JSON"}
         for u in FULL if u not in kept])
    out.mkdir(parents=True, exist_ok=True)
    (out / f"master_clean_{TS}.json").write_text(json.dumps(new_json, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("JSON written: %d top-level, %d total records (input %d)", len(new_json), n_out, n_in)

    # ------------------------------------------------------------------ 9. write CSV
    R["review_flags"] = [";".join(flags[p]) for p in R.pmk_ID]
    R["needs_review"] = R.review_flags != ""
    cols = ["pmk_ID", "ModLeg", "json_leg_uid", "record_type", "Leg_Name", "name_source", "Law_Name_csv_original",
            "Leg_Number", "Year", "is_temporary", "Status", "Status_code", "status_rule", "Issue_Date",
            "Magazine_Number", "Magazine_Page", "Magazine_Date", "Active_Date", "End_Date", "Article_Count",
            "chain_id", "chain_position", "chain_length", "parent_pmk_ID", "prev_pmk_ID", "next_pmk_ID",
            "chain_evidence", "chain_confidence", "articles_check", "articles_source", "has_reflection",
            "amended_articles", "reflection_check", "Canceled_By", "Replaced_By", "Replaced_For",
            "entity", "parent_ministry", "entity_type", "scope", "in_json", "needs_review", "review_flags"]
    for p, arts in new_articles.items():
        R.at[p, "Article_Count"] = str(len(arts))
    final = R.sort_values(["chain_id", "chain_position"], key=lambda s: s.map(as_int) if s.name == "chain_id" else s)[cols]
    out.mkdir(parents=True, exist_ok=True)
    final.to_csv(out / f"laws_kg_clean_{TS}.csv", index=False, encoding="utf-8-sig")

    for k, df in rep.items():
        if df is not None and len(df):
            df.to_csv(out / f"review_{k}_{TS}.csv", index=False, encoding="utf-8-sig")

    s = {"rows_out": len(final), "base": int((final.record_type == "base").sum()),
         "amendment": int((final.record_type == "amendment").sum()), "json_records_out": n_out,
         "needs_review_rows": int(final.needs_review.sum())}
    log.info("SUMMARY %s", json.dumps(s))
    fc = pd.Series([re.sub(r"_\d+", "", f) for x in final.review_flags for f in x.split(";") if f])
    log.info("FLAGS\n%s", fc.value_counts().to_string())
    log.info("CHAIN CONFIDENCE\n%s", final[final.record_type == "amendment"].chain_confidence.value_counts().to_string())
    log.info("REFLECTION\n%s", final.reflection_check.value_counts().to_string())
    for k, df in rep.items():
        log.info("report %-40s %6d rows", k, 0 if df is None else len(df))
    return final, new_json, rep


def main():
    try:
        from dotenv import load_dotenv  # optional: pip install python-dotenv
        load_dotenv()
    except ImportError:
        pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for arg, env in [("json", "MASTER_JSON_PATH"), ("csv", "LAWS_CSV_PATH"), ("diwan_laws", "DIWAN_LAWS_PATH"),
                     ("diwan_amend", "DIWAN_AMENDMENTS_PATH"), ("diwan_amend_art", "DIWAN_AMEND_ARTICLES_PATH"),
                     ("diwan_law_art", "DIWAN_LAW_ARTICLES_PATH"), ("diwan_versions", "DIWAN_ARTICLE_VERSIONS_PATH")]:
        ap.add_argument("--" + arg, default=os.getenv(env), help=f"default: ${env}")
    ap.add_argument("--out", default="outputs")
    a = vars(ap.parse_args())
    missing = [k for k, v in a.items() if not v]
    if missing:
        ap.error("missing paths: " + ", ".join(missing) + " (set them in .env)")
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s",
                        handlers=[logging.FileHandler(f"logs/reno_pipeline_{TS}.log", encoding="utf-8"), logging.StreamHandler()])
    log.info("reno_pipeline v%s | %s", VERSION, json.dumps({k: str(v) for k, v in a.items()}, ensure_ascii=False))
    cfg = {k: Path(v) for k, v in a.items() if k != "out"}
    build(cfg, Path(a["out"]) / f"run_{TS}")


if __name__ == "__main__":
    main()
