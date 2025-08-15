#!/usr/bin/env python3
from __future__ import annotations

import argparse, re, csv
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

# ---------- optional PDF backends ----------
try:
    import fitz
    HAVE_PYMUPDF = True
except Exception:
    HAVE_PYMUPDF = False

try:
    from PyPDF2 import PdfReader
    HAVE_PYPDF2 = True
except Exception:
    HAVE_PYPDF2 = False

# ---------- patterns & constants ----------
DATE_PATTERNS = [
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",          # 31/7/2025
    r"\b\d{4}-\d{2}-\d{2}\b",              # 2025-07-31
    r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b" # 7 May 2025
]

DECIMAL  = r"\d{1,3}(?:,\d{3})*\.\d{1,2}"
INTEGER  = r"\d{1,3}(?:,\d{3})*"

COPY_SUFFIX_RE    = re.compile(r"\(\s*\d+\s*\)$")
VERSION_SUFFIX_RE = re.compile(r"(?i)(?:[ _-]v\d+)$")

CURRENCY_MAP = {
    "US$": "USD", "S$": "SGD", "A$": "AUD", "HK$": "HKD", "NT$": "TWD",
    "$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"
}
ISO_SET = {"SGD","USD","EUR","GBP","JPY","MYR","HKD","TWD","AUD","NZD","CAD","CNY","RMB"}

CURRENCIES = [
    r"SGD", r"S\$", r"USD", r"US\$", r"AUD", r"A\$", r"NZD", r"CAD",
    r"EUR", r"GBP", r"CNY", r"RMB", r"JPY", r"MYR", r"RM", r"HKD",
    r"NTD", r"NT\$",
    r"\$", r"€", r"£", r"¥", r"HK\$", r"NT\$"
]
CURRENCY = r"(?:" + "|".join(CURRENCIES) + r")"



TOTAL_PHRASES = (
    r"(?:grand\s*total)"
    r"|(?:total\s+amount(?:\s+payable(?:\s+including\s+gst)?)?)"
    r"|(?:amount\s+due\s*\(subtotal\))"
    r"|(?:total\s+amount\b)"
    r"|(?:\btotal\b)"
)
TOTAL_WORDS = re.compile(rf"\b(?:{TOTAL_PHRASES})\b", re.I)
TOTAL_ROW_RE = re.compile(rf"^\s*(?:{TOTAL_PHRASES})\b",re.I)
ITEM_LABELS = re.compile(r"\b(?:line\s+item\s+total|amount)\b",re.I)
HEADER_AMOUNT_RE = re.compile(r"\b(amount|total(?:\s*price)?|price|value)\b", re.I)
SKIP_WORDS  = re.compile(
    r"\b(gst|vat|tax|voucher|cash\s*advance|utili[sz]ed|shipping|delivery|discount|"
    r"service\s*charge|fee|charges?)\b", re.I
)

DOC_PATTERNS = [
    # explicit tags first
    (re.compile(r'(?i)\bPO\s*0*([0-9]{6,})\b'),          "po_digits"),
    (re.compile(r'(?i)\bOrder\s*0*([0-9]{6,})\b'),       "order_digits"),
    (re.compile(r'(?i)\bSO\s*0*([0-9]{6,})\b'),          "so_digits"),
    (re.compile(r'(?i)\bINV(?:OICE)?\s*0*([0-9]{5,})\b'),"inv_digits"),
    (re.compile(r'(?i)\bSV0*([0-9]{6,})\b'),             "sv_digits"),
]

COPY_ORIG_RE = re.compile(r'(?i)\b(original|compressed|copy|scanned)\b')
COPY_SUFFIX_RE    = re.compile(r"\(\s*\d+\s*\)$")
VERSION_SUFFIX_RE = re.compile(r"(?i)(?:[ _-]v\d+)$")

AMT_DEC = re.compile(rf"(?:{CURRENCY}\s*)?(?P<num>{DECIMAL})(?:\s*{CURRENCY})?", re.I)
AMT_INT = re.compile(rf"(?:{CURRENCY}\s*)(?P<num>{INTEGER})\b", re.I)

AMT_NUM_IN_TOKEN = re.compile(rf"(?P<num>{DECIMAL}|{INTEGER})")
AMT_TOKEN = re.compile(
    rf"(?<![A-Za-z])(?:{CURRENCY}\s*(?:{DECIMAL}|{INTEGER})|{DECIMAL}\s*{CURRENCY})(?![A-Za-z])",
    re.I
)

DESC_HEADER_RE   = re.compile(r"(purpose\s*/?\s*description|description|details|item|particulars)", re.I)
VENDOR_HEADER_RE = re.compile(r"(vendor|merchant|supplier)", re.I)
CITY_HEADER_RE   = re.compile(r"\bcity\b", re.I)
PAYTYPE_HEADER_RE= re.compile(r"(payment\s*type|payment|type)", re.I)

SUMMARY_START = [
    r"\bExpenses\s+Requiring\s+Receipts\b",
    r"\bExpense\s+Summary\b",
    r"\bSummary\b",
    r"\bExpense\s+Report\b",
    r"\bReport\s+Name\s*:\b",
    r"\bReport\s+Header\b",
    r"\bTransaction\s*Date\b",
]
SUMMARY_END = [
    r"\bAttach\s+required\s+receipts\b",
    r"\bReceipts\s+attached\b",
    r"\bIndividual\s+Receipts?\b",
    r"\bE-?Receipts?\b",
    r"\bCompany\s+Disbursements\b",
    r"\bEmployee\s+Disbursements\b",
    r"\bTotal\s+Paid\s+By\s+Company\b",
]

STOPWORDS = {
    "the","a","an","and","or","of","for","to","in","on","at","by",
    "with","from","this","that","these","those",
    "invoice","receipt","qty","quantity","pcs","unit","item","items"
}
LOW_INFO_TOKENS = {
    "csh","cash","reimb","reimbursement","misc","other","others",
    "fee","fees","charge","charges","voucher","claim","expense",
    "sgd","usd","myr","eur","gbp"
}
BAD_XML = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")

ROW_GLUE = 800
LEFT_GAP_MIN = 40

CUR_AFTER_RE = re.compile(r"^\s*(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)\b", re.I)

def _amount_candidates_in_text(line: str, page_currency: str):
    """
    Return list of (amount:float, currency:str, start:int, end:int).
    If an ISO code appears immediately after the number (e.g. ... 14,300.73 SGD),
    override the symbol-derived currency and use that ISO code.
    """
    cands = []
    for m in AMT_TOKEN.finditer(line):
        token = m.group(0)
        mnum  = AMT_NUM_IN_TOKEN.search(token)
        if not mnum:
            continue
        amt = _norm_amount(mnum.group("num"))
        cur = _resolve_currency_from_token(token, page_currency)

        # look a bit to the right for an explicit ISO code and override currency if present
        tail = line[m.end(): m.end() + 16]
        t2 = CUR_AFTER_RE.search(tail)
        if t2:
            iso = t2.group(1).upper()
            if iso in ISO_SET:
                cur = iso

        cands.append((amt, cur, m.start(), m.end()))
    return cands

def _pick_amount(cands, prefer_currency: str):
    """Prefer amounts with prefer_currency; tie-break by rightmost start; else rightmost overall."""
    if not cands:
        return None
    pref = [c for c in cands if c[1] == prefer_currency]
    pool = pref if pref else cands
    return max(pool, key=lambda c: c[2])  # rightmost by start index

# --- comments helper ---
_PRIORITY = {"OK": 4, "DESC_MISMATCH": 3, "AMOUNT_MISMATCH": 3, "FLAG": 1, "NO_MATCH": 0}

def _comment_from_group(grp: pd.DataFrame) -> str:
    g = grp.copy()
    g["__prio"] = g["status"].map(_PRIORITY).fillna(0)
    best = g.sort_values(["__prio", "match_score"], ascending=[False, False]).iloc[0]
    st = str(best["status"])
    via = str(best.get("doc_filter_used", "none"))
    scr = best.get("match_score", None)
    a   = float(best.get("amount", 0.0))
    b   = best.get("match_amount", None)
    diff = (abs(a - float(b)) if (b is not None and pd.notna(b)) else None)
    row_excel = best.get("actual_row", None)
    row_hint = f" (row {int(row_excel)})" if pd.notna(row_excel) else ""

    if st == "OK":
        k = int((g["status"] == "OK").sum())
        return f"OK – {k} match(es) via {via}{row_hint}"
    if st == "AMOUNT_MISMATCH":
        return f"FLAG – description ok (score {scr}), price diff {diff:.2f}{row_hint}"
    if st == "DESC_MISMATCH":
        return f"FLAG – price ok, weak description (score {scr}){row_hint}"
    if st == "FLAG":
        return f"FLAG – low match (score {scr}){row_hint}"
    return "NO_MATCH"

# ---------- dataclass ----------
@dataclass
class LineItem:
    file: str
    page: int
    date: str
    description: str
    currency: str
    amount: float
    source: str
    docno: str = ""   # normalized file-stem used as doc id

# ---------- IO helpers ----------
def clean_file_stem(stem: str) -> str:
    s = stem.strip()
    s = COPY_SUFFIX_RE.sub("", s)
    s = VERSION_SUFFIX_RE.sub("", s)
    s = COPY_ORIG_RE.sub("", s)
    return s.strip(" _-")

def extract_doc_id(
    stem: str,
    mode: str = "auto",
    segment_sep: str = r"[ _-]",
    segment_index: int = 0,
) -> str:
    s = clean_file_stem(stem)

    # 1) explicit modes
    if mode == "po":
        m = re.search(r'(?i)\bPO\s*0*([0-9]{6,})\b', s);  return m.group(1) if m else ""
    if mode == "order":
        m = re.search(r'(?i)\bOrder\s*0*([0-9]{6,})\b', s); return m.group(1) if m else ""
    if mode in ("invoice","inv"):
        m = re.search(r'(?i)\bINV(?:OICE)?\s*0*([0-9]{5,})\b', s); return m.group(1) if m else ""
    if mode == "sv":
        m = re.search(r'(?i)\bSV0*([0-9]{6,})\b', s); return ("SV" + m.group(1)) if m else ""

    if mode.startswith("regex:"):
        # e.g. mode="regex:(?i)_(SV\d{6,})_:1" or "regex:(\d{8,}):1"
        try:
            pat, grp = mode.split(":", 2)[1:]
            grp = int(grp)
            m = re.search(pat, s)
            return m.group(grp) if m else ""
        except Exception:
            return ""

    if mode == "segment":
        parts = re.split(segment_sep, s)
        return parts[segment_index] if 0 <= segment_index < len(parts) else ""

    if mode in ("first_long_digits","last_long_digits"):
        runs = re.findall(r'(\d{8,})', s)
        if runs:
            return runs[0] if mode == "first_long_digits" else runs[-1]
        return ""

    # 2) auto: try tagged first
    for rx, _name in DOC_PATTERNS:
        m = rx.search(s)
        if m:
            # include tag if it’s alnum prefixed like SVxxxx, else just digits
            if _name.startswith("sv_"):
                return "SV" + m.group(1)
            return m.group(1)

    # 3) longest digit run (>=8). If multiple, pick the longest; tie -> rightmost
    runs = re.findall(r'(\d{8,})', s)
    if runs:
        max_len = max(len(r) for r in runs)
        candidates = [r for r in runs if len(r) == max_len]
        return candidates[-1]

    # 4) alnum tag like SV01954455 / INV123456 if no pure digit runs
    m = re.search(r'([A-Z]{1,4}\d{6,})', s, flags=re.I)
    if m:
        return m.group(1).upper()

    # 5) last resort: your old normalized stem
    return norm_docno(s)

def load_claim_book(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    suf = p.suffix.lower()

    if suf in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        return pd.read_excel(p, engine="openpyxl")
    if suf in {".xls", ".xlt"}:
        return pd.read_excel(p, engine="xlrd")
    if suf in {".xlsb"}:
        return pd.read_excel(p, engine="pyxlsb")

    if suf in {".csv", ".txt"}:
        # Try common CSV encodings in order
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
            try:
                # engine="python" lets pandas auto-infer delimiters if needed
                return pd.read_csv(p, encoding=enc, engine="python")
            except UnicodeDecodeError:
                pass  # try next encoding

        # Last resort: decode bytes with latin1 (never fails) and parse
        from io import StringIO
        with open(p, "rb") as f:
            raw = f.read()
        decoded = raw.decode("latin1", errors="replace")
        return pd.read_csv(StringIO(decoded), engine="python")

    # last resort for odd Excel types
    for eng in ("openpyxl", "xlrd", "pyxlsb"):
        try:
            return pd.read_excel(p, engine=eng)
        except Exception:
            pass

    raise ValueError(f"Cannot read {p}. Please save as .xlsx/.xls/.xlsb/.csv")

# ---------- normalize helpers ----------
def norm_docno(s: str) -> str:
    """Normalize a document id from a filename-like string (drop copy/version suffixes)."""
    s = clean_file_stem(str(s))
    return re.sub(r"[^A-Z0-9]", "", s.upper())

def _norm_space(s: str) -> str:
    s = s.replace("\f", " ")
    s = re.sub(r"-\s+", "-", s)
    return re.sub(r"\s+", " ", s).strip()

def _norm_amount(s: str) -> float:
    raw = re.sub(r"[,\s]", "", str(s))
    neg = raw.startswith("(") and raw.endswith(")")
    raw = raw.strip("()")
    raw = re.sub(r"[^0-9.\-]", "", raw)
    val = float(raw) if raw else 0.0
    return -val if neg else val

def _normalize_date(value: str) -> Optional[str]:
    value = value.strip()
    for f in ("%d/%m/%Y","%Y-%m-%d","%d %b %Y","%d %B %Y","%d/%m/%y"):
        try:
            return datetime.strptime(value, f).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def _resolve_currency_from_token(token: str, page_currency: str) -> str:
    m = re.search(r"(US\$|S\$|A\$|HK\$|NT\$|€|£|¥|\$|\b[A-Z]{3}\b)", token)
    if not m:
        return page_currency
    key = m.group(1)
    if key == "$":
        return page_currency
    if key in CURRENCY_MAP:
        return CURRENCY_MAP[key]
    up = key.upper()
    return up if up in ISO_SET else page_currency

# ---------- extraction ----------
def extract_text_pages(pdf_path: Path) -> List[str]:
    if HAVE_PYMUPDF:
        try:
            doc = fitz.open(str(pdf_path))
            return [page.get_text("text") or "" for page in doc]
        except Exception:
            pass
    if HAVE_PYPDF2:
        reader = PdfReader(str(pdf_path))
        out = []
        for page in reader.pages:
            try:
                out.append(page.extract_text() or "")
            except Exception:
                out.append("")
        return out
    raise RuntimeError("Install PyMuPDF or PyPDF2")

TABLE_HEADER_RE = re.compile(
    r"(description|item|details|particulars|merchant|vendor|expense\s*(type)?|category|purpose|comments?)"
    r".{0,100}(amount|total(?:\s*price)?|price|value|subtotal)",
    re.I
)
COL_SPLIT       = re.compile(r"\s{2,}")

def _extract_header_table(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    items: list[LineItem] = []
    lines = [l for l in block.splitlines() if l.strip()]
    for i, line in enumerate(lines):
        if not TABLE_HEADER_RE.search(line):
            continue
        for raw in lines[i+1:i+80]:
            row = _norm_space(raw)
            if not row:
                break
            is_total = bool(TOTAL_ROW_RE.search(row))
            cols = COL_SPLIT.split(row)
            if len(cols) < 2:
                continue
            desc = cols[0].strip(" -:|•\u2022")
            amt = None
            for part in reversed(cols[1:]):
                t = AMT_DEC.search(part) or AMT_INT.search(part)
                if t:
                    amt = _norm_amount(t.group("num"))
                    break
            if desc and (amt is not None):
                src = "table_total" if is_total else "table"
                items.append(LineItem("", page1, fallback_date, desc[:160], page_currency, amt, src))
                if is_total:
                    break
    return items

def _extract_layout_table(pdf_path: Path, page_index: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    if not HAVE_PYMUPDF:
        return []
    import fitz

    MIN_RIGHT_MARGIN = 12.0
    MAX_RIGHT_MARGIN = 28.0
    MAX_JOIN_GAP_Y   = 16.0   # vertical gap to allow joining lines
    MIN_TOKENS_DESC  = 2      # require >=2 meaningful tokens

    def _dominant_column(xs: list[float], bin_size: float = 10.0) -> tuple[float, float]:
        if not xs:
            return 0.0, 9999.0
        bins: dict[float, int] = {}
        for x in xs:
            k = round(x / bin_size) * bin_size
            bins[k] = bins.get(k, 0) + 1
        dom = max(bins.items(), key=lambda kv: kv[1])[0]
        return dom, bin_size * 1.5

    def _percentile(vals: list[float], q: float) -> float | None:
        """Pure-Python percentile (q in [0,1])."""
        if not vals:
            return None
        v = sorted(vals)
        k = (len(v) - 1) * q
        f = int(k)
        c = min(f + 1, len(v) - 1)
        if f == c:
            return float(v[f])
        return float(v[f] + (v[c] - v[f]) * (k - f))

    doc = fitz.open(str(pdf_path))
    page = doc[page_index - 1]
    words = page.get_text("words")  # x0,y0,x1,y1,word,block_no,line_no,word_no

    # 1) bucket words into lines
    buckets: dict[int, list[tuple[float, float, str]]] = {}
    for x0, y0, x1, y1, w, *_ in words:
        key = round(y0 / 2)  # ~2pt tolerance
        buckets.setdefault(key, []).append((x0, y0, w))

    # 2) build line structures
    line_rows: list[tuple[float, list[tuple[float, str]], str]] = []
    for key in sorted(buckets):
        row = sorted([(x, w) for x, _, w in buckets[key]])
        if not row:
            continue
        y_vals = [y0 for _, y0, _ in buckets[key]]
        y_min = min(y_vals) if y_vals else key * 2.0
        text = " ".join(w for _, w in row)
        line_rows.append((y_min, row, text))

    # 3) detect table band (optional guard)
    lines_text = [(y, t) for (y, _, t) in line_rows]
    y_top, y_bot = None, None
    # (keep your header/top/bottom logic if you want; safe to omit for generality)

    amt_positions: list[float] = []
    gaps_before_amt: list[float] = []
    parsed_lines: list[tuple[float, list[tuple[float, str]], str, int, Optional[float], Optional[float]]] = []

    # 4) scan lines to find the amount column + candidates
    amt_positions: list[float] = []
    parsed_lines: list[tuple[float, list[tuple[float, str]], str, int, Optional[float], Optional[float]]] = []

    for y, row, text in line_rows:
        # collect all (x, amount, currency, idx) candidates on this row
        row_cands = []
        for i in range(len(row) - 1, -1, -1):
            token = row[i][1]
            m = AMT_DEC.search(token) or AMT_INT.search(token)
            if not m:
                continue
            try:
                val = _norm_amount(m.group("num"))
            except Exception:
                continue
            cur = _resolve_currency_from_token(token, page_currency)
            # override with trailing ISO code if the next token is an ISO (USD/SGD/etc.)
            if i + 1 < len(row):
                nxt = row[i + 1][1]
                t2 = re.fullmatch(r"(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)", nxt, re.I)
                if t2:
                    cur = t2.group(1).upper()
            row_cands.append((row[i][0], val, cur, i))

        # default "rightmost" candidate, but prefer page currency
        if row_cands:
            pref = [c for c in row_cands if c[2] == page_currency]
            x_amt, v_amt, _, i_amt = max(pref if pref else row_cands, key=lambda z: z[0])
        else:
            i_amt, x_amt, v_amt = -1, None, None

        parsed_lines.append((y, row, text, i_amt, x_amt, v_amt))
        if x_amt is not None:
            amt_positions.append(x_amt)
            left_xs = [x for x, _ in row if x < x_amt]
            if left_xs:
                gaps_before_amt.append(max(0.0, x_amt - max(left_xs)))

    dom_x, tol_x = _dominant_column(amt_positions, bin_size=10.0)

    # 5) dynamic right margin = clamped 25th percentile of gaps
    q25 = _percentile(gaps_before_amt, 0.25) or 24.0
    right_margin = max(MIN_RIGHT_MARGIN, min(MAX_RIGHT_MARGIN, q25))

    # 6) build per-page noise stoplist (tokens seen in ≥60% of lines with an amount in band)
    lines_with_amt = [(y, row, text, i_amt, x_amt, v_amt)
                      for (y, row, text, i_amt, x_amt, v_amt) in parsed_lines
                      if i_amt >= 0 and x_amt is not None and abs(x_amt - dom_x) <= tol_x]
    freq: dict[str, int] = {}
    for _, row, _, i_amt, x_amt, _ in lines_with_amt:
        desc_right = x_amt - right_margin
        toks = [w.lower() for x, w in row if x < desc_right]
        for t in set(t for t in toks if t.isalpha() and len(t) >= 3):
            freq[t] = freq.get(t, 0) + 1
    n_lines = max(1, len(lines_with_amt))
    page_noise = {t for t, c in freq.items() if c / n_lines >= 0.60}

    def _desc_text_from_row(row, desc_right):
        toks = []
        for x, w in row:
            if x < desc_right:
                lw = w.strip()
                if lw and lw.lower() not in page_noise:
                    toks.append(lw)
        return _norm_space(" ".join(toks))

    def _is_continuation(idx0, y0, desc_right):
        if idx0 + 1 >= len(parsed_lines):
            return False
        y1, row1, text1, i_amt1, x_amt1, _ = parsed_lines[idx0 + 1]
        if (y1 - y0) > MAX_JOIN_GAP_Y:
            return False
        # continuation must not look like a new item (no amount in the band)
        if (i_amt1 >= 0 and x_amt1 is not None and abs(x_amt1 - dom_x) <= tol_x * 0.75):
            return False
        left_text = _desc_text_from_row(row1, desc_right)
        if not left_text or TABLE_HEADER_RE.search(text1) or TOTAL_ROW_RE.search(text1):
            return False
        return True

    # 7) filter + build items
    items: list[LineItem] = []
    used_cont_lines: set[int] = set()
    seen: set[tuple[float, str]] = set()

    for idx, (y, row, text, i_amt, x_amt, v_amt) in enumerate(parsed_lines):
        if idx in used_cont_lines:
            continue
        if i_amt < 0 or x_amt is None or v_amt is None:
            continue
        if abs(x_amt - dom_x) > tol_x:
            continue
        if not _nonzero(v_amt):
            continue  # drop zero amounts

        desc_right = x_amt - right_margin
        desc = _desc_text_from_row(row, desc_right)

        def _meaningful_token_count(text: str) -> int:
            return len([t for t in re.findall(r"[A-Za-z0-9]+", text.lower()) if t not in STOPWORDS])
        
        tok_count = _meaningful_token_count(desc)
        char_len  = len(desc)

        # join up to 2 following lines when they look like continuations
        joined_down = []
        j = idx
        y_prev = y
        for _ in range(3):  # up to 3 lines
            if j + 1 >= len(parsed_lines): break
            y2, row2, text2, i_amt2, x_amt2, _ = parsed_lines[j + 1]
            if (y2 - y_prev) > MAX_JOIN_GAP_Y: break
            if (i_amt2 >= 0 and x_amt2 is not None and abs(x_amt2 - dom_x) <= tol_x * 0.75): break
            if TABLE_HEADER_RE.search(text2) or TOTAL_ROW_RE.search(text2): break
            chunk = _desc_text_from_row(row2, desc_right)
            if not chunk: break
            joined_down.append(chunk)
            used_cont_lines.add(j + 1)
            j += 1
            y_prev = y2

        if joined_down:
            desc = _norm_space(desc + " " + " ".join(joined_down))
            tok_count = _meaningful_token_count(desc)
            char_len  = len(desc)

        k = idx
        y_prev_up = y
        joined_up = []
        for _ in range(2):
            if k - 1 < 0: break
            y0, row0, text0, i_amt0, x_amt0, _ = parsed_lines[k - 1]
            if (y_prev_up - y0) > MAX_JOIN_GAP_Y: break
            # must not look like a new item line with an amount in the band
            if (i_amt0 >= 0 and x_amt0 is not None and abs(x_amt0 - dom_x) <= tol_x * 0.75): break
            if TABLE_HEADER_RE.search(text0) or TOTAL_ROW_RE.search(text0): break
            chunk0 = _desc_text_from_row(row0, desc_right)
            if not chunk0: break
            # only keep if it actually improves the description
            if _meaningful_token_count(chunk0 + " " + desc) <= tok_count and (len(chunk0 + " " + desc) <= char_len):
                break
            joined_up.insert(0, chunk0)
            k -= 1
            y_prev_up = y0

        if joined_up:
            desc = _norm_space(" ".join(joined_up) + " " + desc)
            tok_count = _meaningful_token_count(desc)
            char_len  = len(desc)

        if not desc or _meaningful_token_count(desc) < MIN_TOKENS_DESC:
            # try one line above if thin and close without amount in band
            if idx > 0:
                y0, row0, text0, i_amt0, x_amt0, _ = parsed_lines[idx - 1]
                if (y - y0) <= MAX_JOIN_GAP_Y and not (i_amt0 >= 0 and x_amt0 is not None and abs(x_amt0 - dom_x) <= tol_x * 0.75):
                    pre = _desc_text_from_row(row0, desc_right)
                    if pre and not TABLE_HEADER_RE.search(text0):
                        desc = _norm_space(pre + " " + (desc or ""))

        if not desc or _meaningful_token_count(desc) < MIN_TOKENS_DESC:
            continue

        # keep the rightmost / largest money on the row
        row_amounts = []
        for x, w in row:
            mm = AMT_DEC.search(w) or AMT_INT.search(w)
            if mm:
                try:
                    row_amounts.append((x, _norm_amount(mm.group("num"))))
                except Exception:
                    pass
        if row_amounts:
            rightmost_x = max(rx for rx, _ in row_amounts)
            max_abs_amt = max(abs(ra) for _, ra in row_amounts)
            if (x_amt < rightmost_x - 1) and (abs(v_amt) < max_abs_amt - 0.005):
                continue

        key = (round(v_amt, 2),
                re.sub(r"\s+", " ", desc.lower())[:40],
                round(y, 1))
        if key in seen:
            continue
        seen.add(key)

        is_total = bool(TOTAL_WORDS.search(text) or TOTAL_WORDS.search(desc))
        src = "layout_total" if is_total else "layout"
        items.append(LineItem("", page_index, fallback_date, desc[:160], page_currency, v_amt, src))

    return items

def __from_pages(text_pages: List[str], max_pages: int) -> Tuple[str, int, int]:
    """
    Choose a contiguous window of `max_pages` pages that maximizes a
    simple 'summary-likeness' score, and return that whole block.
    """
    max_pages = max(1, max_pages)
    n = len(text_pages)
    if n == 0:
        return "", 1, 1
    if max_pages >= n:
        block = "\f".join(text_pages)
        return block, 1, n
    
    # page scores
    scores = []
    for t in text_pages:
        lines = [l for l in t.splitlines() if l.strip()]
        amt_hits    = len(list(AMT_TOKEN.finditer(t)))
        header_hits = sum(bool(re.search(r'\b(description|item|details|amount|total)\b', l, re.I))
                          for l in lines[:80])
        cue_hits    = sum(bool(re.search(c, t, re.I)) for c in SUMMARY_START)
        scores.append(amt_hits*3 + header_hits*2 + cue_hits*5)

    # sliding window to find best contiguous block
    best_sum, best_i = -1, 0
    window_sum = sum(scores[:max_pages])
    best_sum, best_i = window_sum, 0
    for i in range(1, n - max_pages + 1):
        window_sum += scores[i + max_pages - 1] - scores[i - 1]
        if window_sum > best_sum:
            best_sum, best_i = window_sum, i

    start_page = best_i + 1
    end_page   = best_i + max_pages
    block = "\f".join(text_pages[best_i:end_page])
    return block, start_page, end_page

def detect_page_currency(text: str, default="SGD") -> str:
    for iso in ("SGD","USD","EUR","GBP","JPY","MYR","HKD","TWD","AUD","NZD","CAD","CNY","RMB"):
        if re.search(rf"\b{iso}\b", text):
            return iso
    m = re.search(r"(US\$|S\$|A\$|HK\$|NT\$|€|£|¥|\$)", text)
    return CURRENCY_MAP.get(m.group(1), default) if m else default

def _find_summary_block(text_pages: List[str], max_pages: int) -> Tuple[str, int, int]:
    """
    Prefer cue-based detection, but return full page(s) starting at the cue,
    not a substring. If no cue exists, fall back to the best contiguous window.
    """
    max_pages = max(1, max_pages)
    n = len(text_pages)
    if n == 0:
        return "", 1, 1

    # 1) cue-based: scan *all* pages
    for i, t in enumerate(text_pages):
        if any(re.search(cue, t, flags=re.I) for cue in SUMMARY_START):
            start_page = i + 1
            end_page   = min(n, start_page + max_pages - 1)
            block = "\f".join(text for text in text_pages[start_page-1:end_page])
            return block, start_page, end_page

    # 2) fallback: best contiguous window by score
    return __from_pages(text_pages, max_pages)

# ---------- description helpers ----------

def _contains_same_words(pdf_desc: str, excel_desc: str) -> bool:
    """
    True if all meaningful Excel tokens appear in the PDF tokens (order-free).
    Allows simple plural/singular variants. Treat empty/low-info Excel desc as OK.
    """
    pdf_set = set(_desc_tokens(pdf_desc))
    # keep only meaningful words from the Excel description
    sig = [w for w in _desc_tokens(excel_desc) if w not in LOW_INFO_TOKENS]
    if not sig:   # Excel desc has nothing but low-info words -> treat as OK
        return True

    def variants(w: str) -> set[str]:
        v = {w}
        if w.endswith("es"): v.add(w[:-2])
        if w.endswith("s"):  v.add(w[:-1])
        return v

    for w in sig:
        if not any(v in pdf_set for v in variants(w)):
            return False
    return True

def _norm_desc_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[-/]", " ", text)
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def norm_desc_series(s: pd.Series) -> pd.Series:
    return s.fillna("").map(_norm_desc_text)

def _desc_tokens(text: str) -> list[str]:
    return [t for t in _norm_desc_text(text).split() if t and t not in STOPWORDS]

def desc_coverage(extracted_desc: str, excel_desc: str) -> tuple[float, set[str]]:
    nx = _norm_desc_text(extracted_desc)
    ne = _norm_desc_text(excel_desc)
    if not ne:
        return 1.0, set()
    if ne in nx:
        etoks = _desc_tokens(excel_desc)
        return 1.0, set(etoks)

    etoks = _desc_tokens(excel_desc)
    xtoks = set(_desc_tokens(extracted_desc))
    if not etoks:
        return 1.0, set()

    matched = []
    total_w = 0
    hit_w = 0
    for t in etoks:
        w = 2 if any(c.isdigit() for c in t) else 1
        total_w += w
        if t in xtoks:
            hit_w += w
            matched.append(t)
    return (hit_w / max(1, total_w)), set(matched)

def best_coverage_match(extracted_desc: str, pool: pd.Series) -> tuple[int, float]:
    best_i, best_cov = -1, 0.0
    for i, txt in pool.items():
        cov, _ = desc_coverage(extracted_desc, str(txt))
        if cov > best_cov:
            best_cov = cov
            best_i = i
    return best_i, best_cov * 100.0

# ---------- claim helpers ----------
ZERO_EPS = 0.005
def _nonzero(a: float) -> bool:
    try: return abs(float(a)) > ZERO_EPS
    except: return False

def detect_doc_column(df: pd.DataFrame, explicit: Optional[str] = None) -> Optional[str]:
    if explicit and explicit in df.columns:
        return explicit
    for c in df.columns:
        name = str(c).lower()
        if re.search(r"\b(doc|document|doc\s*no|invoice|receipt|reference|ref|po|order)\b", name):
            return c
    return None

def _within_price_tol(book_amt: Optional[float], pdf_amt: float,
                      tol_abs: float, tol_pct: float) -> bool:
    if book_amt is None or pd.isna(book_amt):
        return False
    adiff = abs(float(book_amt) - float(pdf_amt))
    pct_ok = (tol_pct > 0 and pdf_amt and (adiff / abs(pdf_amt) * 100.0) <= tol_pct)
    return (adiff <= tol_abs) or pct_ok

def normalize_id_series(s: pd.Series) -> pd.Series:
    def norm_one(x):
        if pd.isna(x):
            return ""
        if isinstance(x, int):
            raw = str(x)
        elif isinstance(x, float):
            raw = str(int(round(x)))
        else:
            raw = str(x).strip()
            if re.fullmatch(r"\d+(?:\.\d+)?[eE][+-]?\d+", raw):
                try:
                    raw = str(int(float(raw)))
                except Exception:
                    pass
            raw = re.sub(r"\.0+$", "", raw)
        return re.sub(r"[^A-Z0-9]", "", raw.upper())
    return s.apply(norm_one)

def build_doc_keys(file_stem: str) -> tuple[set[str], set[str]]:
    base = norm_docno(file_stem)
    eq = {base, base.lstrip("0")}
    contains: set[str] = set()
    if len(base) >= 9: contains.add(base[-9:])
    if len(base) >= 8: contains.add(base[-8:])
    return eq, contains

def _subset_sum_group(indices: list[int], amounts: list[float],
                      target: float, tol_abs: float) -> Optional[list[int]]:
    """Return indices whose amounts sum to target within tol_abs (int cents DP)."""
    target_c = int(round(target * 100))
    tol_c    = int(round(abs(tol_abs) * 100))
    # prune: values way larger than |target| + tol don't help
    hard_cap = abs(target_c) + tol_c

    vals = [(i, int(round(a * 100))) for i, a in zip(indices, amounts) if abs(a) > 0]
    if not vals:
        return None

    # keep numbers sorted by |value| desc (good heuristic)
    vals.sort(key=lambda t: abs(t[1]), reverse=True)

    dp: dict[int, list[int]] = {0: []}  # sum_cents -> indices used
    for i, v in vals:
        ndp = dict(dp)
        for s, comb in dp.items():
            # optional pruning: skip sums that are already far out of reach
            ns = s + v
            if abs(ns) > hard_cap:
                continue
            if len(comb) >= 16:
                continue
            # keep the *shortest* combo for each achievable sum
            cand = comb + [i]
            if (ns not in ndp) or (len(cand) < len(ndp[ns])):
                ndp[ns] = cand
        dp = ndp

    # choose any sum within tolerance, preferring fewest rows
    best = None
    for s, comb in dp.items():
        if abs(s - target_c) <= tol_c:
            if best is None or len(comb) < len(best):
                best = comb
    return best

def _doc_suffix_score(pdf_docno: str, excel_doc: str) -> int:
    a = re.sub(r"\D", "", norm_docno(pdf_docno or ""))
    b = re.sub(r"\D", "", str(excel_doc or ""))
    i = 0
    while i < min(len(a), len(b)) and a[-1 - i] == b[-1 - i]:
        i += 1
    return i

def _build_doc_pool(book: pd.DataFrame, doccol: Optional[str], pdf_docno: str) -> tuple[pd.DataFrame, str, str]:
    if not doccol or not pdf_docno:
        return book, "none", "no-doccol-or-docno"

    eq_tokens, contains_tokens = build_doc_keys(pdf_docno)

    mask_eq = book["_doc_norm"].isin(eq_tokens)
    if mask_eq.any():
        pool = book[mask_eq]
        return pool, "doc_eq", f"eq={mask_eq.sum()}"

    scores = book["_doc_norm"].map(lambda x: _doc_suffix_score(pdf_docno, x))
    strong = (scores >= 8) & book["_doc_isnum"]
    if strong.any():
        best = scores[strong].max()
        pool = book[(scores == best) & strong]
        return pool, f"doc_suffix_{int(best)}", f"suffix_max={int(best)} kept={len(pool)}"

    mask_ct = pd.Series(False, index=book.index)
    for t in contains_tokens:
        mask_ct |= (book["_doc_isnum"] & book["_doc_norm"].str.contains(t, na=False))
    if mask_ct.any():
        pool = book[mask_ct]
        return pool, "doc_contains", f"contains kept={mask_ct.sum()} tokens={list(contains_tokens)}"

    return book, "none", "no-doc-match"
    
# --- extra generic matchers (template-agnostic) ---
TRAILING_TOTAL_RE = re.compile(
    r"(?i)\b(?:line\s*item|item|invoice|order)?\s*total\b[:\s]*"
)

def _extract_trailing_totals(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    lines = [l.strip() for l in block.splitlines()]
    items: list[LineItem] = []
    for i, line in enumerate(lines):
        if not line:
            continue
        if not TRAILING_TOTAL_RE.search(line):
            continue

        cands = _amount_candidates_in_text(line, page_currency)
        if not cands and i + 1 < len(lines):
            cands = _amount_candidates_in_text(f"{line} {lines[i+1]}", page_currency)
        pick = _pick_amount(cands, page_currency)
        if not pick:
            continue
        amount, currency, _, _ = pick
        if not _nonzero(amount):
            continue

        # gather 1–3 lines above to form a compact description, ignoring header-ish or “GST/VAT” noise
        desc_parts = []
        for k in range(i-1, max(-1, i-4), -1):
            if k < 0: break
            prev = lines[k].strip()
            if not prev:
                break  # blank line = new block
            if TABLE_HEADER_RE.search(prev) or TOTAL_WORDS.search(prev):
                break
            if SKIP_WORDS.search(prev):
                continue
            desc_parts.insert(0, prev)

            # stop once description has at least 2 meaningful tokens
            if len([t for t in _desc_tokens(" ".join(desc_parts))]) >= 2:
                break

        desc = _norm_space(" ".join(desc_parts)) or "Item Total"
        items.append(LineItem("", page1, fallback_date, desc[:160], currency, amount, "trail_total"))
    return items


def _extract_ragged_text_table(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    items: list[LineItem] = []
    lines = [l for l in block.splitlines()]
    for i, line in enumerate(lines):
        if not line.strip():
            continue

        cands = _amount_candidates_in_text(line, page_currency)
        endpos = len(line.strip())
        cands = [c for c in cands if c[3] == endpos]
        pick = _pick_amount(cands, page_currency)
        if not pick:
            continue
        amount, currency, start, _ = pick

        if TOTAL_ROW_RE.search(line):
            continue  # skip explicit grand totals; other logic catches them
        # description = text to the left of amount, possibly with the previous line
        left = _norm_space(line[:start].strip(" -:|•\u2022"))
        if SKIP_WORDS.search(left) or TABLE_HEADER_RE.search(left):
            continue

        desc = left
        # if too short, pull the previous line as prefix
        if len([t for t in _desc_tokens(desc)]) < 2 and i > 0:
            prev = _norm_space(lines[i-1])
            if prev and not (TABLE_HEADER_RE.search(prev) or TOTAL_ROW_RE.search(prev) or SKIP_WORDS.search(prev)):
                desc = _norm_space(prev + " " + desc)

        if _nonzero(amount) and desc and len([t for t in _desc_tokens(desc)]) >= 2:
            items.append(LineItem("", page1, fallback_date, desc[:160], currency, amount, "text_row"))
    return items

# ---------- main ----------
def clean_df_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("").astype(str).map(lambda x: BAD_XML.sub("", x))
    return df

def _extract_keyword_pairs(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    items = []
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    seen = set()
    for i, line in enumerate(lines):
        has_total = bool(TOTAL_WORDS.search(line))
        has_item  = bool(ITEM_LABELS.search(line)) and not has_total
        if not (has_total or has_item):
            continue
        cands = _amount_candidates_in_text(line, page_currency)
        if not cands and i + 1 < len(lines):
            # some templates split "amount" onto the next line
            cands = _amount_candidates_in_text(f"{line} {lines[i+1]}", page_currency)
        pick = _pick_amount(cands, page_currency)
        if not pick:
            continue
        amount, currency, _, _ = pick
        if not _nonzero(amount):
            continue

        kind = "total" if has_total else "item"
        dedup_key = (round(amount, 2), kind, TOTAL_WORDS.sub("", line.lower()), i)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        src = "kv_total" if has_total else "kv_item"
        items.append(LineItem("", page1, fallback_date, line[:160], currency, amount, src))
    return items

def _extract_rows_from_block(block: str, page1: int, pageN: Optional[int] = None, pdf_path: Optional[Path] = None) -> List[LineItem]:
    items: List[LineItem] = []

    # dates + fallback
    date_regex = re.compile("|".join(DATE_PATTERNS), re.I)
    dates = [(m.start(), m.end(), _normalize_date(m.group(0)) or "") for m in date_regex.finditer(block)]
    fallback_date = dates[-1][2] if dates else ""
    page_currency = detect_page_currency(block, "SGD")

    # (A) Generic text-only passes first (very cheap and resilient)
    items.extend(_extract_trailing_totals(block, page1, fallback_date, page_currency))
    items.extend(_extract_ragged_text_table(block, page1, fallback_date, page_currency))

    # (B) Header-style tables (text-mode)
    for li in _extract_header_table(block, page1, fallback_date, page_currency):
        li.source = "table_total" if TOTAL_WORDS.search(li.description) else "table"
        items.append(li)

    # (C) Layout (coordinates) across ALL pages in the detected window
    if pdf_path is not None and HAVE_PYMUPDF:
        last_page = pageN if (pageN and pageN >= page1) else page1
        for pg in range(page1, last_page + 1):
            for li in _extract_layout_table(pdf_path, pg, fallback_date, page_currency):
                li.source = "layout_total" if TOTAL_WORDS.search(li.description) else "layout"
                items.append(li)

    # (D) Key-value (“Total …”, “Amount …”) pairs
    items.extend(_extract_keyword_pairs(block, page1, fallback_date, page_currency))

    # free text (nearest date in either direction)
    for m_amt in AMT_TOKEN.finditer(block):
        a_start = m_amt.start()
        token   = m_amt.group(0)
        mnum    = AMT_NUM_IN_TOKEN.search(token)
        page_offset = block[:a_start].count("\f")
        this_page = page1 + page_offset
        if not mnum:
            continue

        amount   = _norm_amount(mnum.group("num"))
        if not _nonzero(amount):
            continue
        currency = _resolve_currency_from_token(token, page_currency)

        nearest, best_d = None, 10**9
        for ds, de, dnorm in dates:
            d = min(abs(a_start - ds), abs(a_start - de))
            if d <= ROW_GLUE and d < best_d:
                best_d = d
                nearest = (ds, de, dnorm)

        if nearest is not None:
            _, de, date_str = nearest
            start_idx = de
        else:
            line_start = block.rfind("\n", 0, a_start)
            start_idx = 0 if line_start < 0 else line_start + 1
            date_str = fallback_date

        raw = _norm_space(block[start_idx:a_start]).strip(" -:|•\u2022")
        if not raw:
            continue
        if SKIP_WORDS.search(raw):  # keep totals; skip other low-signal rows
            continue

        src = "summary_total" if TOTAL_WORDS.search(raw) else "summary"

        items.append(LineItem(
            file="", page=this_page, date=date_str, description=raw[:160],
            currency=currency, amount=amount, source=src
        ))

    return items

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="PDF file or folder")
    ap.add_argument("--out", required=True, help="Output prefix (no extension)")
    ap.add_argument("--pages", type=int, default=3, help="Max summary pages to scan (default 3)")
    ap.add_argument("--check-xlsx", help="Path to Excel to check (needs Description, Amount columns)")
    ap.add_argument("--match-threshold", type=int, default=80, help="Min description match score (0-100)")
    ap.add_argument("--price-tol", type=float, default=0.05, help="Amount tolerance")
    ap.add_argument("--doc-col", help="Explicit Excel column to use as document/reference id")
    ap.add_argument("--require-doc-match", action="store_true",
                    help="Do not fallback to description-only when docno can't be matched")
    ap.add_argument("--explain", action="store_true",
                    help="Add doc_trace/match_trace columns showing how the match was picked")
    ap.add_argument("--price-tol-abs", type=float, default=None,
                    help="Absolute amount tolerance (overrides --price-tol if set)")
    ap.add_argument("--price-tol-pct", type=float, default=0.0,
                    help="Relative tolerance in percent of the PDF amount (e.g., 1.0 = ±1%)")
    ap.add_argument("--docno-mode", default="auto",
                choices=["auto","po","order","invoice","inv","sv",
                         "segment","first_long_digits","last_long_digits","regex"],
                help="How to extract the document id from filename (default: auto)")
    ap.add_argument("--docno-segment-index", type=int, default=0,
                    help="When --docno-mode=segment, which segment index to return (default 0)")
    ap.add_argument("--docno-regex", default="",
                    help="When --docno-mode=regex, a regex with one capture group to return")
    args = ap.parse_args()

    in_path = Path(args.input)
    pdfs = sorted(in_path.glob("**/*.pdf")) if in_path.is_dir() else [in_path]

    extracted: List[LineItem] = []
    for pdf in pdfs:
        pages = extract_text_pages(pdf)
        stem = pdf.stem
        if args.docno_mode == "segment":
            docno = extract_doc_id(stem, mode="segment", segment_index=args.docno_segment_index)
        elif args.docno_mode == "regex":
            # expect --docno-regex to have one capture group; e.g. r"_(SV\d{6,})_"
            pat = args.docno_regex or r"(\d{8,})"
            docno = extract_doc_id(stem, mode=f"regex:{pat}:1")
        else:
            docno = extract_doc_id(stem, mode=args.docno_mode)
        # final hardening: if empty, fall back to normalized whole stem
        if not docno:
            docno = norm_docno(stem)
        block, p1, pN = _find_summary_block([_norm_space(t) for t in pages], max_pages=max(1, args.pages))
        rows = _extract_rows_from_block(block, p1, pN, pdf_path=pdf)
        for r in rows:
            r.file = pdf.name
            r.docno = docno
        extracted.extend(rows)

    df = pd.DataFrame([asdict(x) for x in extracted],   
                      columns=["file","page","date","description","currency","amount","source","docno"])
    if not df.empty:
        df["amount"] = df["amount"].astype(float)
        df = df[df["amount"].abs() > ZERO_EPS].copy()
        df["date"]   = pd.to_datetime(df["date"], errors="coerce")
        df.sort_values(["file","page","date"], inplace=True, kind="stable")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    clean_df_for_excel(df).to_excel(str(out.with_suffix(".items.xlsx")), index=False)
    df.to_csv(str(out.with_suffix(".items.csv")), index=False, encoding="utf-8",
              quoting=csv.QUOTE_MINIMAL, escapechar="\\")

    def _pick_idx_by_amount(cand_df: pd.DataFrame, target_amt: float, tol: float) -> int:
        df = cand_df.copy()
        df["_adiff"] = (df["Amount"] - target_amt).abs()
        within = df[df["_adiff"] <= tol]
        # Prefer amounts within tolerance; otherwise closest overall
        best = (within if not within.empty else df).sort_values("_adiff")
        return best.index[0]

    # --- compare to Excel ---
    if args.check_xlsx:
        book = load_claim_book(args.check_xlsx)
        if not {"Description", "Amount"} <= set(book.columns):
            raise ValueError("Excel must have columns: Description, Amount")

        book["Amount"] = pd.to_numeric(book["Amount"], errors="coerce")
        doccol = args.doc_col or detect_doc_column(book)
        if doccol is not None:
            book["_doc_norm"]   = normalize_id_series(book[doccol])
            book["_doc_digits"] = book["_doc_norm"].str.replace(r"\D","", regex=True)
            book["_doc_isnum"]  = book["_doc_digits"].str.fullmatch(r"\d{7,}").fillna(False)
        else:
            book["_doc_norm"]  = ""
            book["_doc_isnum"] = False
        book["_ndesc"] = norm_desc_series(book["Description"])

        matches = []
        for docno, gdoc in df.groupby("docno", sort=False):
            pool, doc_filter_used, doc_trace = _build_doc_pool(book, doccol, docno)

            if args.require_doc_match and doc_filter_used == "none":
                pool = book.iloc[0:0]

            pool = pool.copy()
            used_idx: set[int] = set()
            gdoc = gdoc.copy()
            gdoc["_ndesc"] = gdoc["description"].map(_norm_desc_text)

            for _, row in gdoc.iterrows():
                amount = float(row["amount"])
                ndesc  = row["_ndesc"]

                tol_abs = args.price_tol_abs if args.price_tol_abs is not None else args.price_tol
                tol_pct = float(args.price_tol_pct or 0.0)

                # candidates not yet used (NO fallback – enforces one-time use)
                cand = pool[~pool.index.isin(used_idx)]
                if cand.empty:
                    matches.append({
                        **row.drop(labels=["_ndesc"]).to_dict(),
                        "actual_row": None,
                        "match_desc": "", "match_amount": None,
                        "match_score": 0.0, "matched_tokens": "",
                        "status": "NO_MATCH",
                        "doc_filter_used": doc_filter_used,
                        "desc_rule": "none",
                    })
                    continue

                # ---------- STRICT PRICE-FIRST ONLY ----------
                cand_amt_ok = cand[cand["Amount"].notna() & cand["Amount"].apply(
                    lambda x: _within_price_tol(x, amount, tol_abs, tol_pct)
                )]

                if cand_amt_ok.empty:
                    # --- GROUP-SUM FALLBACK (same docno, same currency) ---
                    tol_abs_eff = (args.price_tol_abs if args.price_tol_abs is not None
                                else max(args.price_tol, abs(amount) * (args.price_tol_pct or 0.0) / 100.0))

                    # only consider *unused* candidate rows
                    cand_left = cand.copy()
                    # try to match by sum of several Excel rows
                    grp_idx = _subset_sum_group(
                        indices=cand_left.index.tolist(),
                        amounts=cand_left["Amount"].fillna(0).astype(float).tolist(),
                        target=amount,
                        tol_abs=tol_abs_eff,
                    )

                    if grp_idx:
                        # mark all chosen rows as used
                        for gi in grp_idx:
                            used_idx.add(gi)

                        # build a synthetic match record (use the first row’s number for 'actual_row')
                        book_idx   = grp_idx[0]
                        group_desc = " + ".join(str(book.loc[i, "Description"]) for i in grp_idx)
                        group_amt  = float(sum(book.loc[i, "Amount"] for i in grp_idx))

                        matches.append({
                            **row.drop(labels=["_ndesc"]).to_dict(),
                            "actual_row": int(book_idx) + 2,
                            "match_desc": group_desc[:160],
                            "match_amount": group_amt,
                            "match_score": 100.0,                 # price satisfied by group
                            "matched_tokens": "",
                            "status": "OK",                        # treat group-sum as OK
                            "doc_filter_used": doc_filter_used,
                            "desc_rule": "price_group_sum",
                            "group_rows": ",".join(str(i + 2) for i in grp_idx),  # Excel row numbers (1-based + header)
                            "group_size": len(grp_idx),
                        })
                        continue

                    # still nothing -> NO_MATCH
                    matches.append({
                        **row.drop(labels=["_ndesc"]).to_dict(),
                        "actual_row": None,
                        "match_desc": "", "match_amount": None,
                        "match_score": 0.0, "matched_tokens": "",
                        "status": "NO_MATCH",
                        "doc_filter_used": doc_filter_used,
                        "desc_rule": "price_required",
                    })
                    continue

                # Prefer exact normalized description within price-matched rows
                same_desc = cand_amt_ok[cand_amt_ok["_ndesc"] == ndesc]
                if not same_desc.empty:
                    book_idx   = _pick_idx_by_amount(same_desc, amount, tol_abs)
                    best_score = 100.0
                    desc_rule  = "price_then_exact"
                else:
                    # (Optional) keep only rows with >0 token overlap to avoid weak picks
                    def _cov_only(d):
                        cov, _ = desc_coverage(row["description"], str(d))
                        return cov
                    tmp = cand_amt_ok.copy()
                    tmp["_cov"] = tmp["Description"].map(_cov_only)
                    strong = tmp[tmp["_cov"] > 0]  # require any token overlap
                    base = strong if not strong.empty else tmp

                    # tie-break by best coverage, then by closest amount
                    best_i, best_score = best_coverage_match(row["description"], base["Description"])
                    book_idx = best_i if best_i in base.index else base.index[0]
                    desc_rule = "price_then_coverage"

                # mark the chosen Excel row as used globally within this doc group
                used_idx.add(book_idx)

                # ---------- Scoring & status ----------
                b_desc = str(book.loc[book_idx, "Description"])
                b_amt  = float(book.loc[book_idx, "Amount"]) if pd.notna(book.loc[book_idx, "Amount"]) else None

                contains_ok = _contains_same_words(row["description"], b_desc)
                desc_ok = (best_score >= args.match_threshold) or contains_ok

                adiff = None
                relpct = None
                if b_amt is not None:
                    adiff = abs(b_amt - amount)
                    relpct = (adiff / abs(amount) * 100.0) if amount else None

                price_ok = _within_price_tol(b_amt, amount, tol_abs, tol_pct)

                status = "OK" if (desc_ok and price_ok) else (
                        "AMOUNT_MISMATCH" if (desc_ok and not price_ok) else
                        "DESC_MISMATCH"  if (price_ok and not desc_ok) else
                        "FLAG")

                _, matched_tokens = desc_coverage(row["description"], b_desc)

                matches.append({
                    **row.drop(labels=["_ndesc"]).to_dict(),
                    "actual_row": int(book_idx) + 2,
                    "match_desc": b_desc,
                    "match_amount": b_amt,
                    "match_score": round(best_score, 1),
                    "matched_tokens": " ".join(sorted(matched_tokens)),
                    "status": status,
                    "doc_filter_used": doc_filter_used,
                    "desc_rule": desc_rule,
                })

        md = pd.DataFrame(matches)
        # --- drop MISMATCH/FLAG rows when an OK exists for same doc+price ---
        md = md.copy()

        # robust key: docno + currency + amount (2dp) + total/non-total
        md["amount_2dp"] = md["amount"].round(2)
        md["is_total"]   = md["source"].str.contains("total", case=False, na=False)
        md["_ndesc_pdf"] = md["description"].str.lower().str.replace(r"\s+", " ", regex=True)
        key = ["docno", "currency", "amount_2dp", "is_total", "actual_row"]

        # 1) If an OK exists for a key, drop the non-OK rows for that same key
        ok_keys   = pd.MultiIndex.from_frame(md.loc[md["status"].eq("OK"), key]).unique()
        all_keys  = pd.MultiIndex.from_frame(md[key])
        drop_mask = all_keys.isin(ok_keys) & md["status"].ne("OK")
        md = md.loc[~drop_mask].copy()

        # 2) (Optional) collapse multiple OK rows per key to the best by match_score
        ok = md[md["status"].eq("OK")].copy()
        ok = (ok.sort_values(key + ["match_score"], ascending=[True, True, True, True, True , False])
                .drop_duplicates(subset=key, keep="first"))
        rest = md[~md["status"].eq("OK")]
        md = pd.concat([ok, rest], ignore_index=True)

        # now drop helper cols
        md.drop(columns=["amount_2dp", "is_total"], inplace=True)

        clean_df_for_excel(md).to_excel(str(out.with_suffix(".check.xlsx")), index=False)
        md.to_csv(str(out.with_suffix(".check.csv")), index=False, encoding="utf-8",
                  quoting=csv.QUOTE_MINIMAL, escapechar="\\")

        # --- comments back to the claim book ---
        book_out = book.copy()
        comments = pd.Series("NO_MATCH", index=book_out.index)
        best_status = pd.Series("NO_MATCH", index=book_out.index)

        best_rule   = pd.Series("", index=book_out.index)
        best_score  = pd.Series(pd.NA, index=book_out.index)
        best_amount = pd.Series(pd.NA, index=book_out.index)

        if not md.empty:
            md_used = md[md["actual_row"].notna()].copy()
            if not md_used.empty:
                md_used["actual_row"] = md_used["actual_row"].astype(int)

                for actual_row, grp in md_used.groupby("actual_row"):
                    idx0 = actual_row - 2  # DataFrame index (0-based)
                    if 0 <= idx0 < len(book_out):
                        # keep your existing comment synthesis
                        comments.iloc[idx0] = _comment_from_group(grp)

                        # choose the "best" row by status priority, then match_score
                        g = grp.copy()
                        g["__prio"] = g["status"].map(_PRIORITY).fillna(0)
                        best = g.sort_values(["__prio", "match_score"],
                                            ascending=[False, False]).iloc[0]

                        best_status.iloc[idx0] = str(best["status"])
                        best_rule.iloc[idx0]   = str(best.get("desc_rule", ""))
                        best_score.iloc[idx0]  = float(best.get("match_score", float("nan")))
                        best_amount.iloc[idx0] = best.get("match_amount", pd.NA)

        book_out["comments"] = comments
        comments_path = str(out.with_suffix(".claims_with_comments.xlsx"))
        book_out.to_excel(comments_path, index=False)
        print(f"Wrote: {comments_path}")

    print("Done.")

if __name__ == "__main__":
    main()