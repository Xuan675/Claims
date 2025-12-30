# pdf_pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import List
from dataclasses import dataclass, asdict
from typing import Optional, Tuple
from datetime import datetime
import re, csv, io
import shutil, hashlib
import pandas as pd
import pytesseract
import fitz
from pytesseract import Output
import pandas as pd  # type: ignore

HAVE_PYMUPDF = False
HAVE_PYPDF2  = False

try:
    import fitz
    HAVE_PYMUPDF = True
except ImportError:
    pass

try:
    from pypdf import PdfReader
    HAVE_PYPDF2 = True
except ImportError:
    pass

try:
    import pytesseract
    HAVE_TESS = True
except Exception:
    HAVE_TESS = False

ZERO_EPS = 0.004
ROW_GLUE = 800
TESS_CONFIG = r"--oem 3 --psm 6 -c preserve_interword_spaces=1"
NUMERIC_TESS_CFG = r"--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789.,()$€£¥SsUSDAUDSGDHKDNTDRM"

DATE_PATTERNS = [
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",          # 31/7/2025
    r"\b\d{4}-\d{2}-\d{2}\b",              # 2025-07-31
    r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b" # 7 May 2025
]
BROKEN_CENTS_RX = re.compile(r"(\d{2,})\s*[oO]?\s*(\d{2})(?!\d)")
AMOUNT_ONLY_LINE = re.compile(r"^\s*[-(]?\s*\d{1,3}(?:[ ,.\u00A0\u2007\u202F]?\d{3})*(?:[.,]\d{2})?\)?\s*$")
CUR_ISO  = r"(?:SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)"
DATE_RX  = r"(?:\d{1,2}/\d{1,2}/\d{4})"
TWO_DIG_LINE = re.compile(r"^\s*(\d{2})\s*$")
CUR_ONLY_RE  = re.compile(rf"^\s*(?:{CUR_ISO})\s*$", re.I)
END_WITH_CUR = re.compile(rf"(?:{CUR_ISO})\s*$", re.I)
START_DATE_RE= re.compile(rf"^\s*{DATE_RX}\b")
DATE_AT_END_RE = re.compile(rf"{DATE_RX}\s*$")
TABLE_HEADER_RE = re.compile(
    r"(description|item|details|particulars|merchant|vendor|expense\s*(type)?|category|purpose|comments?)"
    r".{0,100}(amount|total(?:\s*price)?|price|value|subtotal)",
    re.I
)
TOTAL_PHRASES = (
    r"(?:grand\s*total)"
    r"|(?:sub\s*total|subtotal)"
    r"|(?:total\s+amount(?:\s+payable(?:\s+including\s+gst)?)?)"
    r"|(?:amount\s+due(?:\s*\(subtotal\))?)"
    r"|(?:balance\s+due)"
    r"|(?:total\s+due)"
    r"|(?:total\s+amount\b)"
    r"|(?:\btotal\b)"
    r"|(?:\bTotal\b)"
)
TOTAL_ROW_RE = re.compile(rf"^\s*(?:{TOTAL_PHRASES})\b",re.I)
TOTAL_WORDS = re.compile(rf"\b(?:{TOTAL_PHRASES})\b", re.I)
NBSP = "\u00A0\u2007\u202F"  # common NBSP variants
SEP = f"[ ,.{NBSP}'\u2019]"  # sp   ace, comma, dot, NNBSPs, apostrophe, right-single-quote
CURR = r"(?:SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB|NT\$|US\$|S\$|A\$|HK\$|C\$|\$|€|£|¥)"
AMOUNT_TOKEN = re.compile(
    rf"""
    (?<!\w)                                   # don’t start in the middle of a word
    (?:\(|-)?\s*                              # optional negative
    (?:{CURR})?\s*                            # optional leading currency
    (?:                                       # integer with optional group seps
        \d{{1,3}}(?:{SEP}?\d{{3}})+ | \d+     # 1,234 or 1234
    )
    (?:                                       # optional decimals with dot or comma
        [.,]\d{{1,2}}
    )?
    (?:{SEP}?-(?:-)?|{SEP}?–)?                # optional Swiss-style .- or typeset dash
    \s*(?:{CURR})?                            # optional trailing currency
    \)?                                       # optional closing paren
    (?!\w)                                    # don’t end in the middle of a word
    """,
    re.VERBOSE,
)
DECIMAL  = r"\d{1,3}(?:,\d{3})*\.\d{1,2}"
INTEGER  = r"\d{1,3}(?:,\d{3})*"
AMT_NUM_IN_TOKEN = re.compile(rf"(?P<num>{DECIMAL}|{INTEGER})")
ISO_SET = {"SGD","USD","EUR","GBP","JPY","MYR","HKD","TWD","AUD","NZD","CAD","CNY","RMB"}
CURRENCY_MAP = {
    "US$": "USD", "S$": "SGD", "A$": "AUD", "HK$": "HKD", "NT$": "TWD",
    "$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY"
}
DATE_INLINE_RE = re.compile("|".join(DATE_PATTERNS), re.I)
INCL_HINT = re.compile(r"(?i)(including\s+gst|incl\.?\s+gst|inc\.?\s+gst|with\s+gst)")
EXCL_HINT = re.compile(r"(?i)(excluding\s+gst|excl\.?\s+gst|without\s+gst)")
GST_PAYABLE_HINT = re.compile(r"(?i)\bgst\s*(?:payable|tax)\b")
TOTAL_WORDS_LOOSE = re.compile(
    r"(?i)total\s+amount\s+payable(?:\s+(?:including|excluding)\s+gst)?"
)
GST_TOTAL_HINT = re.compile(r"(?i)\b(?:including|excluding)\s+gst\b")
AMOUNT_DUE_RE  = re.compile(r"(?i)\bamount\s+due\b")
COL_SPLIT       = re.compile(r"\s{2,}")
SKIP_WORDS  = re.compile(
    r"\b(gst|vat|tax|voucher|cash\s*advance|utili[sz]ed|shipping|delivery|discount|"
    r"service\s*charge|fee|charges?)\b", re.I
)
ITEM_LABELS = re.compile(r"\b(?:line\s+item\s+total|amount)\b",re.I)
TRAILING_TOTAL_RE = re.compile(
    r"(?i)\b("
    r"(?:line\s*item|item|invoice|order)?\s*total"        # existing
    r"|total\s+paid\s+by\s+company"                       # new
    r"|total\s+paid\s+by\s+employee"                      # new
    r"|amount\s+due\s+employee"                           # new
    r"|amount\s+due\s+company\s+card(?:\s+from\s+employee)?"  # new
    r"|company\s+disbursements"                           # optional cue line
    r")\b[:\s]*"
)
STOPWORDS = {
    "the","a","an","and","or","of","for","to","in","on","at","by",
    "with","from","this","that","these","those",
    "invoice","receipt","qty","quantity","pcs","unit","item","items"
}
AMOUNT_DUE_DUAL = re.compile(
    r"(?i)\bamount\s+due\b.*?([$\u20ac\u00a3\u00a5]?\s*\d{1,3}(?:,\d{3})*\.\d{2})\s*USD.*?"
    r"([$\u20ac\u00a3\u00a5]?\s*\d{1,3}(?:,\d{3})*\.\d{2})\s*SGD"
)
CURRENCIES = [
    r"SGD", r"S\$", r"USD", r"US\$", r"AUD", r"A\$", r"NZD", r"CAD",
    r"EUR", r"GBP", r"CNY", r"RMB", r"JPY", r"MYR", r"RM", r"HKD",
    r"NTD", r"NT\$",
    r"\$", r"€", r"£", r"¥", r"HK\$", r"NT\$"
]
CURRENCY = r"(?:" + "|".join(CURRENCIES) + r")"
AMT_DEC = re.compile(rf"(?:{CURRENCY}\s*)?(?P<num>{DECIMAL})(?:\s*{CURRENCY})?", re.I)
AMT_INT = re.compile(rf"(?:{CURRENCY}\s*)(?P<num>{INTEGER})\b", re.I)
CUR_PAREN_AFTER_RE = re.compile(r"^\s*\(?\s*(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)\s*\)?\b", re.I)
CUR_BEFORE_RE      = re.compile(r"(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)\s*[:\-]?\s*$", re.I)
CUR_AFTER_RE = re.compile(r"^\s*(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)\b", re.I)
DATE_LINE_RX = re.compile(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$")
AMOUNT_ONLY_RE = re.compile(r"^\s*\d{1,4}(?:[.,]\d{2})\s*$")
AMT_TAIL = r"[-()0-9 ,.\u00A0\u2007\u202F]+"
ROW_RX = re.compile(
    rf"""
    ^(?P<vendor>\S.+?)\s{{2,}}
    (?P<claim_type>\S.+?)\s{{2,}}
    (?P<currency>{CUR_ISO})\s+(?P<date>{DATE_RX})\s{{2,}}
    (?P<desc>.+?)\s{{2,}}
    (?P<amount>{AMT_TAIL})$
    """,
    re.VERBOSE
)
COPY_SUFFIX_RE    = re.compile(r"\(\s*\d+\s*\)$")
VERSION_SUFFIX_RE = re.compile(r"(?i)(?:[ _-]v\d+)$")
COPY_ORIG_RE = re.compile(r'(?i)\b(original|compressed|copy|scanned)\b')
DOC_PATTERNS = [
    # explicit tags first
    (re.compile(r'(?i)\bPO\s*0*([0-9]{6,})\b'),          "po_digits"),
    (re.compile(r'(?i)\bOrder\s*0*([0-9]{6,})\b'),       "order_digits"),
    (re.compile(r'(?i)\bSO\s*0*([0-9]{6,})\b'),          "so_digits"),
    (re.compile(r'(?i)\bINV(?:OICE)?\s*0*([0-9]{5,})\b'),"inv_digits"),
    (re.compile(r'(?i)\bSV0*([0-9]{6,})\b'),             "sv_digits"),
]

@dataclass
class LineItem:
    file: str
    page: int
    date: str
    description: str
    currency: str
    amount: float
    source: str
    docno: str = ""

def read_debug_pages_for_pdf(pdf_path: Path, debug_root: Optional[Path] = None) -> List[str]:
    """
    Try shared <debug_root>/debug_pages first, then legacy:
      - <pdf_dir>/debug_pages
      - <pdf_path without suffix>/debug_pages
    Filenames must be <safe_stem>_pNN.txt.
    """
    candidates: List[Path] = []

    # Preferred shared location
    if debug_root is not None:
        shared = Path(debug_root) / "debug_pages"
        if shared.exists():
            candidates.append(shared)

    # Legacy sibling folder
    legacy_sibling = pdf_path.parent / "debug_pages"
    if legacy_sibling.exists():
        candidates.append(legacy_sibling)

    # Legacy folder named after pdf
    legacy_under_pdf = pdf_path.with_suffix("") / "debug_pages"
    if legacy_under_pdf.exists():
        candidates.append(legacy_under_pdf)

    if not candidates:
        return []

    stem = _safe_name(pdf_path.stem)
    rx = re.compile(rf"^{re.escape(stem)}_p(\d+)\.txt$", re.I)

    for folder in candidates:
        hits = []
        for p in folder.iterdir():
            m = rx.match(p.name)
            if m:
                try:
                    hits.append((int(m.group(1)), p))
                except ValueError:
                    pass
        if hits:
            hits.sort(key=lambda t: t[0])
            out: List[str] = []
            for _, fp in hits:
                try:
                    out.append(fp.read_text(encoding="utf-8"))
                except Exception:
                    out.append("")
            return out

    return []

def _safe_name(stem: str, maxlen: int = 60) -> str:
    """Filesystem-safe, short name with a hash suffix to avoid collisions."""
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("_")
    if len(base) <= maxlen:
        return base
    h = hashlib.md5(stem.encode("utf-8")).hexdigest()[:8]
    return f"{base[:maxlen-9]}_{h}"

def normalize_debug_wrapping(block: str) -> str:
    """
    Fix common hard-wrap artifacts in debug_pages / text dumps:
    1) line ends with currency and the next line starts with a date    -> join
    2) amount broken across newline as '1234' \\n '56'                -> join to '1234.56'
    3) collapse NBSPs and excessive spaces
    """
    lines = block.splitlines()
    out = []
    i = 0
    while i < len(lines):
        cur = lines[i]
        nxt = lines[i+1] if i + 1 < len(lines) else ""

        # (A) Amount-only line following a row that already has CUR+DATE -> join
        #     e.g. "... SGD 14/03/2025 <desc>" \n "55.28"  => one line
        if AMOUNT_ONLY_LINE.match(cur) and i > 0:
            prev = out[-1] if out else lines[i-1]
            if re.search(rf"{CUR_ISO}\s+{DATE_RX}", prev, flags=re.I):
                if out:
                    out[-1] = prev.rstrip() + " " + cur.strip()
                else:
                    out.append(prev.rstrip() + " " + cur.strip())
                i += 1
                continue

        # (B) Pure integer on this line and 2-digit line next -> make cents
        #     e.g. "55" \n "28" => "55.28"
        if re.fullmatch(r"\s*\d{1,3}\s*", cur) and re.fullmatch(r"\s*\d{2}\s*", nxt):
            out.append(cur.strip() + "." + nxt.strip())
            i += 2
            continue

        # (C) Previous logic: number ending + 2-digit line => join as cents (keep)
        if i + 1 < len(lines):
            prev = cur.rstrip()
            if re.search(r"\d\s*$", prev) and TWO_DIG_LINE.match(nxt):
                cents = TWO_DIG_LINE.match(nxt).group(1)
                if re.search(r"\d{2,}\s*$", prev):
                    out.append(prev + "." + cents)
                    i += 2
                    continue

        if re.search(r"\d{2,}\s+\d{2}$", cur):  # e.g. "867 60"
            out.append(re.sub(r"(\d{2,})\s+(\d{2})$", r"\1.\2", cur))
            i += 1
            continue

        # (1) "... <CUR>"  +  "\n<DATE> ..."  => join
        if END_WITH_CUR.search(cur) and START_DATE_RE.search(nxt):
            out.append(cur.rstrip() + " " + nxt.lstrip())
            i += 2
            continue

        # (1b) "<CUR>" alone line followed by date line => join
        if CUR_ONLY_RE.match(cur) and START_DATE_RE.search(nxt):
            out.append(cur.strip() + " " + nxt.lstrip())
            i += 2
            continue

        # (1c) "... <CUR>"  +  "\n<AMOUNT>"  => join (Concur wrap: currency on its own line, amount on next)
        if END_WITH_CUR.search(cur) and AMOUNT_ONLY_LINE.match(nxt):
            out.append(cur.rstrip() + " " + nxt.strip())
            i += 2
            continue

        # (1d) "<CUR>" alone line followed by "<AMOUNT>" line => join
        if CUR_ONLY_RE.match(cur) and AMOUNT_ONLY_LINE.match(nxt):
            out.append(cur.strip() + " " + nxt.strip())
            i += 2
            continue

        # (1e) "<DATE>" line followed by currency-only line  => join
        if DATE_AT_END_RE.search(cur) and CUR_ONLY_RE.match(nxt):
            # (1f) also fold in amount-only on the next-next line
            nn = lines[i+2] if i + 2 < len(lines) else ""
            if AMOUNT_ONLY_LINE.match(nn):
                out.append(cur.rstrip() + " " + nxt.strip() + " " + nn.strip())
                i += 3
                continue
            out.append(cur.rstrip() + " " + nxt.strip())
            i += 2
            continue
            
        # (1f) <DATE> line, then 1–3 description lines, then currency-only,
        #      then amount-only  => join into one logical row:
        #      "DATE <desc...> CUR AMOUNT"
        if DATE_AT_END_RE.search(cur):
            j = i + 1
            mid = []
            # collect up to 3 short description lines that are not headers/totals/currency/amount
            while j < len(lines) and len(mid) < 3:
                probe = lines[j].strip()
                if not probe:   
                    break
                if CUR_ONLY_RE.match(probe) or AMOUNT_ONLY_LINE.match(probe):
                    break
                if TABLE_HEADER_RE.search(probe) or TOTAL_ROW_RE.search(probe):
                    break
                mid.append(probe)
                j += 1

            # expect currency-only next
            if j < len(lines) and CUR_ONLY_RE.match(lines[j]):
                # and an amount-only right after currency
                if (j + 1) < len(lines) and AMOUNT_ONLY_LINE.match(lines[j+1]):
                    joined = cur.rstrip()
                    if mid:
                        joined += " " + " ".join(m.strip() for m in mid if m.strip())
                    joined += " " + lines[j].strip() + " " + lines[j+1].strip()
                    out.append(joined)
                    i = j + 2
                    continue

        # (2) "... 867" + "\n" + "60"  => "... 867.60"
        if i + 1 < len(lines):
            prev = cur.rstrip()
            if re.search(r"\d\s*$", prev) and TWO_DIG_LINE.match(nxt):
                cents = TWO_DIG_LINE.match(nxt).group(1)
                if re.search(r"\d{2,}\s*$", prev):  # avoid invoice numbers like "... 8"
                    out.append(prev + "." + cents)
                    i += 2
                    continue
        
        if re.search(r"\d{3,}\s*$", cur) and re.match(r"^\s*\d{2}\s*$", nxt):
            out.append(cur.rstrip() + "." + nxt.strip())
            i += 2
            continue

        out.append(cur)
        i += 1

    text = "\n".join(out)
    text = text.replace("\u00A0", " ").replace("\u2007", " ").replace("\u202F", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = join_broken_cents(text)
    return text

def join_broken_cents(text: str) -> str:
    def repl(m):
        left, cents = m.group(1), m.group(2)
        # don’t re-dot numbers that already have a decimal
        if "." in left or "," in left:
            return f"{left}{cents}"
        # be conservative: require >=3 digits on the left to avoid “8 67 -> 8.67”
        if len(left) < 3:
            return f"{left}{cents}"
        return f"{left}.{cents}"
    return BROKEN_CENTS_RX.sub(repl, text)

def extract_text_pages(
    pdf_path: Path,
    ocr_if_empty: bool = True,
    ocr_lang: str = "eng",
    dump_root: Optional[Path] = None
) -> Tuple[List[str], List[int]]:
    import fitz

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"Skipping file {pdf_path}: {e}")
        return [], []
    if doc is None:
        return [], []

    pages: List[str] = []
    errors: List[str] = []

    # Shared debug dir (preferred); fallback to sibling if not provided
    dbg_pages = (Path(dump_root) / "debug_pages") if dump_root is not None else (pdf_path.parent / "debug_pages")
    dbg_pages.mkdir(parents=True, exist_ok=True)
    safe = _safe_name(pdf_path.stem)

    if HAVE_PYMUPDF:
        from PIL import Image, ImageOps
        Image.MAX_IMAGE_PIXELS = None
        pages_text: List[str] = []
        garbage_pages: List[int] = []
        doc = fitz.open(str(pdf_path))

        for i, page in enumerate(doc, start=1):
            txt = page.get_text("text") or ""
            # OCR if looks empty/garbage
            if (len(txt.strip()) < 25 or _looks_like_garbage(txt)) and HAVE_TESS:
                try:
                    txt_ocr = _ocr_page_to_text(
                        page, pdf_path.stem, i, dpi=300, lang=ocr_lang,
                        out_prefix=dump_root if dump_root is not None else None
                    )
                    if not _looks_like_garbage(txt_ocr):
                        txt = txt_ocr
                except Exception:
                    pass

            pages_text.append(txt)
            (dbg_pages / f"{safe}_p{i:02d}.txt").write_text(txt or "", encoding="utf-8")
            if (len((txt or "").strip()) < 25) or _looks_like_garbage(txt or ""):
                garbage_pages.append(i)

        # Optional: textdump/garbage index under dump_root
        if dump_root is not None and garbage_pages:
            td_root = Path(dump_root) / "textdump"
            gar_dir = td_root / "garbage"
            gar_dir.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(str(pdf_path), str(gar_dir / pdf_path.name))
            except Exception:
                pass

            combo = []
            for i, t in enumerate(pages_text, start=1):
                flag = " (GARBAGE)" if i in garbage_pages else ""
                combo.append(f"--- Page {i}{flag} ---\n{t or ''}\n")
            (gar_dir / f"{safe}.txt").write_text("".join(combo), encoding="utf-8")

            idx_csv = td_root / "index.csv"
            header_needed = not idx_csv.exists()
            with idx_csv.open("a", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                if header_needed:
                    w.writerow(["file", "garbage_pages"])
                w.writerow([pdf_path.name, ",".join(map(str, garbage_pages))])

        return pages_text, garbage_pages

    # ----- PyPDF2 fallback -----
    if HAVE_PYPDF2:
        try:
            from pypdf import PdfReader
            reader = PdfReader(str(pdf_path))
            for page in reader.pages:
                try:
                    t = (page.extract_text() or "").strip()
                    pages.append(t)
                except Exception as e:
                    errors.append(f"pypdf page extract: {type(e).__name__}: {e}")
                    pages.append("")

            # OCR any empty pages via PyMuPDF+Tesseract if available
            if ocr_if_empty and HAVE_PYMUPDF and HAVE_TESS:
                import fitz
                doc = fitz.open(str(pdf_path))
                for i, t in enumerate(pages):
                    if len(t) < 25:
                        try:
                            pix = doc[i].get_pixmap(matrix=fitz.Matrix(300/72, 300/72), alpha=False)
                            from PIL import Image, ImageOps
                            Image.MAX_IMAGE_PIXELS = None
                            img = Image.open(io.BytesIO(pix.tobytes("png")))
                            img = ImageOps.exif_transpose(img)
                            dbg = None
                            if 'args' in globals() and getattr(args, 'out', None):
                                dbg = Path(args.out).with_suffix("") / "ocr_data"
                            pages[i] = _ocr_image_to_data_text(
                                img, ocr_lang, pdf_path.stem, i+1, config=TESS_CONFIG, debug_dir=dbg
                            ) or t
                        except Exception as e:
                            errors.append(f"pypdf OCR page {i+1}: {type(e).__name__}: {e}")

            # write dumps
            for i, text in enumerate(pages, 1):
                (dbg_pages / f"{safe}_p{i:02d}.txt").write_text(text or "", encoding="utf-8")
            return pages, []

        except Exception as e:
            errors.append(f"pypdf open/extract: {type(e).__name__}: {e}")

    # ----- pdfminer fallback -----
    try:
        from pdfminer.high_level import extract_pages as pm_extract_pages
        from pdfminer.layout import LTTextContainer
        pages = []
        for layout in pm_extract_pages(str(pdf_path)):
            buf = []
            for elem in layout:
                if isinstance(elem, LTTextContainer):
                    buf.append(elem.get_text())
            pages.append("".join(buf).strip())
        if pages:
            for i, text in enumerate(pages, 1):
                (dbg_pages / f"{safe}_p{i:02d}.txt").write_text(text or "", encoding="utf-8")
            return pages, []
    except Exception as e:
        errors.append(f"pdfminer: {type(e).__name__}: {e}")

    detail = " | ".join(errors) if errors else "no errors captured"
    print(f"Skipping file {pdf_path}: All backends failed. Details: {detail}")
    return [], []

def _looks_like_garbage(s: str) -> bool:
    if not s:
        return True
    n = len(s)
    printable = sum(ch.isprintable() or ch in "\t\n\r" for ch in s)
    frac_printable = printable / max(1, n)
    wordy = sum(ch.isalnum() or ch.isspace() for ch in s)
    frac_wordy = wordy / max(1, n)
    import re
    words = re.findall(r"[A-Za-z]{3,}", s)
    word_ratio = len(words) / max(1, n/30)
    head = s[:1500]
    non_ascii = sum(ord(c) > 126 for c in head)
    frac_non_ascii_head = non_ascii / max(1, len(head))
    return (
        frac_printable < 0.90 or
        frac_wordy     < 0.55 or
        word_ratio     < 0.15 or
        frac_non_ascii_head > 0.25
    )

def _ocr_page_to_text(page, pdf_stem: str, page_idx1: int, dpi=360, lang="eng", out_prefix: Optional[Path]=None):
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    import io, PIL.Image, PIL.ImageOps as ImageOps
    img = PIL.Image.open(io.BytesIO(pix.tobytes("png")))
    img = ImageOps.exif_transpose(img)

    # optional debug dir (same convention as ocr_tester)
    debug_dir = None
    if out_prefix is not None:
        debug_dir = Path(out_prefix).with_suffix("") / "ocr_data"

    return _ocr_image_to_data_text(img, lang=lang, pdf_name=pdf_stem,
                                   page_num=page_idx1, config=TESS_CONFIG,
                                   debug_dir=debug_dir) or ""

def _ocr_image_to_data_text(img, lang: str, pdf_name: str, page_num: int,
                            config: str = TESS_CONFIG, debug_dir: Optional[Path] = None) -> str:
    pimg = _preprocess_for_ocr(img)
    df = pytesseract.image_to_data(pimg, lang=lang, config=config, output_type=Output.DATAFRAME)
    df = df[df.conf != -1].copy()
    df["text"] = df["text"].fillna("").astype(str)
    try:
        df = _re_ocr_numeric_tokens(pimg, df)
    except Exception:
        pass
    lines = []
    for (_, _, line_num), g in df.groupby(["block_num","par_num","line_num"], sort=True):
        words = [w for w in g["text"].tolist() if w.strip()]
        if words:
            lines.append(" ".join(words))
    text = "\n".join(lines)
    if debug_dir is not None:
        debug_dir.mkdir(parents=True, exist_ok=True)
        base = f"{pdf_name}_p{page_num:02d}"
        df.to_csv(debug_dir / f"{base}.ocr_words.csv", index=False, encoding="utf-8")
    return text

def _preprocess_for_ocr(pil_img):
    import PIL.ImageOps as ImageOps, numpy as np, PIL.Image as Image
    w, h = pil_img.size
    img = pil_img.convert("L").resize((w*2, h*2), Image.Resampling.LANCZOS)
    try:
        import cv2
        arr = np.array(img)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        arr = clahe.apply(arr)
        arr = cv2.adaptiveThreshold(arr, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                    cv2.THRESH_BINARY, 31, 8)
        return Image.fromarray(arr)
    except Exception:
        return ImageOps.autocontrast(img, cutoff=2)
    
def _re_ocr_numeric_tokens(img, df_words):
    import numpy as np, pytesseract
    from PIL import Image
    Image.MAX_IMAGE_PIXELS = None
    if df_words.empty:
        return df_words
    def _looks_money(t):
        if not t: return False
        t = str(t).strip()
        return bool(re.search(r"^[($€£¥S]?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\)?$|^\d+[.,]\d{2}$", t))
    need_fix = df_words[(df_words["conf"] >= 0) & ((df_words["conf"] < 90) | (df_words["text"].map(_looks_money)))].copy()
    if need_fix.empty:
        return df_words
    arr = np.array(img.convert("L"))
    out = df_words.copy()
    for i, r in need_fix.iterrows():
        try:
            x, y, w, h = int(r["left"]), int(r["top"]), int(r["width"]), int(r["height"])
            pad = max(2, int(0.15*h))
            crop = arr[max(0,y-pad):y+h+pad, max(0,x-pad):x+w+pad]
            if crop.size == 0: 
                continue
            crop_img = Image.fromarray(crop)
            txt2 = pytesseract.image_to_string(crop_img, lang="eng", config=NUMERIC_TESS_CFG).strip()
            txt2 = re.sub(r"\s+", "", txt2).replace("O","0").replace("o","0").replace("l","1").replace("I","1").replace("S","5").replace("B","8")
            def _score(t):
                t = (t or "").strip().replace("—","-").replace("–","-")
                t = re.sub(r"(\d)[\.\-]$", r"\1", t)
                try:
                    float(re.sub(r"[^\d\.\-]", "", t.replace(",", "")))
                    return 2
                except Exception:
                    return 0
            if _score(txt2) > _score(str(r["text"])):
                out.at[i, "text"] = txt2
        except Exception:
            pass
    return out

def _extract_rows_from_block(block: str, page1: int, pageN: Optional[int] = None, pdf_path: Optional[Path] = None) -> List[LineItem]:
    items: List[LineItem] = []

    block = normalize_debug_wrapping(block)
    block = normalize_for_amounts(block)
    page_currency = detect_page_currency(block, "SGD")
    # dates + fallback
    date_regex = re.compile("|".join(DATE_PATTERNS), re.I)
    dates = [(m.start(), m.end(), _normalize_date(m.group(0)) or "") for m in date_regex.finditer(block)]
    fallback_date = dates[-1][2] if dates else ""
    page_currency = detect_page_currency(block, "SGD")

    items.extend(_extract_dual_currency_amount_due(block, page1, fallback_date))
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
    for m_amt in AMOUNT_TOKEN.finditer(block):
        a_start = m_amt.start()
        token   = m_amt.group(0)

        mnum    = AMT_NUM_IN_TOKEN.search(token)
        page_offset = block[:a_start].count("\f")
        this_page = page1 + page_offset
        if not mnum:
            continue
        amount = parse_amount_loose(token)
        if amount is None or not _nonzero(amount):
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

        # Build a compact same-line description using the helper
        line_end = block.find("\n", a_start)
        if line_end < 0:
            line_end = len(block)
        line_text = block[start_idx:line_end]
        rel_start = a_start - start_idx

        left = _compact_desc_from_line(
            line_text, rel_start,
            min_tokens=2, max_tokens=12, min_chars=12, max_chars=64
        )
        desc = left

        # If still thin, bring in one neighbor line above/below (skipping headers/totals/GST)
        def _good_desc(txt: str) -> bool:
            toks = [t for t in _desc_tokens(txt)]
            return (len(toks) >= 2 and len(txt) >= 12)

        if not _good_desc(desc):
            # previous line
            prev_start = block.rfind("\n", 0, start_idx)
            if prev_start >= 0:
                prev_line = _norm_space(block[prev_start+1:start_idx])
                if prev_line and not (TABLE_HEADER_RE.search(prev_line)
                                    or TOTAL_ROW_RE.search(prev_line)
                                    or SKIP_WORDS.search(prev_line)):
                    desc = _norm_space(prev_line + " " + desc)

        if not _good_desc(desc):
            # next line
            nxt_end = block.find("\n", line_end + 1)
            if nxt_end < 0:
                nxt_end = len(block)
            nxt_line = _norm_space(block[line_end+1:nxt_end])
            if nxt_line and not (TABLE_HEADER_RE.search(nxt_line)
                                or TOTAL_ROW_RE.search(nxt_line)
                                or SKIP_WORDS.search(nxt_line)):
                desc = _norm_space(desc + " " + nxt_line)

        raw = desc[:160]

        if not raw:
            continue
        is_total_line = looks_like_total(raw)
        if SKIP_WORDS.search(raw) and not is_total_line:  
            continue

        src = "summary_total" if is_total_line else "summary"

        items.append(LineItem(
            file="", page=this_page, date=date_str, description=raw[:160],
            currency=currency, amount=amount, source=src
        ))

    return items

def normalize_for_amounts(s: str) -> str:
    s = re.sub(r"[\u00A0\u2007\u202F]", " ", s)
    s = re.sub(r"(S|US|NT|HK|A)\s*\$\s*(\d)", r"\1$\2", s, flags=re.I)
    s = re.sub(r"([€£¥$])\s+(\d)", r"\1\2", s)
    s = re.sub(r"\b(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB)\s+(\d)", r"\1 \2", s, flags=re.I)
    s = re.sub(r"(?<=\d)O(?=\d)", "0", s)
    s = re.sub(r"(?<=\d)[lI](?=\d)", "1", s)
    s = re.sub(r"(?<=\d)S(?=\d)", "5", s)
    s = s.replace("\u2019", "'")
    s = re.sub(r"(?<=\d)'\s*(?=\d{3}\b)", " ", s)

    s = join_broken_cents(s)
    return s

def _extract_dual_currency_amount_due(block: str, page1: int, fallback_date: str) -> list[LineItem]:
    out = []
    for i, raw in enumerate(block.splitlines()):
        m = AMOUNT_DUE_DUAL.search(raw)
        if not m:
            # try joining with next line (line break in the middle)
            nxt = block.splitlines()[i+1] if i+1 < len(block.splitlines()) else ""
            m = AMOUNT_DUE_DUAL.search(raw + " " + nxt)
        if m:
            amt_sgd = _norm_amount(m.group(2))
            if _nonzero(amt_sgd):
                out.append(LineItem("", page1, fallback_date, raw[:160], "SGD", amt_sgd, "kv_total"))
    return out

def _extract_layout_table(pdf_path: Path, page_index: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    if not pdf_path or not HAVE_PYMUPDF:
        return []

    MIN_RIGHT_MARGIN = 12.0
    MAX_RIGHT_MARGIN = 28.0
    MAX_JOIN_GAP_Y   = 20.0   # vertical gap to allow joining lines
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
    if page_index < 1 or page_index > doc.page_count:
        print(
            f"[warn] _extract_layout_table: page_index {page_index} "
            f"out of range for {pdf_path} (page_count={doc.page_count})"
        )
        doc.close()
        return []
    page = doc[page_index - 1]
    words = page.get_text("words")

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

    gaps_before_amt: list[float] = []

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
            continue

        desc_right = x_amt - right_margin
        desc = _desc_text_from_row(row, desc_right)
        if desc:
            toks = re.findall(r"[A-Za-z0-9._-]+", desc)
            tail = []
            for t in reversed(toks):
                cand = " ".join([t] + tail)
                if len(cand) > 90 or len(tail) >= 16:
                    break
                tail.insert(0, t)
            if tail:
                desc = " ".join(tail)

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

        is_total = looks_like_total(text) or looks_like_total(desc)
        src = "layout_total" if is_total else "layout"
        items.append(LineItem("", page_index, fallback_date, desc[:160], page_currency, v_amt, src))

    return items

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
        if pick is None and looks_like_total(line):
            if cands:
                pick = max(cands, key=lambda c:abs(c[0]))
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
    """
    Ragged text-table extractor (general).
    - Picks rightmost money on a line, builds a compact left description.
    - Detects 2-line totals (label on line i, amount on line i+1).
    - Emits 'echo' amounts found immediately around a header/row (Allocations/Percentage), tagged via source.
    - Emits allocation parts when found right after a row, tagged via source.
    """
    items: list[LineItem] = []

    # work with normalized lines, but keep raw too for small lookbacks
    raw_lines = re.split(r"\r?\n|\f", block)
    lines = [normalize_for_amounts(l) for l in raw_lines]

    def _good_desc(txt: str) -> bool:
        return len([t for t in _desc_tokens(txt)]) >= 2

    def _add_item(desc: str, amt: float, cur: str, kind: str):
        if not _nonzero(amt) or not desc:
            return
        src = "text_row_total" if kind == "total" else ("text_row|echo" if kind == "echo" else ("text_row|alloc_part" if kind=="alloc" else "text_row"))
        items.append(LineItem("", page1, fallback_date, desc[:160], cur, float(amt), src))

    def _compact(left_line: str, start: int) -> str:
        return _compact_desc_from_line(
            left_line, start,
            min_tokens=3, max_tokens=12,
            min_chars=18, max_chars=64
        )

    def _context_desc(i: int, left_now: str) -> str:
        # Grow description from neighbors until it looks decent.
        parts = [_norm_space(left_now)] if left_now else []
        if not _good_desc(" ".join(parts)):
            k = i - 1
            while k >= 0 and len(parts) < 3 and not _good_desc(" ".join(parts)):
                prev = _norm_space(lines[k])
                if prev and not (TABLE_HEADER_RE.search(prev) or TOTAL_ROW_RE.search(prev) or SKIP_WORDS.search(prev)):
                    parts.insert(0, prev)
                else:
                    break
                k -= 1
        if not _good_desc(" ".join(parts)):
            k = i + 1
            while k < len(lines) and len(parts) < 4 and not _good_desc(" ".join(parts)):
                nxt = _norm_space(lines[k])
                if nxt and not (TABLE_HEADER_RE.search(nxt) or TOTAL_ROW_RE.search(nxt) or SKIP_WORDS.search(nxt)):
                    parts.append(nxt)
                else:
                    break
                k += 1
        return _norm_space(" ".join(p for p in parts if p))

    def _nearby_has_alloc(i: int) -> bool:
        # light check for an Allocations block very close
        wnd = raw_lines[i:i+6]
        big = " ".join(wnd).lower()
        return ("allocation" in big) or ("percentage" in big)

    def _collect_alloc_parts(i: int, parent_amt: float) -> list[tuple[str, float, str]]:
        """
        Look ahead a handful of lines for small 'allocation' amounts (and possible WBS/company context).
        Returns list of (desc, amount, currency). Also detects 'echo' (amount == parent).
        """
        parts = []
        cur = page_currency
        # harvest small context tags to decorate part descriptions
        ctx_tags = []
        for k in range(i+1, min(i+10, len(lines))):
            t = lines[k].strip()
            if not t:
                break
            # try to capture WBS / Cost Center / Company labels
            if re.search(r"(?i)\b(cost\s*center/?wbs|wbs|cost\s*center|company|cost\s*object\s*type)\b", t):
                ctx_tags.append(_norm_space(t))
            # amounts on the right of these lines
            cands = _amount_candidates_in_text(t, page_currency)
            if not cands:
                continue
            # choose the best per line; if label suggests GST/VAT/tax skip
            if SKIP_WORDS.search(t):
                continue
            pick = _pick_amount_by_context(t, cands, page_currency) or max(cands, key=lambda c: abs(c[0]))
            amt, cur, _, _ = pick
            if not _nonzero(amt):
                continue
            # treat exact repeat of parent as echo (UI duplication)
            if abs(float(amt) - float(parent_amt)) <= 0.005:
                parts.append(("(echo) " + _norm_space(t), float(amt), cur, "echo"))
            else:
                # decorate part with last seen context tags (WBS/Company etc.)
                ctx = ""
                if ctx_tags:
                    # keep the last two context lines to keep it short
                    ctx = " | ".join(ctx_tags[-2:])
                desc = _norm_space(ctx) if ctx else _compact(t, 0) or "Allocation part"
                parts.append((desc, float(amt), cur, "alloc"))
        return parts

    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue

        # 0) two-line total: "Total ..." on this line, amount on next line
        if TOTAL_WORDS.search(line) and (i + 1) < len(lines):
            nxt = lines[i+1]
            cands_n = _amount_candidates_in_text(nxt, page_currency)
            if cands_n:
                pick_n = _pick_amount(cands_n, page_currency) or max(cands_n, key=lambda c: abs(c[0]))
                amt_n, cur_n, *_ = pick_n
                if _nonzero(amt_n):
                    left = _compact(line, len(line))  # all text is to the left
                    desc = _context_desc(i, left or line)
                    _add_item(desc, amt_n, cur_n, kind="total")
                    i += 2
                    continue  # processed the pair

        # 1) regular row with money on this line
        cands = _amount_candidates_in_text(line, page_currency)
        pick = None
        if cands:
            pick = _pick_amount_by_context(line, cands, page_currency)
            if pick is None and looks_like_total(line):
                pick = max(cands, key=lambda c: abs(c[0]))

        if not pick:
            i += 1
            continue

        amount, currency, start, _ = pick
        if not _nonzero(amount):
            i += 1
            continue

        # require some currency hint unless total-ish
        is_total_row = looks_like_total(line)
        if not is_total_row and not re.search(r"(SGD|USD|EUR|GBP|JPY|MYR|HKD|TWD|AUD|NZD|CAD|CNY|RMB|US\$|S\$|A\$|HK\$|NT\$|\$|€|£|¥)", line, re.I):
            i += 1
            continue

        # build a compact description with neighbor context
        left = _compact(line, start)
        desc = _context_desc(i, left)
        if not _good_desc(desc):
            i += 1
            continue

        _add_item(desc, float(amount), currency, kind=("total" if (is_total_row or looks_like_total(desc)) else "row"))

        # 2) optional: capture near "Allocations/Percentage" echoes & parts
        if _nearby_has_alloc(i):
            parts = _collect_alloc_parts(i, float(amount))
            # keep echoes but tag; keep alloc parts as proper rows
            for p_desc, p_amt, p_cur, p_kind in parts:
                _add_item(f"{desc} — {p_desc}", p_amt, p_cur, kind=p_kind)

        i += 1

    return items

def _extract_keyword_pairs(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    items = []
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    seen = set()

    for i, line in enumerate(lines):
        prev = lines[i-1] if i > 0 else ""
        nxt  = lines[i+1] if i+1 < len(lines) else ""
        # look for totals in a small window
        has_total = (looks_like_total(line)
                     or looks_like_total(f"{prev} {line}")
                     or looks_like_total(f"{line} {nxt}"))

        has_item  = bool(ITEM_LABELS.search(line)) and not has_total
        if not (has_total or has_item):
            continue

        # amounts can be on this line or the next line
        cands = _amount_candidates_in_text(line, page_currency)
        if not cands and nxt:
            cands = _amount_candidates_in_text(f"{line} {nxt}", page_currency)
        if not cands and prev:
            cands = _amount_candidates_in_text(f"{prev} {line}", page_currency)
        if not cands:
            continue

        # prefer the SGD figure when a dual-currency total is present
        pick = _pick_amount_by_context(f"{prev} {line} {nxt}", cands, page_currency) if has_total else _pick_amount(cands, page_currency)
        if not pick:
            continue
        amount, currency, _, _ = pick
        if not _nonzero(amount):
            continue

        kind = "total" if has_total else "item"
        # dedup within this extractor only
        dedup_key = (round(amount, 2), kind, TOTAL_WORDS.sub("", line.lower()), i)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        src = "kv_total" if has_total else "kv_item"
        items.append(LineItem("", page1, fallback_date, line[:160], currency, amount, src))
    return items

def _extract_header_table(block: str, page1: int, fallback_date: str, page_currency: str) -> list[LineItem]:
    items: list[LineItem] = []
    lines = [normalize_for_amounts(l) for l in block.splitlines()] 
    for i, line in enumerate(lines):
        if not TABLE_HEADER_RE.search(line):
            continue
        for raw in lines[i+1:i+80]:
            row = _norm_space(raw)
            if not row: 
                break
            is_total = bool(TOTAL_ROW_RE.search(row) or looks_like_total(row))
            cols = COL_SPLIT.split(row)
            if len(cols) < 2:
                continue
            desc = cols[0].strip(" -:|•\u2022")
            amt = None
            # scan the non-description cells right-to-left, but score by context
            best = None
            for part in reversed(cols[1:]):
                cands = _amount_candidates_in_text(part, page_currency)
                if not cands:
                    continue
                cand = _pick_amount_by_context(part, cands, page_currency)
                # fallback: if it's a total-ish row and context didn't pick, keep the largest amount
                if cand is None and (is_total or looks_like_total(part)):
                    cand = max(cands, key=lambda c: abs(c[0]))

                if best is None:
                    best = cand
                else:
                    # keep the one with the better context score (inline scorer)
                    try:
                        def _ctx(line, c):
                            start = c[2]
                            left  = line[max(0, start - 56): start]
                            score = 0.0
                            if INCL_HINT.search(left):       score += 50
                            if EXCL_HINT.search(left):       score += 10
                            if GST_PAYABLE_HINT.search(left):score -= 30
                            if c[1] == page_currency:        score += 2
                            score += start * 1e-3
                            return score
                        if _ctx(part, cand) > _ctx(part, best):
                            best = cand
                    except Exception:
                        pass
            if best:
                amt = float(best[0])

            if desc and (amt is not None):
                src = "table_total" if is_total else "table"
                items.append(LineItem("", page1, fallback_date, desc[:160], page_currency, amt, src))
                if is_total:
                    break
    return items

def parse_amount_loose(tok: str) -> float | None:
    s = tok.strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")

    s = s.replace("O","0").replace("o","0").replace("l","1").replace("I","1").replace("S","5")
    s = s.replace(",", " ")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"(\d)[\.\-]$", r"\1", s)
    try:
        val = float(s)
        return -val if neg else val
    except Exception:
        return None

def _nonzero(a: float) -> bool:
    try: return abs(float(a)) > ZERO_EPS
    except: return False

def _norm_amount(s: str) -> float:
    raw = re.sub(r"[,\s]", "", str(s))
    neg = raw.startswith("(") and raw.endswith(")")
    raw = raw.strip("()")
    raw = re.sub(r"[^0-9.\-]", "", raw)
    val = parse_amount_loose(raw)
    if val is None:
        # last-resort: old behaviour (avoids crashes)
        raw2 = re.sub(r"[,\s]", "", raw)
        raw2 = re.sub(r"[^0-9.\-]", "", raw2)
        val = float(raw2) if raw2 else 0.0
    return -float(val) if neg else float(val)

def _resolve_currency_from_token(token: str, page_currency: str) -> str:
    m = re.search(r"(US\$|S\$|A\$|HK\$|NT\$|C\$|RM|NTD|€|£|¥|\$|\b[A-Z]{2,3}\b)", token)
    if not m:
        return page_currency
    key = m.group(1)
    if key == "$":
        return page_currency
    if key in CURRENCY_MAP:
        return CURRENCY_MAP[key]
    up = key.upper()
    return up if up in ISO_SET else page_currency

def _compact_desc_from_line(s: str, amt_start: int,
                            min_tokens: int = 3,   # require at least this many tokens
                            max_tokens: int = 16,  # never exceed this many
                            min_chars: int = 22,   # require at least this many characters
                            max_chars: int = 90) -> str:
    """
    Return a concise, generic description taken from the left side of the amount.
    Grows from the end (closest to the amount) until min_tokens/min_chars are met,
    but never exceeds max_tokens/max_chars. Strips a leading inline date.
    """
    left_full = s[:amt_start]

    # Strip a leading inline date at the start of the line
    mdate = DATE_INLINE_RE.search(left_full)
    if mdate and mdate.start() <= 4:
        left_full = left_full[mdate.end():]

    left_full = _norm_space(left_full.strip(" -:|•\u2022"))
    if not left_full:
        return ""

    # Tokenize and drop generic noise
    toks = re.findall(r"[A-Za-z0-9._-]+", left_full)
    drop = {
        "sgd","usd","eur","gbp","jpy","myr","hkd","twd","aud","nzd","cad","cny","rmb",
        "amount","total","subtotal","balance","due","payable","incl","including",
        "excl","excluding","gst","vat","tax"
    }
    toks = [t for t in toks if t.lower() not in drop]

    # Build adaptively from the end (closest to amount)
    picked: list[str] = []
    for t in reversed(toks):
        # try adding this token to the front
        cand = " ".join([t] + picked)
        if len(cand) > max_chars or len(picked) >= max_tokens:
            break
        picked.insert(0, t)
        if len(picked) >= min_tokens and len(cand) >= min_chars:
            break

    # Fail-safe: if still short, keep adding older tokens while under limits
    i = len(toks) - len(picked) - 1
    while (len(picked) < min_tokens or len(" ".join(picked)) < min_chars) and i >= 0:
        cand = " ".join([toks[i]] + picked)
        if len(cand) <= max_chars and len(picked) < max_tokens:
            picked.insert(0, toks[i])
        i -= 1

    return " ".join(picked)

def _norm_space(s: str) -> str:
    s = s.replace("\f", " ")
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"-\s+", "-", s)
    return re.sub(r"\s+", " ", s).strip()

def looks_like_total(text: str) -> bool:
    t = str(text or "")
    return bool(
        TOTAL_WORDS.search(t) or
        TOTAL_WORDS_LOOSE.search(t) or
        GST_TOTAL_HINT.search(t) or
        AMOUNT_DUE_RE.search(t)
    )

def _amount_candidates_in_text(line: str, page_currency: str):
    """
    Return list of (amount:float, currency:str, start:int, end:int).
    Honors currency within token, or immediately after/before (incl. '(SGD)').
    """
    line = normalize_for_amounts(line)
    cands = []
    for m in AMOUNT_TOKEN.finditer(line):
        token = m.group(0)
        amt = parse_amount_loose(token)
        if amt is None:
            mnum = AMT_NUM_IN_TOKEN.search(token)
            if not mnum:
                continue
            amt = parse_amount_loose(mnum.group("num"))
            if amt is None:
                continue

        has_currency = bool(re.search(r"(US\$|S\$|A\$|HK\$|NT\$|C\$|RM|NTD|€|£|¥|\$|\b[A-Z]{2,3}\b)", token))
        has_cents    = bool(re.search(r"[.,]\d{2}\)?\s*$", token))
        if not (has_currency or has_cents):
            continue

        cur = _resolve_currency_from_token(token, page_currency)

        # look a bit to the right for ISO (SGD) or SGD
        tail = line[m.end(): m.end() + 24]
        t2 = CUR_AFTER_RE.search(tail) or CUR_PAREN_AFTER_RE.search(tail)
        if t2:
            iso = t2.group(1).upper()
            if iso in ISO_SET:
                cur = iso
        else:
            # also allow just before the number e.g. "SGD: 123.45"
            head = line[max(0, m.start()-24): m.start()]
            t3 = CUR_BEFORE_RE.search(head)
            if t3:
                iso = t3.group(1).upper()
                if iso in ISO_SET:
                    cur = iso

        cands.append((float(amt), cur, m.start(), m.end()))
    return cands

def _pick_amount_by_context(line: str, cands, prefer_currency: str):
    """
    Choose an amount using local label context on the same line.
    Strongly prefer values whose left-context mentions 'including GST';
    mildly prefer 'excluding' over 'gst payable'; down-weight 'gst payable'.
    Fallback bias is slightly to the right to preserve your old behavior.
    """
    if not cands:
        return None

    def _score(c):
        start = c[2]  # start index of the match in the line
        left  = line[max(0, start - 56): start]  # small left-context window
        score = 0.0
        if INCL_HINT.search(left):       score += 50
        if EXCL_HINT.search(left):       score += 10
        if GST_PAYABLE_HINT.search(left):score -= 30
        if SKIP_WORDS.search(left):      score -= 15
        if c[1] == prefer_currency:      score += 2
        score += start * 1e-3            # slight rightward tie-break
        return score

    return max(cands, key=_score)

def _pick_amount(cands, prefer_currency: str):
    """Prefer amounts with prefer_currency; tie-break by rightmost start; else rightmost overall."""
    if not cands:
        return None
    pref = [c for c in cands if c[1] == prefer_currency]
    pool = pref if pref else cands
    return max(pool, key=lambda c: c[2])

def _desc_tokens(text: str) -> list[str]:
    return [t for t in _norm_desc_text(text).split() if t and t not in STOPWORDS]

def _norm_desc_text(text: str) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text)

    s = s.lower()
    s = re.sub(r"[-/]", " ", s)           # replace dash/slash with space
    s = re.sub(r"[^a-z0-9 ]+", " ", s)    # keep only a-z, 0-9 and spaces
    s = re.sub(r"\s+", " ", s).strip()    # collapse spaces
    return s

def extract_receipt_lines_from_block(block: str) -> list[dict]:
    block = normalize_debug_wrapping(block)
    rows = []
    rows.extend(extract_stacked_receipts(block))
    for raw in block.splitlines():
        if not raw.strip():
            continue
        if raw.lstrip().startswith("--- Page"):
            continue

        line = raw.rstrip()

        # 1) Strict fixed-width (your original)
        m = ROW_RX.search(line)
        if m:
            desc_raw = _clean_dump_line_for_desc(m.group("desc"))
            amt = parse_amount_loose(m.group("amount"))
            if amt is not None:
                rows.append({"description": desc_raw, "amount": round(float(amt), 2)})
            continue

        # 2) Fallback: single-space rows like "… SGD 14/03/2025 … 1181.59"
        m2 = re.search(
            rf"{CUR_ISO}\s+{DATE_RX}\s+(?P<desc>.+?)\s+(?P<amount>{AMT_TAIL})$",
            line, flags=re.I
        )
        if m2:
            desc_raw = _clean_dump_line_for_desc(m2.group("desc"))
            amt = parse_amount_loose(m2.group("amount"))
            if amt is not None and desc_raw:
                rows.append({"description": desc_raw, "amount": round(float(amt), 2)})
            continue

        # 3) (optional) ultra-light fallback: rightmost amount with a sane mid chunk
        #    only if the line contains a currency+date somewhere
        if re.search(rf"{CUR_ISO}\s+{DATE_RX}", line, flags=re.I):
            # rightmost amount
            m_amt = re.search(r"(\d{1,3}(?:[ ,]\d{3})*[\.,]\d{2})", line)
            if m_amt:
                amt = parse_amount_loose(m_amt.group(1))
                if amt is not None:
                    # take text between date and amount as description
                    m_date = re.search(rf"{CUR_ISO}\s+{DATE_RX}\s+", line, flags=re.I)
                    desc_raw = _clean_dump_line_for_desc(line[m_date.end(): m_amt.start()])
                    if desc_raw:
                        rows.append({"description": desc_raw, "amount": round(float(amt), 2)})
        
        if AMOUNT_ONLY_LINE.match(line):
            amt = parse_amount_loose(line)
            if amt is not None:
                rows.append({"description": "(no desc)", "amount": round(float(amt), 2)})
                continue

    return rows

def extract_stacked_receipts(block: str):
    lines = [l.strip() for l in block.splitlines() if l.strip()]
    out = []
    i = 0
    while i < len(lines):
        if DATE_LINE_RX.match(lines[i]):
            date = lines[i]
            desc_parts = []
            amt = None
            j = i + 1
            while j < len(lines):
                if CUR_ONLY_RE.match(lines[j]) and j+1 < len(lines) and AMOUNT_ONLY_RE.match(lines[j+1]):
                    amt = parse_amount_loose(lines[j+1])
                    if amt is not None:
                        amt = float(amt)
                    break
                if TABLE_HEADER_RE.search(lines[j]) or TOTAL_ROW_RE.search(lines[j]):
                    break
                desc_parts.append(lines[j])
                j += 1

            if amt is not None:
                desc = _norm_space(" ".join(desc_parts))
                if desc:
                    out.append({"date": date, "description": desc, "amount": round(amt, 2)})
            i = (j + 2) if (amt is not None) else (i + 1)
        else:
            i += 1
    return out

def _clean_dump_line_for_desc(s: str) -> str:
    # keep words but normalize whitespace
    s = s.replace("\u2019", "'")
    s = re.sub(r"[\u00A0\u2007\u202F]", " ", s)
    return re.sub(r"[ \t]+", " ", s).strip()

def detect_page_currency(text: str, default="SGD") -> str:
    for iso in ("SGD","USD","EUR","GBP","JPY","MYR","HKD","TWD","AUD","NZD","CAD","CNY","RMB"):
        if re.search(rf"\b{iso}\b", text, flags=re.I):
            return iso
    m = re.search(r"(US\$|S\$|A\$|HK\$|NT\$|€|£|¥|\$)", text, flags=re.I)
    return CURRENCY_MAP.get(m.group(1), default) if m else default

def _normalize_date(value: str) -> Optional[str]:
    value = value.strip()
    for f in ("%d/%m/%Y","%Y-%m-%d","%d %b %Y","%d %B %Y","%d/%m/%y"):
        try:
            return datetime.strptime(value, f).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

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

def clean_file_stem(stem: str) -> str:
    s = stem.strip()
    s = COPY_SUFFIX_RE.sub("", s)
    s = VERSION_SUFFIX_RE.sub("", s)
    s = COPY_ORIG_RE.sub("", s)
    return s.strip(" _-")

def norm_docno(s: str) -> str:
    """Normalize a document id from a filename-like string (drop copy/version suffixes)."""
    s = clean_file_stem(str(s))
    return re.sub(r"[^A-Z0-9]", "", s.upper())

def extract_pdf_lineitems(
    pdfs,
    args,
    common_dbg_root: Path,
    run_textdump_path: Path,
) -> pd.DataFrame:
    """
    Extract LineItem rows from all PDFs and return as a DataFrame.
    This is basically your old 'PDF loop' from main(), refactored.
    """    
    extracted: List[LineItem] = []
    for pdf in pdfs:
        # --- Get pages (debug -> fallback to live) ---
        pages = None
        page_source = "live"

        if args.use_debug_pages:
            pages = read_debug_pages_for_pdf(pdf, debug_root=common_dbg_root)
            if pages:
                pages = [normalize_debug_wrapping(p) for p in pages]
                page_source = "debug"

        if not pages:
            # live extraction (either not using debug, or no debug files found)
            pages, _ = extract_text_pages(pdf, ocr_if_empty=True, ocr_lang="eng", dump_root=common_dbg_root)
            if not pages:
                continue

        # --- Always write a single readable textdump (for both modes) ---
        textdump = "\n\n".join([f"--- Page {i+1} ---\n{p}" for i, p in enumerate(pages)])
        run_textdump_path.write_text(textdump, encoding="utf-8")

        block = "\f".join(pages)
        p1, pN = 1, len(pages)

        # --- Compute docno for this PDF (MUST be before we add rows that use it) ---
        stem = pdf.stem
        if args.docno_mode == "segment":
            docno = extract_doc_id(stem, mode="segment", segment_index=args.docno_segment_index)
        elif args.docno_mode == "regex":
            pat = args.docno_regex or r"(\d{8,})"
            docno = extract_doc_id(stem, mode=f"regex:{pat}:1")
        else:
            docno = extract_doc_id(stem, mode=args.docno_mode)
        if not docno:
            docno = norm_docno(stem)

        # --- Parse rows from the block ---
        pdf_for_layout = pdf if HAVE_PYMUPDF else None  
        rows = _extract_rows_from_block(block, p1, pN, pdf_path=pdf_for_layout)
        rec_lines = extract_receipt_lines_from_block(block)

        if rec_lines:
            page_currency = detect_page_currency(block, "SGD")
            date_regex = re.compile("|".join(DATE_PATTERNS), re.I)
            dates = [(_m.start(), _m.end(), _normalize_date(_m.group(0)) or "")
                    for _m in date_regex.finditer(block)]
            fallback_date = dates[-1][2] if dates else ""
            for r in rec_lines:
                li = LineItem(
                    file=pdf.name,
                    page=p1,
                    date=fallback_date,
                    description=r["description"][:160],
                    currency=page_currency,
                    amount=float(r["amount"]),
                    source="receipt_row|live" if page_source == "live" else "receipt_row|debug",
                    docno=docno,
                )
                rows.append(li)

        # de-dup & final tagging
        seen, clean = set(), []
        for li in rows:
            if "alloc_part" in li.source.lower():
                continue
            key = (round(li.amount, 2), re.sub(r"\s+", " ", li.description.lower()))
            if key in seen:
                continue
            seen.add(key)
            li.file, li.docno, li.source = pdf.name, docno, f"{li.source}|{page_source}"
            clean.append(li)
        extracted.extend(clean)


    df = pd.DataFrame([asdict(x) for x in extracted],
                  columns=["file","page","date","description","currency","amount","source","docno"])
    
    if df.empty:
        return df
    
    # --- helper columns needed by echo synthesis ---
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["_is_echo"] = df["source"].str.contains(r"\|echo\b", case=False, na=False)
    df["_is_alloc_part"] = df["source"].str.contains(r"alloc_part", case=False, na=False)
    df["_amt_2dp"] = df["amount"].round(2)
    # a light-normalized description key for grouping
    df["_desc_key"] = (
        df["description"].astype(str).str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"[^a-z0-9 ]", "", regex=True)
        .str.replace(r"\b(sgd|usd|cash|card|allocations?)\b", "", regex=True)
        .str.strip()
    )

    # --- synthesize echoes: count echo rows and duplicate primaries that many times ---
    to_add = []
    echo_counts = (
        df[df["_is_echo"] & ~df["_is_alloc_part"]]
        .groupby(["docno","currency","_amt_2dp","_desc_key"])
        .size()
    )  # Series with MultiIndex

    primaries = df[~df["_is_echo"] & ~df["_is_alloc_part"]]
    for _, r in primaries.iterrows():
        key = (r["docno"], r["currency"], r["_amt_2dp"], r["_desc_key"])
        m = int(echo_counts.get(key, 0))   # 0 when the key is absent
        if m:
            base = r.drop(labels=["_is_echo","_is_alloc_part","_amt_2dp","_desc_key"]).to_dict()
            for _ in range(m):
                to_add.append({**base, "source": str(r["source"]) + "|echo_synth"})

    if to_add:
        df = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True)

    # --- now continue with your existing cleaning/sorting/period tagging ---
    df = df[df["amount"].abs() > ZERO_EPS].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.sort_values(["file","page","date"], inplace=True, kind="stable")

    # net-zero marking (unchanged)
    def _mark_net_zero_pairs(dfx: pd.DataFrame) -> pd.Series:
        dfx["_desc_norm"] = dfx["description"].str.lower().str.replace(r"\s+", " ", regex=True)
        net_zero = pd.Series(False, index=dfx.index)
        for _, g in dfx.groupby(["docno","currency","_desc_norm"], group_keys=False):
            if len(g) >= 2 and abs(round(g["amount"].sum(), 2)) < 0.01:
                net_zero.loc[g.index] = True
        return net_zero
    df["net_zero_pair"] = _mark_net_zero_pairs(df)
    df.drop(columns=["_desc_norm"], errors="ignore", inplace=True)

    return df