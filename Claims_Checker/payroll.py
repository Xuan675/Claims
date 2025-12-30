# claims_checker/payroll.py
from __future__ import annotations
from pathlib import Path
from io import BytesIO
import re
import pandas as pd
from typing import Optional, Set
import extract_msg

try:
    import msoffcrypto
    HAVE_MSOFFCRYPTO = True
except Exception:
    msoffcrypto = None
    HAVE_MSOFFCRYPTO = False

def read_payroll_excel(path: str | Path, password: str | None = None) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        print(f"[ERR] Payroll file does not exist: {p}")
        return pd.DataFrame()

    print(f"[DBG] File: {p}  Ext: {p.suffix}  HAVE_MSOFFCRYPTO={HAVE_MSOFFCRYPTO}")
    # 1) decrypt path if possible
    if password and HAVE_MSOFFCRYPTO:
        try:
            with open(p, "rb") as f:
                office = msoffcrypto.OfficeFile(f)
                office.load_key(password=password)
                bio = BytesIO()
                office.decrypt(bio)
                bio.seek(0)
                print("[DBG] Decrypt OK → reading in-memory stream")
                return _read_all_sheets_into_df(bio, suffix=p.suffix)
        except Exception as e:
            print(f"[WARN] Decrypt read failed: {e!r}")

    # 2) plain read
    try:    
        print("[DBG] Plain read (no decrypt)")
        return _read_all_sheets_into_df(p, suffix=p.suffix)
    except Exception as e:
        print(f"[WARN] Plain read failed: {e!r}")
        return pd.DataFrame()

def read_payroll_with_msg(xlsx_path: str | Path, msg_path: str | Path) -> pd.DataFrame:
    """
    Attempt to read a password from a message file (txt/md) next to the payroll workbook,
    then load the payroll Excel with that password.
    If no password is found, falls back to plain read.
    """
    password = _extract_password_from_msg(msg_path)
    return read_payroll_excel(xlsx_path, password=password)

def _read_all_sheets_into_df(handle, suffix: str | None = None) -> pd.DataFrame:
    eng = None
    if suffix:
        ext = suffix.lower()
        if ext in (".xlsx", ".xlsm"):
            eng = "openpyxl"
        elif ext == ".xls":
            eng = "xlrd"
        elif ext == ".xlsb":
            eng = "pyxlsb"

    print(f"[DBG] Using engine={eng}")
    xl = pd.ExcelFile(handle, engine=eng)
    print(f"[DBG] Sheets: {xl.sheet_names}")
    frames = []

    # robust: parse ALL sheets via read_excel dict, more tolerant than xl.parse loop
    sheets = pd.read_excel(handle, sheet_name=None, dtype=object, engine=eng)
    for sh, df in sheets.items():
        try:
            print(f"[DBG] {sh}: raw shape={df.shape}")
            if df is None:
                continue
            # drop all-empty rows and columns
            df = df.dropna(how="all")
            if df.empty:
                print(f"[DBG] {sh}: empty after dropna(rows)")
                continue
            df = df.loc[:, df.notna().any(axis=0)]
            if df.empty:
                print(f"[DBG] {sh}: empty after dropna(cols)")
                continue
            df["_sheet"] = sh
            print(f"[DBG] {sh}: kept shape={df.shape}")
            frames.append(df)
        except Exception as e:
            print(f"[WARN] Could not parse sheet {sh}: {e!r}")

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"[DBG] Concatenated rows: {len(out)}")
    return out

def _extract_password_from_msg(msg_path: str | Path) -> str | None:
    p = Path(msg_path)
    if not p.exists():
        print("[WARN] Message file not found.")
        return None

    text = ""
    if p.suffix.lower() == ".msg":
        try:
            msg = extract_msg.Message(str(p))
            msg_message = ""
            # Safely combine subject + body (handle possible encoding)
            if hasattr(msg, "subject") and msg.subject:
                msg_message += msg.subject + "\n"
            if hasattr(msg, "body") and msg.body:
                msg_message += msg.body
            elif hasattr(msg, "messageBody"):
                msg_message += msg.messageBody
            text = msg_message.strip()
            msg.close()
        except Exception as e:
            print(f"[WARN] Could not parse .msg: {e}")
            return None
    else:
        # Fallback for text files
        text = p.read_text(encoding="utf-8", errors="ignore")

    # Common password patterns
    patterns = [
        r"pw\s*(?:for [^:]+)?:\s*([^\s\"']+)",
        r"password\s*[:=]\s*([^\s\"']+)",
        r"\bpass(?:word)?\s+is\s+\"?([^\s\"']+)\"?",
        r"\bpwd\s*[:=]\s*([^\s\"']+)",
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            cand = m.group(1).strip()
            if cand.endswith("."):
                cand = cand[:-1]
            if len(cand) >= 4 and " " not in cand:
                print(f"[INFO] Extracted password: {cand}")
                return cand

    print("[WARN] No password found in .msg file.")
    return None

_EOM_KEYWORDS = [
    "eom", "end of month", "month end", "payroll", "salary", "salaries", "cpf", "net pay"
]

def extract_claims_eom(book_df: pd.DataFrame, date_col: str, amount_col: str) -> pd.DataFrame:
    """
    Extract likely 'EOM' rows from the claims book based on description keywords.
    Returns a slice (view) of the original claims DataFrame (preserving indices),
    with numeric Amount and parsed date column.
    """
    if book_df is None or book_df.empty:
        return pd.DataFrame()

    df = book_df.copy()
    if amount_col not in df.columns:
        return pd.DataFrame()

    # Normalize columns
    desc_col = None
    for c in df.columns:
        if str(c).strip().lower() == "description":
            desc_col = c
            break

    # 2) Fallback: 'Budget Category'
    if desc_col is None:
        for c in df.columns:
            if str(c).strip().lower() == "budget category":
                desc_col = c
                break
    if desc_col is None:
        # best effort: pick a column that contains 'desc'
        for c in df.columns:
            if "desc" in str(c).lower():
                desc_col = c
                break
    if desc_col is None:
        return pd.DataFrame()

    # Numeric amount
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce")

    # Parse date if available
    if date_col in df.columns:
        df["_eom_date"] = pd.to_datetime(df[date_col], errors="coerce")
    else:
        df["_eom_date"] = pd.NaT

    # Keyword filter on description
    def _has_kw(x: str) -> bool:
        t = str(x or "").lower()
        return any(kw in t for kw in _EOM_KEYWORDS)

    mask = df[desc_col].map(_has_kw)
    eom = df[mask].copy()

    # Keep only rows with a real amount
    eom = eom[eom[amount_col].notna()].copy()

    # Convenience renames for downstream reconciliation
    eom = eom.rename(columns={amount_col: "_eom_amount"})
    eom["_eom_desc_col"] = desc_col  # keep which column we used
    return eom

def multi_match(cand: pd.DataFrame, target: float, amt_col: str, tol_abs: float) -> Optional[list[int]]:

    if cand.empty:
        return None
    
    diffs = (cand[amt_col] - target).abs()
    if not diffs.empty:
        i = diffs.idxmin()
        if abs(float(cand.at[i, amt_col]) - target) <= tol_abs:
            return [i]
        
    idx = list(cand.index)
    vals = [float(cand.at[i, amt_col]) for i in idx]

    n = len(idx)
    for a in range(n):
        va = vals[a]
        for b in range(a+1, n):
            s = va + vals[b]
            if abs(s - target) <= tol_abs:
                return[idx[a], idx[b]]

def reconcile_eom_line_then_total(
    claims_eom: pd.DataFrame,
    payroll_df: pd.DataFrame,
    start_dt,
    end_dt,
    amount_col_candidates: tuple[str, ...] = (
        "Net Pay", "Net", "Net Amount", "Net amount", "NetPay", "Amount", "Pay Amount"
    ),
    date_col_candidates: tuple[str, ...] = (
        "Payment Date", "Pay Date", "Date", "Txn Date", "Transaction Date"
    ),
    tol_abs: float = 0.01,
) -> dict:
    """
    Try to match each EOM claim amount to a payroll row (exact/near amount match).
    If line matches are incomplete, try totals backstop:
      sum(eom in period) ≈ sum(payroll in period) within tol_abs.

    Returns:
      {
        "matched_idx": set[int],          # indices in claims_eom (original book index)
        "total_ok_idx": set[int],         # indices covered by totals backstop
        "diag": DataFrame({ "totals_match":[bool], "claims_total_left":[float], "payroll_total_left":[float] })
      }
    """
    matched_idx: Set[int] = set()
    total_ok_idx: Set[int] = set()

    if claims_eom is None or claims_eom.empty or payroll_df is None or payroll_df.empty:
        return {"matched_idx": matched_idx, "total_ok_idx": total_ok_idx,
                "diag": pd.DataFrame({"totals_match":[True], "claims_total_left":[0.0], "payroll_total_left":[0.0]})}

    # --- Choose amount & date columns in payroll ---
    pay = payroll_df.copy()
    pay_amount_col = _pick_first_present(pay.columns, amount_col_candidates)
    if not pay_amount_col:
        # try generic numeric column named like 'amount'
        pay_amount_col = _pick_first_present(pay.columns, ("amount", "AMOUNT", "Amount"))
        if not pay_amount_col:
            return {"matched_idx": matched_idx, "total_ok_idx": total_ok_idx,
                    "diag": pd.DataFrame({"totals_match":[False], "claims_total_left":[claims_eom["_eom_amount"].sum()], "payroll_total_left":[0.0]})}

    pay[pay_amount_col] = pd.to_numeric(pay[pay_amount_col], errors="coerce")
    pay = pay[pay[pay_amount_col].notna()].copy()

    pay_date_col = _pick_first_present(pay.columns, date_col_candidates)
    if pay_date_col:
        pay["_pdate"] = pd.to_datetime(pay[pay_date_col], errors="coerce")
    else:
        pay["_pdate"] = pd.NaT

    # --- Filter by period if available ---
    def _in_period(d):
        if pd.isna(d):
            # allow if period is unspecified
            return True if (start_dt is None and end_dt is None) else False
        if start_dt is not None and d < start_dt: return False
        if end_dt   is not None and d > end_dt:   return False
        return True

    if start_dt is not None or end_dt is not None:
        pay = pay[pay["_pdate"].map(_in_period)].copy()

    # --- Line-by-line matching on amount ---
    # Work on a copy; we will mark used payroll rows to avoid double-matching
    # --- Line-by-line matching on amount (with small combo support) ---
    pay["_used"] = False

    for idx, row in claims_eom.iterrows():
        amt = float(row.get("_eom_amount", 0.0) or 0.0)

        # candidate payroll rows not yet used
        cand = pay[~pay["_used"]].copy()
        if cand.empty:
            continue

        # If both sides have dates, prefer same pay date window (same day)
        claim_dt = row.get("_eom_date", pd.NaT)
        if pd.notna(claim_dt) and cand["_pdate"].notna().any():
            same_day = cand[cand["_pdate"].dt.normalize() == pd.to_datetime(claim_dt).normalize()]
            if not same_day.empty:
                cand = same_day

        # 1) single-row closest within tol_abs
        cand["_adiff"] = (cand[pay_amount_col] - amt).abs()
        best = cand.sort_values("_adiff").head(1)
        if not best.empty and best["_adiff"].iat[0] <= tol_abs:
            ridx = best.index[0]
            pay.at[ridx, "_used"] = True
            matched_idx.add(idx)
            continue

        # 2) pairs/triples within tol_abs
        combo = multi_match(cand, amt, pay_amount_col, tol_abs)
        if combo:
            for ridx in combo:
                pay.at[ridx, "_used"] = True
            matched_idx.add(idx)
            continue

    # --- Totals backstop ---
    # Remaining EOM amounts vs remaining payroll amounts
    eom_left = claims_eom.loc[[i for i in claims_eom.index if i not in matched_idx], "_eom_amount"].sum()
    pay_left = pay.loc[~pay["_used"], pay_amount_col].sum()

    totals_match = abs((eom_left or 0.0) - (pay_left or 0.0)) <= tol_abs

    if totals_match:
        # Cover all unmatched EOM rows with totals backstop
        total_ok_idx = set([i for i in claims_eom.index if i not in matched_idx])

    diag = pd.DataFrame({
        "totals_match": [bool(totals_match)],
        "claims_total_left": [float(eom_left or 0.0)],
        "payroll_total_left": [float(pay_left or 0.0)],
    })

    return {
        "matched_idx": matched_idx,
        "total_ok_idx": total_ok_idx,
        "diag": diag
    }

# =========================
# Small helpers
# =========================

def _pick_first_present(columns, candidates: tuple[str, ...]) -> Optional[str]:
    low = {str(c).lower(): c for c in columns}
    for name in candidates:
        if str(name).lower() in low:
            return low[str(name).lower()]
    # also try contains (e.g., "Net Pay (SGD)")
    for name in candidates:
        for c in columns:
            if str(name).lower() in str(c).lower():
                return c
    return None