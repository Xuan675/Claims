#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sys, subprocess
from pathlib import Path

import pandas as pd
from payroll import read_payroll_excel, read_payroll_with_msg
from claims_pipeline import reconcile_claims_and_write_outputs
from pdf_pipeline import extract_pdf_lineitems

def discover_files(root: Path) -> dict:
    """
    Scan a folder tree and guess:
      - claims_xlsx: the claim report (xlsx/csv)
      - payroll_xlsx: the payroll report (xlsx/xls)
      - payroll_msg: .msg that likely carries the message
      - pdfs: list of PDFs
    """
    root = Path(root)

    def _is_tmp(p: Path) -> bool:
        return p.name.startswith("~$")

    def _is_igms_csv(p: Path) -> bool:
        n = p.name.lower()
        return p.suffix.lower() == ".csv" and ("igms" in n or "grant claim report" in n or "claim report" in n)

    def _claim_rank(p: Path) -> tuple:
        n = p.name.lower()
        primary = (
            0 if _is_igms_csv(p) else
            1 if any(k in n for k in ["clarification", "postings", "igms", "wbs", "claim", "report", "fsoa"]) else
            2
        )
        # prefer CSV over XLS* when same primary rank
        secondary = 0 if p.suffix.lower() == ".csv" else 1
        # shorter names a tiny bit earlier (tie-breaker)
        tertiary = len(n)
        return (primary, secondary, tertiary)

    all_files = [p for p in root.rglob("*") if p.is_file() and not _is_tmp(p)]

    # PDFs
    pdfs = [p for p in all_files if p.suffix.lower() == ".pdf"]

    # Claims workbook candidates (csv + xlsx)
    claim_cands = [
        p for p in all_files
        if p.suffix.lower() in {".csv", ".xlsx", ".xlsm", ".xlsb", ".xls"}
    ]
    claim_cands.sort(key=_claim_rank)
    claims_xlsx = claim_cands[0] if claim_cands else None

    # Payroll workbook candidates (xlsx only; if none, you'll fall back to payslip PDFs later)
    payroll_cands = [
        p for p in all_files
        if p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}
        and any(k in p.name.lower() for k in ["schedule", "payroll", "salary", "cpf", "sdf"])
    ]
    # light ranking for payroll
    def _payroll_rank(p: Path) -> tuple:
        n = p.name.lower()
        primary = 0 if "payroll" in n else 1
        secondary = 0 if "posting" in n else 1
        return (primary, secondary, len(n))
    payroll_cands.sort(key=_payroll_rank)
    payroll_xlsx = payroll_cands[0] if payroll_cands else None

    excel_exts = {".xlsx", ".xlsm", ".xls", ".xlsb"}
    all_excels = [p for p in all_files if p.suffix.lower() in excel_exts]

    support_excels: list[Path] = []
    for p in all_excels:
        parents_lower = [parent.name.lower().replace(" ","") for parent in p.parents]
        if any(("support" in name) or name.startswith("sup") for name in parents_lower):
            support_excels.append(p)
    if not support_excels:
        ignore = {claims_xlsx, payroll_xlsx}
        support_excels = [p for p in all_excels if p not in ignore]

    # .msg that likely has a password
    msg_cands = [p for p in all_files if p.suffix.lower() == ".msg"]
    def _msg_rank(p: Path) -> tuple:
        n = p.name.lower()
        return (
            0 if "password" in n or "pwd" in n else 1,
            0 if "payroll" in n else 1,
            len(n),
        )
    msg_cands.sort(key=_msg_rank)
    payroll_msg = msg_cands[0] if msg_cands else None

    # Print a neat summary for user visibility
    # print("\nFile Discovery Summary")
    # print("──────────────────────────────")
    # if claims_xlsx:
    #     print(f"Claims workbook:  {claims_xlsx}")
    # else:
    #     print("No claims workbook found.")

    # if payroll_xlsx:
    #     print(f"Payroll workbook: {payroll_xlsx}")
    # else:
    #     print("No payroll workbook found.")

    # if payroll_msg:
    #     print(f"Payroll email (.msg): {payroll_msg}")
    # else:
    #     print("No payroll email found.")

    # if pdfs:
    #     print(f"PDFs found ({len(pdfs)}):")
    #     for p in pdfs[:5]:
    #         print(f"   • {p}")
    #     if len(pdfs) > 5:
    #         print(f"   ...and {len(pdfs) - 5} more")
    # else:
    #     print("No PDFs found.")
    # print("──────────────────────────────\n")

    return {
        "claims_xlsx": claims_xlsx,
        "payroll_xlsx": payroll_xlsx,
        "payroll_msg": payroll_msg,
        "pdfs": pdfs,
        "support_excels": support_excels,
    }

def _in_period(dt, start_dt, end_dt):
    if pd.isna(dt):
        return True
    if start_dt is not None and dt < start_dt:
        return False
    if end_dt   is not None and dt > end_dt:
        return False
    return True

def run_project_folder(project_root: str) -> str:
    root = Path(project_root)
    out_dir = root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_prefix = out_dir / "result"  # will produce result.claims_with_comments.xlsx

    # Build the same args you use in terminal (WITHOUT debug by default)
    args = [
        sys.executable, "-m", "test2_copy",
        "--auto-input", str(root),
        "--out", str(out_prefix),
        "--match-threshold", "20",
        "--price-tol", "0.05",
        "--doc-col", "Document No.",
        "--claim-start", "2025-04-01",
        "--claim-end", "2025-06-31",
    ]

    # Only enable debug pages if the uploaded project actually has them
    has_debug_txt = any(root.rglob("debug_pages/*.txt"))
    if has_debug_txt:
        args.append("--use-debug-pages")    

    # Run and raise on failures so Streamlit shows the error
    subprocess.run(args, check=True)

    # Return the path your script writes
    return str(out_prefix.with_suffix(".claims_with_comments.xlsx"))

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="claims-checker")
    ap.add_argument("--auto-input", help="Folder to auto-discover claims workbook, payroll workbook, PDFs, and period")
    ap.add_argument("--out", required=True, help="Output prefix (no extension)")
    ap.add_argument("--match-threshold", type=int, default=80, help="Min description match score (0-100)")
    ap.add_argument("--price-tol", type=float, default=0.05, help="Amount tolerance")
    ap.add_argument("--doc-col", help="Explicit Excel column to use as document/reference id")
    ap.add_argument("--require-doc-match", action="store_true", help="Do not fallback to description-only when docno can't be matched")
    ap.add_argument("--price-tol-abs", type=float, default=None, help="Absolute amount tolerance (overrides --price-tol if set)")
    ap.add_argument("--price-tol-pct", type=float, default=0.0, help="Relative tolerance in percent of the PDF amount (e.g., 1.0 = ±1%)")
    ap.add_argument("--docno-mode", default="auto", choices=["auto","po","order","invoice","inv","sv", "segment","first_long_digits","last_long_digits","regex"], help="How to extract the document id from filename (default: auto)")
    ap.add_argument("--docno-segment-index", type=int, default=0, help="When --docno-mode=segment, which segment index to return (default 0)")
    ap.add_argument("--docno-regex", default="", help="When --docno-mode=regex, a regex with one capture group to return")
    ap.add_argument("--grossup", type=float, default=1.09, help="GST gross-up factor (only tested last), default 1.09")
    ap.add_argument("--use-debug-pages",action="store_true", help="Read text from debug_pages/*.txt next to each PDF instead of re-extracting")
    ap.add_argument("--claim-start", help="Inclusive claim period start (YYYY-MM-DD)")
    ap.add_argument("--claim-end", help="Inclusive claim period end (YYYY-MM-DD)")
    ap.add_argument("--keep-debug", action="store_true", help="Keep debug artifacts (debug_pages/, ocr_data/, textdump/). Default: deleted.")
    ap.add_argument("--claims-date-col",default=None,help=("Name of date column in the claims workbook used for period filter ""If not given, the tool will try common names."),
)
    return ap.parse_args()

def main():
    args = parse_args()

    if args.auto_input:
        discovery_root = Path(args.auto_input)
    else:
        discovery_root = Path(args.out).resolve().parent if args.out else Path.cwd()

    discovered = discover_files(discovery_root)

    claim_path = discovered["claims_xlsx"]
    if not claim_path:
        raise FileNotFoundError(
            f"Could not auto-discover a claims workbook in: {discovery_root}. "
            f"Expected an Excel with 'Description' and 'Amount' columns."
        )
    claim_xlsx = str(claim_path)

    support_excels = discovered.get("support_excels", [])

    # Payroll
    payroll_path = discovered["payroll_xlsx"]
    payroll_msg  = discovered["payroll_msg"]

    payroll_df = None
    if payroll_path:
        try:
            if payroll_msg:
                payroll_df = read_payroll_with_msg(payroll_path, payroll_msg)
            else:
                payroll_df = read_payroll_excel(payroll_path)
            print(f"[INFO] Payroll rows loaded: {0 if payroll_df is None else len(payroll_df)}")
        except Exception as e:
            print(f"Failed to read payroll workbook: {e}")

    # PDFs
    pdfs = discovered.get("pdfs", []) or []

    common_dbg_root = Path(args.out).parent
    (common_dbg_root / "debug_pages").mkdir(parents=True, exist_ok=True)
    run_textdump_path = Path(args.out).with_suffix(".textdump.txt")
    
    df = extract_pdf_lineitems(
        pdfs=pdfs,
        args=args,
        common_dbg_root=common_dbg_root,
        run_textdump_path=run_textdump_path,
    )

    start_dt = pd.to_datetime(args.claim_start, errors="coerce") if args.claim_start else None
    end_dt   = pd.to_datetime(args.claim_end, errors="coerce") if args.claim_end else None

    if start_dt is not None or end_dt is not None:
        df["in_period"] = df["date"].map(lambda d: _in_period(d, start_dt, end_dt))
    else:
        df["in_period"] = True

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    if claim_xlsx:
        reconcile_claims_and_write_outputs(
            df=df,
            args=args,
            claim_xlsx=claim_xlsx,
            support_excels=support_excels,
            payroll_df=payroll_df,
            common_dbg_root=common_dbg_root,
            out_prefix=out,
            start_dt=start_dt,
            end_dt=end_dt,
        )

    if not args.keep_debug:
        run_textdump_path.unlink(missing_ok=True)
        for p in ("debug_pages", "ocr_data", "textdump"):
            shutil.rmtree(common_dbg_root / p, ignore_errors=True)

    print("Done.")

if __name__ == "__main__":
    main()