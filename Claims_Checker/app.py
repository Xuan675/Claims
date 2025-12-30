import streamlit as st
import tempfile, zipfile, shutil, sys, subprocess
from pathlib import Path

st.set_page_config(page_title="Claims Checker")

st.title("Tool")
st.markdown("""
Upload your **project folder (.zip)** — containing:
- claims workbook  
- payroll workbook  
- optional .msg for password  
- supporting PDFs  

The app will automatically reconcile everything and let you download the result.
""")

st.subheader("Settings")

doc_col = st.text_input(
    "Document column name in claims workbook (optional)",
    value="Document No.",  # default, user can change/clear
    help='Examples: "Document No.", "Document no.", "Source Doc Ref". Leave blank if unsure.'
)

date_col = st.text_input(
    "Claims date column name (optional)",
    value="",  # or e.g. "Posting date" if you want a default
    help='Examples: "Posting date", "Document Date". Leave blank to let the tool auto-detect.'
)

st.caption("Claim period (optional – leave as is for no filter)")
claim_start = st.date_input(
    "Claim start date",
    value=None,
    format="YYYY-MM-DD",
)
claim_end = st.date_input(
    "Claim end date",
    value=None,
    format="YYYY-MM-DD",
)

uploaded_zip = st.file_uploader("📁 Drop your zipped project folder here", type=["zip"])

def run_project_folder(
    project_root: str,
    doc_col: str | None,
    claim_start: str | None,
    claim_end: str | None,
    claims_date_col: str | None,
) -> str:
    """
    Wraps your CLI script test2_copy as a subprocess call, using
    the options chosen in the Streamlit UI.
    """
    match_threshold = 20
    price_tol = 0.05

    root = Path(project_root)
    out_dir = root / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_prefix = out_dir / "result"

    args = [
        sys.executable, "-m", "test2_copy",
        "--auto-input", str(root),
        "--out", str(out_prefix),
        "--match-threshold", str(match_threshold),
        "--price-tol", str(price_tol),
    ]

    if doc_col:
        args += ["--doc-col", doc_col]
    if claim_start:
        args += ["--claim-start", claim_start]
    if claim_end:
        args += ["--claim-end", claim_end]
    if claims_date_col:
        args += ["--claims-date-col", claims_date_col]

    # Only enable debug pages if the uploaded project already has them
    has_debug_txt = any(root.rglob("debug_pages/*.txt"))
    if has_debug_txt:
        args.append("--use-debug-pages")

    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "CLI run failed "
            f"(exit code {result.returncode}).\n\n"
            f"STDOUT:\n{result.stdout}\n\n"
            f"STDERR:\n{result.stderr}"
        )

    return str(out_prefix.with_suffix(".claims_with_comments.xlsx"))

if uploaded_zip:
    status = st.empty()

    run_clicked = st.button("▶️ Start reconciliation")

    if run_clicked: 
        # temp root for this request
        tmpdir = Path(tempfile.mkdtemp())
        extract_path = tmpdir / "project"
        extract_path.mkdir(parents=True, exist_ok=True)

        # Save uploaded zip to disk first (more robust than using file-like directly)
        outer_zip_path = tmpdir / "uploaded_project.zip"
        with open(outer_zip_path, "wb") as f:
            f.write(uploaded_zip.read())

        # 1) Extract main project zip
        with zipfile.ZipFile(outer_zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        # 2) Auto-unzip any nested .zip files (e.g. Supporting_Docs.zip)
        for nested_zip in extract_path.rglob("*.zip"):
            try:
                # create a folder with the same name (without .zip)
                target_dir = nested_zip.with_suffix("")
                target_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(nested_zip, "r") as zf:
                    zf.extractall(target_dir)
                nested_zip.unlink(missing_ok=True)
            except Exception as e:
                pass

        status.success("Project folder extracted!")

        status.info("⚙️ Running reconciliation... please wait")
        try:
            claim_start_str = claim_start.isoformat() if claim_start else None
            claim_end_str = claim_end.isoformat() if claim_end else None

            output_file = run_project_folder(
                project_root=str(extract_path),
                doc_col=doc_col.strip() or None,
                claim_start=claim_start_str,
                claim_end=claim_end_str,
                claims_date_col=date_col.strip() or None,
            )
            
            output_path = Path(output_file)
            
            if output_path.exists():    
                status.success("Reconciliation complete!")
                with open(output_path, "rb") as f:
                    st.download_button(
                        label="⬇️ Download Results (.xlsx)",
                        data=f.read(),
                        file_name=output_path.name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
            else:
                status.error("❌ Output file not found — check logs.")
        except Exception as e:
            status.error(f"❌ Error during processing: {e}")
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
else:
    st.info("Please upload a zipped project folder first.")