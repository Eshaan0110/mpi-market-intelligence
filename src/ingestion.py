"""
src/ingestion.py
RBI Payment System Indicators — Parser
Extracts credit/debit card transaction data from RBI Table 45 Excel.

Column mapping (0-indexed, verified against actual RBI file):
  1   = Month/Year
  52  = Credit Cards Volume (Lakh)
  53  = Credit Cards Value (Rupees Crores)
  58  = Debit Cards Volume (Lakh)
  59  = Debit Cards Value (Rupees Crores)
  118 = Credit Cards Outstanding (Lakh)
  120 = Debit Cards Outstanding (Lakh)
"""

import pandas as pd
import openpyxl
from pathlib import Path
from loguru import logger

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR  = ROOT_DIR / "data" / "raw"
PROC_DIR = ROOT_DIR / "data" / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ── Exact column indices from RBI Excel (0-based, verified) ───────────────
COL_DATE             = 1
COL_CREDIT_VOL       = 52
COL_CREDIT_VAL       = 53
COL_DEBIT_VOL        = 58
COL_DEBIT_VAL        = 59
COL_CREDIT_CARDS_OUT = 118
COL_DEBIT_CARDS_OUT  = 120
DATA_START_ROW       = 7    # first row index of actual monthly data


def safe_float(row, idx):
    """Return float from row[idx], or None if missing/dash/invalid."""
    val = row[idx] if len(row) > idx else None
    if val is None or str(val).strip() in ("-", ""):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def parse_psi_excel(filepath: str | Path) -> pd.DataFrame:
    """
    Parse RBI Table 45 Payment System Indicators Excel.
    Returns a clean monthly DataFrame.
    """
    filepath = Path(filepath)
    logger.info(f"Parsing: {filepath.name}")

    wb   = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws   = wb.active
    rows = list(ws.iter_rows(values_only=True))

    records = []
    for row in rows[DATA_START_ROW:]:
        date_val = row[COL_DATE] if len(row) > COL_DATE else None

        # Stop at footnote rows at the bottom of the file
        if not date_val or not isinstance(date_val, str):
            continue
        try:
            date = pd.to_datetime(date_val, format="%b-%Y")
        except Exception:
            logger.debug(f"Skipping non-date row: {date_val}")
            continue

        records.append({
            "date":                          date,
            "credit_card_vol_lakh":          safe_float(row, COL_CREDIT_VOL),
            "credit_card_val_cr":            safe_float(row, COL_CREDIT_VAL),
            "debit_card_vol_lakh":           safe_float(row, COL_DEBIT_VOL),
            "debit_card_val_cr":             safe_float(row, COL_DEBIT_VAL),
            "credit_cards_outstanding_lakh": safe_float(row, COL_CREDIT_CARDS_OUT),
            "debit_cards_outstanding_lakh":  safe_float(row, COL_DEBIT_CARDS_OUT),
        })

    df = (pd.DataFrame(records)
            .sort_values("date")
            .reset_index(drop=True))

    logger.info(
        f"Parsed {len(df)} rows | "
        f"{df['date'].min().strftime('%b %Y')} → "
        f"{df['date'].max().strftime('%b %Y')}"
    )
    return df


def save_outputs(df: pd.DataFrame) -> None:
    """Save to both CSV (human readable) and Parquet (ML ready)."""
    df.to_csv(PROC_DIR / "rbi_psi_cards.csv", index=False)
    df.to_parquet(PROC_DIR / "rbi_psi_cards.parquet", index=False)
    logger.info(f"Outputs saved to {PROC_DIR}")


def run_ingestion(excel_path: str | Path = None) -> pd.DataFrame:
    """Main entry point — parse, validate, save."""
    if excel_path is None:
        # Default: look for the file in data/raw/
        matches = list(RAW_DIR.glob("*Payment_System*"))
        if not matches:
            raise FileNotFoundError(
                f"No Payment System Indicators Excel found in {RAW_DIR}\n"
                "Download it from: RBI DBIE → Statistics → Financial Sector → Payment Systems"
            )
        excel_path = matches[0]
        logger.info(f"Auto-found: {excel_path.name}")

    df = parse_psi_excel(excel_path)

    # Warn on columns with too many nulls
    for col, pct in (df.isnull().mean() * 100).items():
        if pct > 20:
            logger.warning(f"High nulls in '{col}': {pct:.1f}%")

    save_outputs(df)

    print("\n=== Sample — last 5 months ===")
    print(df.tail().to_string(index=False))
    print(f"\nTotal rows : {len(df)}")
    print(f"Date range : {df['date'].min().strftime('%b %Y')} → {df['date'].max().strftime('%b %Y')}")
    print(f"\nNull counts:\n{df.isnull().sum()}")

    return df


if __name__ == "__main__":
    run_ingestion()