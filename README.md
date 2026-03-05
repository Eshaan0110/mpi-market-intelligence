# MPi — Market Intelligence Platform

RBI Payment Systems data ingestion and ML forecasting pipeline.
Built as part of the MPi intern technical assignment.

---

## Project Structure
```
mpi-market-intelligence/
├── src/
│   ├── ingestion.py      # Task 1 — RBI data parser
│   └── forecast.py       # Task 2 — Prophet forecasting
├── data/
│   ├── raw/              # Downloaded RBI Excel files (not committed)
│   └── processed/        # Cleaned Parquet + CSV outputs
├── plots/                # Interactive HTML forecast charts
├── PIPELINE_DESIGN.md    # Task 3 — Production architecture
├── pyproject.toml        # Dependencies (managed via uv)
└── README.md
```

---

## Setup

**Requirements:** Python 3.11+, [uv](https://astral.sh/uv)
```bash
git clone https://github.com/YOUR_USERNAME/mpi-market-intelligence.git
cd mpi-market-intelligence
uv sync                        # installs all dependencies from uv.lock
```

---

## Running the Pipeline

### Task 1 — Data Ingestion

1. Download the RBI Payment System Indicators Excel manually:
   - Go to [RBI DBIE](https://dbie.rbi.org.in)
   - Navigate to Statistics → Financial Sector → Payment Systems
   - Click "Payment System Indicators" (Monthly) → Download Excel
   - Save to `data/raw/`

2. Run the parser:
```bash
uv run python -m src.ingestion
```

Output: `data/processed/rbi_psi_cards.parquet` and `rbi_psi_cards.csv`

---

### Task 2 — Forecasting
```bash
uv run python -m src.forecast
```

Output:
- `data/processed/forecast_credit_card_vol_lakh.csv`
- `data/processed/forecast_debit_card_vol_lakh.csv`
- `plots/forecast_credit_card_vol_lakh.html` ← open in browser
- `plots/forecast_debit_card_vol_lakh.html` ← open in browser

---

### Task 3 — Pipeline Design

See [PIPELINE_DESIGN.md](./PIPELINE_DESIGN.md)

---

## Key Design Decisions

**Why Prophet over ARIMA?**
74 months of data is on the lower end for ARIMA. Prophet handles
short series better, deals with missing months gracefully, and
gives uncertainty intervals out of the box.

**Why Parquet over CSV for processed data?**
Typed columns, 5x smaller, 10x faster reads for ML workflows.
CSV kept alongside for human readability.

**Why uv over pip?**
10-100x faster installs, automatic lockfile for reproducibility,
replaces pip + venv + requirements.txt in one tool.

**On debit card forecast accuracy (MAPE 31%):**
The debit card series has a genuine structural break — UPI has
been displacing debit card transactions since 2022. This isn't
a modelling failure; it reflects a real market shift that no
purely historical model can fully anticipate. The credit card
model performs well at 2.1% MAPE as that series has no such
disruption.

**On RBI portal access:**
The DBIE portal renders entirely via JavaScript — direct HTTP
returns an empty page. Playwright is used to automate browser
interaction. The ingestion script auto-detects the Excel file
in data/raw/ by filename pattern so the download step can be
manual or automated interchangeably.

---

## Results Summary

| Model | MAE | MAPE |
|---|---|---|
| Credit Card Volume (Prophet) | 493 Lakh | 2.1% |
| Debit Card Volume (Prophet) | 182 Lakh | 31% |

Anomaly detection flagged COVID lockdown months (Apr-May 2020)
and UPI transition period (2022) as expected outliers.