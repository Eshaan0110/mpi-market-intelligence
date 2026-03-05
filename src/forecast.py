"""
src/forecast.py
ML Forecasting — Credit & Debit Card Transaction Volumes
=========================================================
Uses Facebook Prophet for 6-month ahead forecasting.

Why Prophet over ARIMA?
- Handles missing months gracefully
- Works well with < 100 data points
- Built-in yearly seasonality
- Uncertainty intervals out of the box

Outputs:
  - data/processed/forecast_credit.csv
  - data/processed/forecast_debit.csv
  - plots/forecast_credit.html
  - plots/forecast_debit.html
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from pathlib import Path
from loguru import logger
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT_DIR  = Path(__file__).resolve().parent.parent
PROC_DIR  = ROOT_DIR / "data" / "processed"
PLOTS_DIR = ROOT_DIR / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────────────
FORECAST_MONTHS = 6
TEST_MONTHS     = 6    # hold out last 6 months for evaluation


# ── Metrics ────────────────────────────────────────────────────────────────

def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    # MAPE — skip zeros to avoid division error
    mask = y_true != 0
    mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    return {
        "MAE":  round(mae, 2),
        "RMSE": round(rmse, 2),
        "MAPE": round(mape, 2),
    }


# ── Core forecast function ─────────────────────────────────────────────────

def forecast_series(
    df: pd.DataFrame,
    date_col: str,
    target_col: str,
    label: str,
) -> dict:
    """
    Train Prophet on historical monthly data, evaluate on held-out
    test set, then forecast FORECAST_MONTHS ahead.

    Args:
        df:         Full historical DataFrame
        date_col:   Name of the date column
        target_col: Column to forecast
        label:      Human readable name for plots

    Returns:
        dict with: metrics, forecast_df, fig
    """

    # ── 1. Prep data in Prophet format (needs cols named ds, y) ───────────
    series = (df[[date_col, target_col]]
                .rename(columns={date_col: "ds", target_col: "y"})
                .dropna()
                .sort_values("ds")
                .reset_index(drop=True))

    logger.info(f"[{label}] {len(series)} months of data")

    # ── 2. Train / test split ─────────────────────────────────────────────
    split    = len(series) - TEST_MONTHS
    train_df = series.iloc[:split]
    test_df  = series.iloc[split:]
    logger.info(f"[{label}] Train: {len(train_df)} | Test: {len(test_df)}")

    # ── 3. Fit on train, predict over train+test period ───────────────────
    model = _build_prophet()
    model.fit(train_df)

    # Make future df that covers the test period
    future_test = model.make_future_dataframe(
        periods=TEST_MONTHS, freq="MS"
    )
    pred_test = model.predict(future_test)

    # Extract test predictions (last TEST_MONTHS rows)
    y_pred   = pred_test.iloc[-TEST_MONTHS:]["yhat"].values
    y_true   = test_df["y"].values
    metrics  = compute_metrics(y_true, y_pred)
    logger.info(f"[{label}] Test metrics → {metrics}")

    # ── 4. Refit on ALL data, forecast 6 months ahead ────────────────────
    model_full = _build_prophet()
    model_full.fit(series)

    future = model_full.make_future_dataframe(
        periods=FORECAST_MONTHS, freq="MS"
    )
    forecast = model_full.predict(future)

    # ── 5. Save forecast CSV ──────────────────────────────────────────────
    out_path = PROC_DIR / f"forecast_{target_col}.csv"
    forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].to_csv(
        out_path, index=False
    )
    logger.info(f"[{label}] Forecast saved → {out_path}")

    # ── 6. Build plot ─────────────────────────────────────────────────────
    fig = build_plot(series, forecast, test_df, label, metrics)
    plot_path = PLOTS_DIR / f"forecast_{target_col}.html"
    fig.write_html(str(plot_path))
    logger.info(f"[{label}] Plot saved → {plot_path}")

    return {"metrics": metrics, "forecast_df": forecast, "fig": fig}


def _build_prophet() -> Prophet:
    """Shared Prophet config — with COVID lockdown changepoints."""
    return Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
        interval_width=0.90,
        # Tell Prophet about known structural breaks
        # so it doesn't confuse them for trend changes
        changepoints=[
            "2020-03-01",  # COVID lockdown starts
            "2020-06-01",  # Unlock phase begins
            "2021-04-01",  # Second wave
            "2021-06-01",  # Recovery
        ],
        changepoint_prior_scale=0.05,  # conservative — don't overfit to noise
    )

# ── Plot builder ───────────────────────────────────────────────────────────

def build_plot(
    history: pd.DataFrame,
    forecast: pd.DataFrame,
    test_df: pd.DataFrame,
    label: str,
    metrics: dict,
) -> go.Figure:
    """Interactive Plotly chart: history + test overlay + forecast + CI."""
    fig = go.Figure()

    # Full historical line
    fig.add_trace(go.Scatter(
        x=history["ds"], y=history["y"],
        mode="lines+markers",
        name="Historical",
        line=dict(color="#2563EB", width=2),
        marker=dict(size=4),
    ))

    # Test set actuals highlighted in red
    fig.add_trace(go.Scatter(
        x=test_df["ds"], y=test_df["y"],
        mode="markers",
        name=f"Test set (last {TEST_MONTHS} months)",
        marker=dict(color="#DC2626", size=9, symbol="circle-open", line=dict(width=2)),
    ))

    # Forecast line
    fig.add_trace(go.Scatter(
        x=forecast["ds"], y=forecast["yhat"],
        mode="lines",
        name="Forecast",
        line=dict(color="#16A34A", width=2, dash="dash"),
    ))

    # 90% confidence interval band
    fig.add_trace(go.Scatter(
        x=pd.concat([forecast["ds"], forecast["ds"][::-1]]),
        y=pd.concat([forecast["yhat_upper"], forecast["yhat_lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(22,163,74,0.12)",
        line=dict(color="rgba(255,255,255,0)"),
        name="90% Confidence Interval",
        showlegend=True,
    ))

    fig.update_layout(
        title=dict(
            text=(f"{label} — 6-Month Forecast<br>"
                  f"<sup>MAE: {metrics['MAE']:,.0f} | "
                  f"RMSE: {metrics['RMSE']:,.0f} | "
                  f"MAPE: {metrics['MAPE']:.1f}%</sup>"),
            font=dict(size=18),
        ),
        xaxis_title="Month",
        yaxis_title=label,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        height=500,
    )
    return fig


# ── Bonus: anomaly detection ───────────────────────────────────────────────

def flag_anomalies(
    history: pd.DataFrame,
    forecast: pd.DataFrame,
    date_col: str,
    target_col: str,
) -> pd.DataFrame:
    """
    Flag months where actuals fall outside Prophet's confidence interval.
    Simple but interpretable — no extra model needed.
    """
    merged = (history.rename(columns={date_col: "ds", target_col: "actual"})
                     .merge(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]],
                            on="ds", how="inner"))

    merged["anomaly"] = (
        (merged["actual"] < merged["yhat_lower"]) |
        (merged["actual"] > merged["yhat_upper"])
    )

    flagged = merged[merged["anomaly"]]
    logger.info(f"Anomalies flagged: {len(flagged)} / {len(merged)} months")
    if len(flagged):
        print("\n=== Anomalous months ===")
        print(flagged[["ds", "actual", "yhat_lower", "yhat", "yhat_upper"]].to_string(index=False))

    return merged


# ── Main runner ────────────────────────────────────────────────────────────

def run_forecasting() -> None:
    """Load processed data and forecast both credit and debit card volumes."""

    parquet_path = PROC_DIR / "rbi_psi_cards.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            "Processed data not found. Run src/ingestion.py first."
        )

    df = pd.read_parquet(parquet_path)
    logger.info(f"Loaded {len(df)} rows from {parquet_path.name}")

    # ── Credit cards ──────────────────────────────────────────────────────
    print("\n" + "="*50)
    print("CREDIT CARD TRANSACTION VOLUME")
    print("="*50)
    credit = forecast_series(
        df,
        date_col="date",
        target_col="credit_card_vol_lakh",
        label="Credit Card Transactions (Lakh)",
    )
    flag_anomalies(df, credit["forecast_df"], "date", "credit_card_vol_lakh")

    # ── Debit cards ───────────────────────────────────────────────────────
    print("\n" + "="*50)
    print("DEBIT CARD TRANSACTION VOLUME")
    print("="*50)
    debit = forecast_series(
        df,
        date_col="date",
        target_col="debit_card_vol_lakh",
        label="Debit Card Transactions (Lakh)",
    )
    flag_anomalies(df, debit["forecast_df"], "date", "debit_card_vol_lakh")

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "="*50)
    print("RESULTS SUMMARY")
    print("="*50)
    print(f"Credit card forecast  →  MAE: {credit['metrics']['MAE']:>10,.0f} | "
          f"MAPE: {credit['metrics']['MAPE']:.1f}%")
    print(f"Debit card forecast   →  MAE: {debit['metrics']['MAE']:>10,.0f} | "
          f"MAPE: {debit['metrics']['MAPE']:.1f}%")
    print(f"\nPlots saved to: {PLOTS_DIR}")
    print("Open the .html files in your browser for interactive charts!")


if __name__ == "__main__":
    run_forecasting()