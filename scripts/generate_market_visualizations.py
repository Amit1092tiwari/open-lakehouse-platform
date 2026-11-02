"""
================================================================================
Indian Stock Market Visualizations & Analytics Chart Generator
================================================================================
Generates high-resolution data visualization assets for the Indian Stock Market.
Outputs saved to: Datapipeline/assets/
"""

import os
import sys
import matplotlib
matplotlib.use('Agg')  # Headless rendering for headless environments / scripts
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Import scraper from same scripts directory
from indian_stock_market_scraper import IndianStockMarketScraper, DEFAULT_INDIAN_TICKERS, SECTOR_MAPPING

def generate_visualizations():
    assets_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assets"))
    os.makedirs(assets_dir, exist_ok=True)

    print("[1/4] Scraping live Indian market data...")
    scraper = IndianStockMarketScraper(request_timeout=15)
    quotes_df = scraper.fetch_quotes(DEFAULT_INDIAN_TICKERS)

    equities_df = quotes_df[~quotes_df["symbol"].str.startswith("^")].copy()
    indices_df = quotes_df[quotes_df["symbol"].str.startswith("^")].copy()

    # Style configuration
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['figure.dpi'] = 150

    # -------------------------------------------------------------------------
    # Chart 1: Top Indian Equities by Market Capitalization
    # -------------------------------------------------------------------------
    print("[2/4] Generating Chart 1: Market Capitalization Distribution...")
    top12_mcap = equities_df.sort_values(by="MarketCap_Cr", ascending=True).tail(12)

    fig, ax = plt.subplots(figsize=(11, 6.5))
    colors = plt.cm.viridis(np.linspace(0.25, 0.9, len(top12_mcap)))

    bars = ax.barh(
        top12_mcap["shortName"].str.slice(0, 24),
        top12_mcap["MarketCap_Cr"] / 1e5,
        color=colors,
        edgecolor="#2c3e50",
        linewidth=0.8
    )
    ax.set_xlabel("Market Capitalization (INR Lakh Crores)", fontweight="bold", fontsize=11)
    ax.set_title("Top 12 Indian Companies by Market Capitalization (NSE / BSE)", fontsize=14, fontweight="bold", pad=14)

    for bar in bars:
        width = bar.get_width()
        ax.text(
            width + 0.15,
            bar.get_y() + bar.get_height() / 2,
            f"INR {width:.2f}L Cr",
            va='center', ha='left', fontsize=9, fontweight='bold', color="#2c3e50"
        )

    ax.set_xlim(0, max(top12_mcap["MarketCap_Cr"] / 1e5) * 1.18)
    plt.tight_layout()
    chart1_path = os.path.join(assets_dir, "indian_market_cap_distribution.png")
    fig.savefig(chart1_path, dpi=150)
    plt.close(fig)
    print(f" -> Saved: {chart1_path}")

    # -------------------------------------------------------------------------
    # Chart 2: Valuation (P/E) vs 52-Week High Proximity Scatter Matrix
    # -------------------------------------------------------------------------
    print("[3/4] Generating Chart 2: Valuation Multiples & Price Proximity...")
    val_df = equities_df.dropna(subset=["trailingPE", "Pct_From_52W_High"]).copy()
    val_df = val_df[val_df["trailingPE"] < 90]

    fig, ax = plt.subplots(figsize=(11, 6.5))
    scatter = ax.scatter(
        val_df["Pct_From_52W_High"],
        val_df["trailingPE"],
        s=val_df["MarketCap_Cr"] / 4000 + 50,
        c=val_df["regularMarketChangePercent"],
        cmap="coolwarm",
        alpha=0.88,
        edgecolors="#2c3e50",
        linewidth=0.9
    )

    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Daily Return (%)", fontweight="bold", fontsize=10)

    for _, row in val_df.iterrows():
        ax.annotate(
            row["symbol"].replace(".NS", ""),
            (row["Pct_From_52W_High"], row["trailingPE"]),
            textcoords="offset points", xytext=(6, 5), fontsize=8.5, fontweight="bold", color="#1a252f"
        )

    ax.axvline(x=0, color="#7f8c8d", linestyle="--", alpha=0.7)
    ax.set_xlabel("% Distance from 52-Week High (0% = At 52W High)", fontweight="bold", fontsize=11)
    ax.set_ylabel("Trailing P/E Ratio (x)", fontweight="bold", fontsize=11)
    ax.set_title("Valuation (P/E) vs Distance to 52-Week High (Bubble Size = Market Cap)", fontsize=13, fontweight="bold", pad=14)
    plt.tight_layout()
    chart2_path = os.path.join(assets_dir, "indian_stock_valuation_matrix.png")
    fig.savefig(chart2_path, dpi=150)
    plt.close(fig)
    print(f" -> Saved: {chart2_path}")

    # -------------------------------------------------------------------------
    # Chart 3: Historical Technical Indicators (Reliance Industries)
    # -------------------------------------------------------------------------
    print("[4/4] Generating Chart 3: Technical Indicators & Bollinger Bands...")
    ticker = "RELIANCE.NS"
    ohlcv_df = scraper.fetch_historical_ohlcv(ticker, time_range="6mo", interval="1d")

    if not ohlcv_df.empty:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7.5), sharex=True, gridspec_kw={'height_ratios': [3, 1]})
        dates = pd.to_datetime(ohlcv_df["Date"])

        # Panel 1: Price, SMA & Bollinger Bands
        ax1.plot(dates, ohlcv_df["Close"], label=f"{ticker} Close Price", color="#1f77b4", linewidth=2.2)
        ax1.plot(dates, ohlcv_df["SMA_20"], label="20-Day SMA", color="#ff7f0e", linestyle="--", linewidth=1.5)
        ax1.plot(dates, ohlcv_df["SMA_50"], label="50-Day SMA", color="#2ca02c", linestyle="-.", linewidth=1.5)
        ax1.fill_between(dates, ohlcv_df["BB_Lower"], ohlcv_df["BB_Upper"], color="#95a5a6", alpha=0.2, label="20D Bollinger Bands (±2σ)")

        ax1.set_ylabel("Price in INR (₹)", fontweight="bold", fontsize=11)
        ax1.set_title(f"{ticker} - Historical Trend, Moving Averages & Volatility Bands (6 Months)", fontsize=13, fontweight="bold", pad=12)
        ax1.legend(loc="upper left", frameon=True, framealpha=0.9)

        # Panel 2: 14-period RSI
        ax2.plot(dates, ohlcv_df["RSI_14"], label="14-Day RSI", color="#8e44ad", linewidth=1.7)
        ax2.axhline(70, color="#c0392b", linestyle=":", alpha=0.8, label="Overbought (70)")
        ax2.axhline(30, color="#27ae60", linestyle=":", alpha=0.8, label="Oversold (30)")
        ax2.axhline(50, color="#7f8c8d", linestyle="--", alpha=0.5)
        ax2.set_ylabel("RSI", fontweight="bold", fontsize=11)
        ax2.set_xlabel("Trading Date", fontweight="bold", fontsize=11)
        ax2.set_ylim(15, 85)
        ax2.legend(loc="lower left", fontsize=9, frameon=True, framealpha=0.9)

        plt.tight_layout()
        chart3_path = os.path.join(assets_dir, "reliance_technical_indicators.png")
        fig.savefig(chart3_path, dpi=150)
        plt.close(fig)
        print(f" -> Saved: {chart3_path}")

    print("\n[SUCCESS] All Indian stock market visualizations generated successfully in assets/ directory!")

if __name__ == "__main__":
    generate_visualizations()
