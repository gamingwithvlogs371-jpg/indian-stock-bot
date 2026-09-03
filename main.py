from pathlib import Path

code = r'''import os
import math
import asyncio
from datetime import datetime, timezone, timedelta

import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse


# ============================================================
# QUANT ENGINE V2
# Paper-trading quantitative engine for Indian NSE stocks.
#
# IMPORTANT:
# - This version does NOT call Gemini for every trade.
# - It is PAPER TRADING ONLY. It does not send broker orders.
# - Put DATABASE_URL in your hosting environment variables.
# ============================================================

app = FastAPI(title="Quant Engine V2 - Indian Paper Trading")


# -----------------------------
# Configuration
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

INITIAL_CASH = float(os.getenv("INITIAL_CASH", "50000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.0075"))       # 0.75%
MAX_POSITION_PCT = float(os.getenv("MAX_POSITION_PCT", "0.25"))    # 25% max capital
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "4"))
MAX_DAILY_LOSS_PCT = float(os.getenv("MAX_DAILY_LOSS_PCT", "0.02")) # 2%
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "60"))

# Paper-trading watchlist.
WATCHLIST = [
    "SBIN.NS",
    "TATAMOTORS.NS",
    "ITC.NS",
    "INFY.NS",
    "RELIANCE.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS",
    "BHARTIARTL.NS",
    "TCS.NS",
    "AXISBANK.NS",
]

IST = timezone(timedelta(hours=5, minutes=30))


# ============================================================
# Time / market helpers
# ============================================================

def now_ist():
    return datetime.now(IST)


def to_ist_datetime_str(ts):
    if not ts:
        return "-"

    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except Exception:
            return str(ts)[:19]

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return ts.astimezone(IST).strftime("%d-%b %H:%M:%S")


def is_market_open():
    now = now_ist()

    if now.weekday() >= 5:
        return False, f"Market Closed - {now.strftime('%A')}"

    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if start <= now <= end:
        return True, "Market Open"

    return False, "Market Closed - NSE trading hours 09:15-15:30 IST"


# ============================================================
# Database
# ============================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")

    return psycopg2.connect(DATABASE_URL)


def init_db():
    if not DATABASE_URL:
        print("DATABASE_URL not configured. Server can start, but trading is disabled.")
        return

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio (
                    id INT PRIMARY KEY DEFAULT 1,
                    cash DOUBLE PRECISION NOT NULL,
                    starting_cash DOUBLE PRECISION NOT NULL DEFAULT 50000,
                    day_start_equity DOUBLE PRECISION NOT NULL DEFAULT 50000,
                    day_start_date DATE NOT NULL DEFAULT CURRENT_DATE
                );

                CREATE TABLE IF NOT EXISTS holdings (
                    symbol TEXT PRIMARY KEY,
                    qty INT NOT NULL,
                    buy_price DOUBLE PRECISION NOT NULL,
                    stop_price DOUBLE PRECISION NOT NULL,
                    target_price DOUBLE PRECISION NOT NULL,
                    entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS ledger (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    qty INT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    total_value DOUBLE PRECISION NOT NULL,
                    pnl_pct DOUBLE PRECISION,
                    pnl_value DOUBLE PRECISION,
                    balance DOUBLE PRECISION NOT NULL,
                    reason TEXT
                );

                CREATE TABLE IF NOT EXISTS decision_logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    price DOUBLE PRECISION,
                    status TEXT,
                    reason TEXT
                );

                CREATE TABLE IF NOT EXISTS valuation_history (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_value DOUBLE PRECISION NOT NULL,
                    cash DOUBLE PRECISION NOT NULL
                );

                INSERT INTO portfolio
                    (id, cash, starting_cash, day_start_equity, day_start_date)
                VALUES
                    (1, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT (id) DO NOTHING;
                """,
                (INITIAL_CASH, INITIAL_CASH, INITIAL_CASH),
            )

            conn.commit()


# ============================================================
# Technical analysis
# ============================================================

def calculate_indicators(df):
    """
    Calculates indicators from OHLCV data.

    Strategy:
      - EMA 9 / 21 / 50
      - RSI 14
      - ATR 14
      - MACD histogram
      - volume ratio
      - recent breakout
    """

    if df is None or df.empty:
        return None

    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(df.columns):
        return None

    data = df.copy()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data = data.dropna(subset=["High", "Low", "Close"])

    if len(data) < 60:
        return None

    close = data["Close"]
    high = data["High"]
    low = data["Low"]
    volume = data["Volume"].fillna(0)

    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-12)
    rsi = 100 - (100 / (1 + rs))

    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = true_range.rolling(14).mean()

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - macd_signal

    avg_volume = volume.rolling(20).mean()
    volume_ratio = volume / avg_volume.replace(0, 1e-12)

    # Previous 20-bar high, excluding current bar.
    previous_20_high = high.shift(1).rolling(20).max()

    latest = data.index[-1]

    values = {
        "price": float(close.iloc[-1]),
        "ema9": float(ema9.iloc[-1]),
        "ema21": float(ema21.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "rsi": float(rsi.iloc[-1]),
        "atr": float(atr.iloc[-1]),
        "macd": float(macd.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "macd_hist": float(macd_hist.iloc[-1]),
        "volume": float(volume.iloc[-1]),
        "volume_ratio": float(volume_ratio.iloc[-1]),
        "previous_20_high": float(previous_20_high.iloc[-1]),
        "timestamp": latest,
    }

    if not all(math.isfinite(v) for k, v in values.items() if k != "timestamp"):
        return None

    return values


def generate_signal(ind):
    """
    Score-based entry model.

    A BUY requires:
      - bullish trend
      - positive momentum
      - acceptable RSI
      - volume confirmation
      - breakout OR strong trend alignment

    Score >= 5 is required.
    """

    price = ind["price"]
    ema9 = ind["ema9"]
    ema21 = ind["ema21"]
    ema50 = ind["ema50"]
    rsi = ind["rsi"]
    macd_hist = ind["macd_hist"]
    volume_ratio = ind["volume_ratio"]
    previous_20_high = ind["previous_20_high"]

    score = 0
    reasons = []

    # Trend alignment.
    if price > ema50:
        score += 1
        reasons.append("price>EMA50")

    if ema9 > ema21:
        score += 1
        reasons.append("EMA9>EMA21")

    if ema21 > ema50:
        score += 1
        reasons.append("EMA21>EMA50")

    # Momentum.
    if macd_hist > 0:
        score += 1
        reasons.append("MACD+")

    # RSI: avoid buying extremely overbought conditions.
    if 50 <= rsi <= 68:
        score += 1
        reasons.append("RSI healthy")

    # Volume confirmation.
    if volume_ratio >= 1.15:
        score += 1
        reasons.append("volume confirmation")

    # Breakout.
    if price > previous_20_high:
        score += 2
        reasons.append("20-bar breakout")

    # Hard trend requirement.
    trend_ok = price > ema50 and ema9 > ema21

    if score >= 5 and trend_ok:
        return True, score, "BUY: " + ", ".join(reasons)

    return False, score, "No entry: score=%d | %s" % (
        score,
        ", ".join(reasons) if reasons else "filters not met",
    )


# ============================================================
# Risk management
# ============================================================

def calculate_position(ind, cash, equity):
    """
    Risk-based position sizing.

    Risk per trade = fixed percentage of equity.
    Stop distance = 1.5 ATR.
    Position is also capped at MAX_POSITION_PCT of equity.
    """

    price = ind["price"]
    atr = ind["atr"]

    if price <= 0 or atr <= 0 or cash <= 0 or equity <= 0:
        return 0, None, None

    stop_distance = max(1.5 * atr, price * 0.008)

    risk_budget = equity * RISK_PER_TRADE

    qty_by_risk = int(risk_budget / stop_distance)

    max_capital = min(cash, equity * MAX_POSITION_PCT)
    qty_by_capital = int(max_capital // price)

    qty = min(qty_by_risk, qty_by_capital)

    if qty < 1:
        return 0, None, None

    stop_price = price - stop_distance

    # Reward:risk = 2:1.
    target_price = price + (stop_distance * 2.0)

    return qty, stop_price, target_price


def daily_loss_limit_hit(equity, day_start_equity):
    if day_start_equity <= 0:
        return False

    loss_pct = (equity - day_start_equity) / day_start_equity
    return loss_pct <= -MAX_DAILY_LOSS_PCT


# ============================================================
# Market data
# ============================================================

def get_market_data(symbol):
    try:
        ticker = yf.Ticker(symbol)

        # Yahoo generally provides 15m data for recent history only.
        hist = ticker.history(
            period="60d",
            interval="15m",
            auto_adjust=False,
            prepost=False,
        )

        if hist is None or hist.empty:
            return None

        # Remove timezone complications before indicator calculations.
        if getattr(hist.index, "tz", None) is not None:
            hist.index = hist.index.tz_convert(None)

        return hist

    except Exception as exc:
        print(f"[DATA ERROR] {symbol}: {exc}")
        return None


# ============================================================
# Portfolio helpers
# ============================================================

def get_portfolio_state(cur):
    cur.execute(
        "SELECT cash, starting_cash, day_start_equity, day_start_date "
        "FROM portfolio WHERE id=1;"
    )
    row = cur.fetchone()

    if not row:
        return {
            "cash": INITIAL_CASH,
            "starting_cash": INITIAL_CASH,
            "day_start_equity": INITIAL_CASH,
            "day_start_date": now_ist().date(),
        }

    return dict(row)


def reset_daily_baseline_if_needed(cur, equity):
    today = now_ist().date()

    cur.execute(
        "SELECT day_start_date FROM portfolio WHERE id=1;"
    )
    row = cur.fetchone()

    if not row:
        return

    stored_date = row["day_start_date"]

    if stored_date != today:
        cur.execute(
            """
            UPDATE portfolio
            SET day_start_equity=%s,
                day_start_date=%s
            WHERE id=1;
            """,
            (equity, today),
        )


def get_holdings(cur):
    cur.execute("SELECT * FROM holdings ORDER BY symbol;")
    return {row["symbol"]: dict(row) for row in cur.fetchall()}


# ============================================================
# Trading engine
# ============================================================

async def process_market_cycle():
    if not DATABASE_URL:
        return

    market_open, market_reason = is_market_open()

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            portfolio = get_portfolio_state(cur)
            cash = float(portfolio["cash"])

            holdings = get_holdings(cur)

            # ------------------------------------------------
            # First value current holdings.
            # ------------------------------------------------
            current_prices = {}
            holdings_value = 0.0

            for symbol, position in holdings.items():
                hist = get_market_data(symbol)

                if hist is None:
                    continue

                ind = calculate_indicators(hist)

                if not ind:
                    continue

                price = ind["price"]
                current_prices[symbol] = price
                holdings_value += position["qty"] * price

            equity = cash + holdings_value

            reset_daily_baseline_if_needed(cur, equity)

            portfolio = get_portfolio_state(cur)
            day_start_equity = float(portfolio["day_start_equity"])

            # ------------------------------------------------
            # Market closed.
            # ------------------------------------------------
            if not market_open:
                cur.execute(
                    """
                    SELECT status
                    FROM decision_logs
                    ORDER BY id DESC
                    LIMIT 1;
                    """
                )
                last_log = cur.fetchone()

                if not last_log or last_log["status"] != "MARKET CLOSED":
                    cur.execute(
                        """
                        INSERT INTO decision_logs
                            (symbol, price, status, reason)
                        VALUES
                            (%s, %s, 'MARKET CLOSED', %s);
                        """,
                        ("NSE/BSE", 0, market_reason),
                    )

                conn.commit()
                return

            # ------------------------------------------------
            # Daily loss protection.
            # ------------------------------------------------
            if daily_loss_limit_hit(equity, day_start_equity):
                cur.execute(
                    """
                    INSERT INTO decision_logs
                        (symbol, price, status, reason)
                    VALUES
                        (%s, %s, 'RISK HALT', %s);
                    """,
                    (
                        "PORTFOLIO",
                        0,
                        "Daily loss limit reached. New entries disabled.",
                    ),
                )
                conn.commit()
                return

            # ------------------------------------------------
            # Manage existing positions.
            # ------------------------------------------------
            for symbol, position in list(holdings.items()):

                hist = get_market_data(symbol)
                if hist is None:
                    continue

                ind = calculate_indicators(hist)
                if not ind:
                    continue

                price = ind["price"]
                qty = int(position["qty"])
                buy_price = float(position["buy_price"])
                stop_price = float(position["stop_price"])
                target_price = float(position["target_price"])

                pnl_pct = ((price - buy_price) / buy_price) * 100

                # Dynamic trailing stop:
                # Once trade is profitable, protect part of the gain.
                trailing_stop = price - (1.5 * ind["atr"])

                if pnl_pct > 1.0:
                    stop_price = max(stop_price, trailing_stop)

                    cur.execute(
                        """
                        UPDATE holdings
                        SET stop_price=%s
                        WHERE symbol=%s;
                        """,
                        (stop_price, symbol),
                    )

                sell_reason = None

                if price <= stop_price:
                    sell_reason = "ATR STOP"
                elif price >= target_price:
                    sell_reason = "2R TARGET"
                elif ind["ema9"] < ind["ema21"] and ind["macd_hist"] < 0:
                    sell_reason = "TREND + MOMENTUM REVERSAL"

                if sell_reason:
                    revenue = round(qty * price, 2)
                    pnl_value = round((price - buy_price) * qty, 2)

                    cash += revenue

                    cur.execute(
                        "UPDATE portfolio SET cash=%s WHERE id=1;",
                        (cash,),
                    )

                    cur.execute(
                        "DELETE FROM holdings WHERE symbol=%s;",
                        (symbol,),
                    )

                    cur.execute(
                        """
                        INSERT INTO ledger
                            (symbol, action, qty, price, total_value,
                             pnl_pct, pnl_value, balance, reason)
                        VALUES
                            (%s, 'SELL', %s, %s, %s, %s, %s, %s, %s);
                        """,
                        (
                            symbol,
                            qty,
                            price,
                            revenue,
                            pnl_pct,
                            pnl_value,
                            cash,
                            sell_reason,
                        ),
                    )

                    cur.execute(
                        """
                        INSERT INTO decision_logs
                            (symbol, price, status, reason)
                        VALUES
                            (%s, %s, 'EXECUTED SELL', %s);
                        """,
                        (
                            symbol,
                            price,
                            f"{sell_reason} | P&L {pnl_pct:+.2f}%",
                        ),
                    )

                    holdings.pop(symbol, None)

                else:
                    cur.execute(
                        """
                        INSERT INTO decision_logs
                            (symbol, price, status, reason)
                        VALUES
                            (%s, %s, 'HOLD', %s);
                        """,
                        (
                            symbol,
                            price,
                            (
                                f"P&L {pnl_pct:+.2f}% | "
                                f"Stop ₹{stop_price:.2f} | "
                                f"Target ₹{target_price:.2f}"
                            ),
                        ),
                    )

            # ------------------------------------------------
            # Refresh cash/holdings after exits.
            # ------------------------------------------------
            portfolio = get_portfolio_state(cur)
            cash = float(portfolio["cash"])
            holdings = get_holdings(cur)

            # ------------------------------------------------
            # New entries.
            # ------------------------------------------------
            if len(holdings) < MAX_POSITIONS:

                for symbol in WATCHLIST:

                    if symbol in holdings:
                        continue

                    # Re-check portfolio risk before each new trade.
                    if len(holdings) >= MAX_POSITIONS:
                        break

                    hist = get_market_data(symbol)

                    if hist is None:
                        continue

                    ind = calculate_indicators(hist)

                    if not ind:
                        continue

                    price = ind["price"]

                    buy_signal, score, reason = generate_signal(ind)

                    if not buy_signal:
                        cur.execute(
                            """
                            INSERT INTO decision_logs
                                (symbol, price, status, reason)
                            VALUES
                                (%s, %s, 'NO SIGNAL', %s);
                            """,
                            (symbol, price, reason),
                        )
                        continue

                    # Current holdings valuation.
                    current_holdings_value = 0.0

                    for held_symbol, held_position in holdings.items():
                        held_price = current_prices.get(
                            held_symbol,
                            float(held_position["buy_price"]),
                        )
                        current_holdings_value += (
                            held_position["qty"] * held_price
                        )

                    equity = cash + current_holdings_value

                    qty, stop_price, target_price = calculate_position(
                        ind,
                        cash,
                        equity,
                    )

                    if qty < 1:
                        cur.execute(
                            """
                            INSERT INTO decision_logs
                                (symbol, price, status, reason)
                            VALUES
                                (%s, %s, 'REJECTED', %s);
                            """,
                            (
                                symbol,
                                price,
                                "Position size below 1 share after risk controls.",
                            ),
                        )
                        continue

                    cost = round(qty * price, 2)

                    if cost > cash:
                        continue

                    # Final portfolio cap.
                    if cost > equity * MAX_POSITION_PCT:
                        qty = int((equity * MAX_POSITION_PCT) // price)
                        cost = round(qty * price, 2)

                    if qty < 1 or cost > cash:
                        continue

                    # ------------------------------------------------
                    # PAPER BUY
                    # ------------------------------------------------
                    cash -= cost

                    cur.execute(
                        "UPDATE portfolio SET cash=%s WHERE id=1;",
                        (cash,),
                    )

                    cur.execute(
                        """
                        INSERT INTO holdings
                            (symbol, qty, buy_price, stop_price, target_price)
                        VALUES
                            (%s, %s, %s, %s, %s);
                        """,
                        (
                            symbol,
                            qty,
                            price,
                            stop_price,
                            target_price,
                        ),
                    )

                    cur.execute(
                        """
                        INSERT INTO ledger
                            (symbol, action, qty, price, total_value,
                             pnl_pct, pnl_value, balance, reason)
                        VALUES
                            (%s, 'BUY', %s, %s, %s, NULL, NULL, %s, %s);
                        """,
                        (
                            symbol,
                            qty,
                            price,
                            cost,
                            cash,
                            f"Score {score}/8 | {reason}",
                        ),
                    )

                    cur.execute(
                        """
                        INSERT INTO decision_logs
                            (symbol, price, status, reason)
                        VALUES
                            (%s, %s, 'EXECUTED BUY', %s);
                        """,
                        (
                            symbol,
                            price,
                            (
                                f"Score {score}/8 | "
                                f"{reason} | "
                                f"SL ₹{stop_price:.2f} | "
                                f"TP ₹{target_price:.2f}"
                            ),
                        ),
                    )

                    holdings[symbol] = {
                        "symbol": symbol,
                        "qty": qty,
                        "buy_price": price,
                        "stop_price": stop_price,
                        "target_price": target_price,
                    }

                    if len(holdings) >= MAX_POSITIONS:
                        break

            # ------------------------------------------------
            # Portfolio valuation snapshot.
            # ------------------------------------------------
            holdings_value = 0.0

            for symbol, position in holdings.items():
                price = current_prices.get(symbol)

                if price is None:
                    price = float(position["buy_price"])

                holdings_value += int(position["qty"]) * price

            total_value = round(cash + holdings_value, 2)

            cur.execute(
                """
                INSERT INTO valuation_history
                    (total_value, cash)
                VALUES
                    (%s, %s);
                """,
                (total_value, cash),
            )

            # Keep dashboard data small.
            cur.execute(
                """
                DELETE FROM decision_logs
                WHERE id NOT IN (
                    SELECT id
                    FROM decision_logs
                    ORDER BY id DESC
                    LIMIT 200
                );
                """
            )

            cur.execute(
                """
                DELETE FROM valuation_history
                WHERE id NOT IN (
                    SELECT id
                    FROM valuation_history
                    ORDER BY id DESC
                    LIMIT 500
                );
                """
            )

            conn.commit()

            print(
                f"[{now_ist().strftime('%H:%M:%S')}] "
                f"Equity ₹{total_value:.2f} | "
                f"Cash ₹{cash:.2f} | "
                f"Positions {len(holdings)}"
            )


# ============================================================
# Background loop
# ============================================================

async def trading_loop():
    print("Quant Engine V2 started.")

    while True:
        try:
            await asyncio.to_thread(
                lambda: asyncio.run(process_market_cycle())
            )
        except Exception as exc:
            print(f"[TRADING LOOP ERROR] {exc}")

        await asyncio.sleep(LOOP_SECONDS)


@app.on_event("startup")
async def startup_event():
    try:
        await asyncio.to_thread(init_db)
    except Exception as exc:
        print(f"[DATABASE ERROR] {exc}")

    asyncio.create_task(trading_loop())


# ============================================================
# Dashboard
# ============================================================

@app.get("/", response_class=HTMLResponse)
def dashboard():
    if not DATABASE_URL:
        return """
        <html>
        <body style="background:#020617;color:white;font-family:Arial;padding:40px">
        <h1>Quant Engine V2</h1>
        <p>DATABASE_URL is not configured.</p>
        </body>
        </html>
        """

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            cur.execute(
                "SELECT cash, starting_cash, day_start_equity "
                "FROM portfolio WHERE id=1;"
            )
            portfolio = cur.fetchone()

            cash = float(portfolio["cash"]) if portfolio else INITIAL_CASH
            starting_cash = (
                float(portfolio["starting_cash"])
                if portfolio
                else INITIAL_CASH
            )
            day_start_equity = (
                float(portfolio["day_start_equity"])
                if portfolio
                else INITIAL_CASH
            )

            cur.execute("SELECT * FROM holdings ORDER BY symbol;")
            holdings = cur.fetchall()

            cur.execute(
                """
                SELECT timestamp, symbol, action, qty, price,
                       pnl_pct, pnl_value, balance, reason
                FROM ledger
                ORDER BY id DESC
                LIMIT 100;
                """
            )
            trades = cur.fetchall()

            cur.execute(
                """
                SELECT timestamp, symbol, status, price, reason
                FROM decision_logs
                ORDER BY id DESC
                LIMIT 100;
                """
            )
            decisions = cur.fetchall()

            cur.execute(
                """
                SELECT timestamp, total_value, cash
                FROM valuation_history
                ORDER BY id ASC;
                """
            )
            history = cur.fetchall()

    # Current prices for open positions.
    current_prices = {}
    holdings_value = 0.0

    for h in holdings:
        hist = get_market_data(h["symbol"])
        if hist is not None:
            ind = calculate_indicators(hist)
            if ind:
                current_prices[h["symbol"]] = ind["price"]

        price = current_prices.get(
            h["symbol"],
            float(h["buy_price"]),
        )

        holdings_value += int(h["qty"]) * price

    equity = round(cash + holdings_value, 2)
    total_return_pct = (
        ((equity - starting_cash) / starting_cash) * 100
        if starting_cash
        else 0
    )
    day_return_pct = (
        ((equity - day_start_equity) / day_start_equity) * 100
        if day_start_equity
        else 0
    )

    # Chart data.
    chart_labels = [
        to_ist_datetime_str(x["timestamp"])
        for x in history
    ]

    chart_values = [
        float(x["total_value"])
        for x in history
    ]

    if not chart_labels:
        chart_labels = ["Now"]
        chart_values = [equity]

    history_json = pd.Series(chart_values).to_json(orient="values")
    labels_json = (
        "[" + ",".join(
            '"' + str(x).replace('"', '\\"') + '"'
            for x in chart_labels
        ) + "]"
    )

    # HTML rows.
    holdings_rows = ""

    for h in holdings:
        symbol = h["symbol"]
        buy_price = float(h["buy_price"])
        current_price = current_prices.get(symbol, buy_price)
        qty = int(h["qty"])
        pnl_pct = ((current_price - buy_price) / buy_price) * 100

        holdings_rows += f"""
        <tr class="border-b border-slate-800">
            <td class="p-3 font-semibold">{symbol}</td>
            <td class="p-3">{qty}</td>
            <td class="p-3">₹{buy_price:.2f}</td>
            <td class="p-3">₹{current_price:.2f}</td>
            <td class="p-3 {'text-emerald-400' if pnl_pct >= 0 else 'text-rose-400'}">
                {pnl_pct:+.2f}%
            </td>
            <td class="p-3">₹{float(h['stop_price']):.2f}</td>
            <td class="p-3">₹{float(h['target_price']):.2f}</td>
        </tr>
        """

    if not holdings_rows:
        holdings_rows = """
        <tr>
            <td colspan="7" class="p-4 text-slate-500">
                No active positions
            </td>
        </tr>
        """

    activity = []

    for t in trades:
        activity.append(
            {
                "timestamp": t["timestamp"],
                "symbol": t["symbol"],
                "type": t["action"],
                "price": t["price"],
                "pnl": t["pnl_pct"],
                "reason": t["reason"] or "",
            }
        )

    for d in decisions:
        # Avoid duplicating executed trade rows in the activity table.
        if d["status"] in ("EXECUTED BUY", "EXECUTED SELL"):
            continue

        activity.append(
            {
                "timestamp": d["timestamp"],
                "symbol": d["symbol"] or "-",
                "type": d["status"],
                "price": d["price"],
                "pnl": None,
                "reason": d["reason"] or "",
            }
        )

    activity.sort(
        key=lambda x: x["timestamp"] or datetime.min,
        reverse=True,
    )

    activity = activity[:50]

    activity_rows = ""

    for row in activity:
        status = row["type"]

        if status == "BUY":
            badge = '<span class="text-emerald-400 font-bold">BUY</span>'
        elif status == "SELL":
            badge = '<span class="text-rose-400 font-bold">SELL</span>'
        elif status == "HOLD":
            badge = '<span class="text-amber-400 font-bold">HOLD</span>'
        elif status == "RISK HALT":
            badge = '<span class="text-red-500 font-bold">RISK HALT</span>'
        elif status == "NO SIGNAL":
            badge = '<span class="text-slate-400 font-semibold">NO SIGNAL</span>'
        else:
            badge = (
                '<span class="text-purple-400 font-semibold">'
                + str(status)
                + "</span>"
            )

        price = row["price"]
        price_display = (
            f"₹{float(price):.2f}"
            if price is not None and float(price) != 0
            else "-"
        )

        pnl_display = (
            f"{float(row['pnl']):+.2f}%"
            if row["pnl"] is not None
            else "-"
        )

        activity_rows += f"""
        <tr class="border-b border-slate-800">
            <td class="p-3 text-slate-400">
                {to_ist_datetime_str(row['timestamp'])}
            </td>
            <td class="p-3 font-semibold">{row['symbol']}</td>
            <td class="p-3">{badge}</td>
            <td class="p-3">{price_display}</td>
            <td class="p-3">{pnl_display}</td>
            <td class="p-3 text-xs text-slate-300">
                {row['reason']}
            </td>
        </tr>
        """

    if not activity_rows:
        activity_rows = """
        <tr>
            <td colspan="6" class="p-4 text-slate-500">
                Engine is waiting for market data...
            </td>
        </tr>
        """

    market_open, market_reason = is_market_open()

    market_badge = (
        '<span class="text-emerald-400 font-bold">● MARKET OPEN</span>'
        if market_open
        else '<span class="text-slate-400 font-bold">● MARKET CLOSED</span>'
    )

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="30">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quant Engine V2</title>

    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>

<body class="bg-slate-950 text-slate-100 min-h-screen p-4 md:p-8">

<div class="max-w-7xl mx-auto space-y-6">

    <header class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
        <div class="flex flex-col md:flex-row md:justify-between gap-4">
            <div>
                <h1 class="text-3xl font-black">
                    📈 Quant Engine V2
                </h1>
                <p class="text-slate-400 mt-2">
                    Multi-factor EMA + RSI + MACD + Volume + ATR paper strategy
                </p>
                <p class="mt-3">
                    {market_badge}
                    <span class="text-slate-500 ml-2">{market_reason}</span>
                </p>
            </div>

            <div class="text-left md:text-right">
                <p class="text-xs text-slate-400">Portfolio Equity</p>
                <p class="text-4xl font-black">₹{equity:.2f}</p>
                <p class="text-slate-400 mt-1">Cash ₹{cash:.2f}</p>
            </div>
        </div>
    </header>

    <section class="grid grid-cols-1 md:grid-cols-4 gap-4">

        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
            <p class="text-xs text-slate-400">Total Return</p>
            <p class="text-2xl font-bold mt-2">
                {total_return_pct:+.2f}%
            </p>
        </div>

        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
            <p class="text-xs text-slate-400">Today</p>
            <p class="text-2xl font-bold mt-2">
                {day_return_pct:+.2f}%
            </p>
        </div>

        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
            <p class="text-xs text-slate-400">Open Positions</p>
            <p class="text-2xl font-bold mt-2">
                {len(holdings)} / {MAX_POSITIONS}
            </p>
        </div>

        <div class="bg-slate-900 border border-slate-800 rounded-xl p-5">
            <p class="text-xs text-slate-400">Risk / Trade</p>
            <p class="text-2xl font-bold mt-2">
                {RISK_PER_TRADE * 100:.2f}%
            </p>
        </div>

    </section>

    <section class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
        <h2 class="text-xl font-bold mb-4">
            Portfolio Valuation
        </h2>

        <canvas id="equityChart" height="100"></canvas>
    </section>

    <section class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
        <h2 class="text-xl font-bold mb-4">
            Open Positions
        </h2>

        <div class="overflow-x-auto">
            <table class="w-full text-left text-sm">
                <thead>
                    <tr class="border-b border-slate-700 text-slate-400">
                        <th class="p-3">Symbol</th>
                        <th class="p-3">Qty</th>
                        <th class="p-3">Buy</th>
                        <th class="p-3">Current</th>
                        <th class="p-3">P&L</th>
                        <th class="p-3">Stop</th>
                        <th class="p-3">Target</th>
                    </tr>
                </thead>
                <tbody>
                    {holdings_rows}
                </tbody>
            </table>
        </div>
    </section>

    <section class="bg-slate-900 border border-slate-800 rounded-2xl p-6">
        <h2 class="text-xl font-bold mb-4">
            Activity & Execution Register
        </h2>

        <div class="overflow-x-auto">
            <table class="w-full text-left text-sm">
                <thead>
                    <tr class="border-b border-slate-700 text-slate-400">
                        <th class="p-3">Date & Time IST</th>
                        <th class="p-3">Symbol</th>
                        <th class="p-3">Status</th>
                        <th class="p-3">Price</th>
                        <th class="p-3">P&L</th>
                        <th class="p-3">Strategy / Reason</th>
                    </tr>
                </thead>
                <tbody>
                    {activity_rows}
                </tbody>
            </table>
        </div>
    </section>

    <footer class="text-center text-xs text-slate-600 py-4">
        PAPER TRADING ONLY • Quant Engine V2 • No broker orders are sent
    </footer>

</div>

<script>
const labels = {labels_json};
const values = {history_json};

new Chart(
    document.getElementById("equityChart").getContext("2d"),
    {{
        type: "line",
        data: {{
            labels: labels,
            datasets: [{{
                label: "Portfolio Equity (₹)",
                data: values,
                fill: true,
                tension: 0.25,
                borderWidth: 2
            }}]
        }},
        options: {{
            responsive: true,
            plugins: {{
                legend: {{
                    display: false
                }}
            }}
        }}
    }}
);
</script>

</body>
</html>
"""


# ============================================================
# Excel export
# ============================================================

@app.get("/export/excel")
def export_excel():
    if not DATABASE_URL:
        return HTMLResponse(
            "<h1>DATABASE_URL not configured</h1>",
            status_code=500,
        )

    with get_db() as conn:
        df = pd.read_sql(
            "SELECT * FROM ledger ORDER BY id ASC;",
            conn,
        )

    file_path = "/tmp/Trade_Register.xlsx"

    with pd.ExcelWriter(
        file_path,
        engine="openpyxl",
    ) as writer:
        df.to_excel(
            writer,
            index=False,
            sheet_name="Ledger",
        )

    return StreamingResponse(
        open(file_path, "rb"),
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition":
                "attachment; filename=Trade_Register.xlsx"
        },
    )


# ============================================================
# Health endpoint
# ============================================================

@app.get("/health")
def health():
    market_open, reason = is_market_open()

    return {
        "status": "ok",
        "mode": "paper",
        "market_open": market_open,
        "market_status": reason,
        "database_configured": bool(DATABASE_URL),
        "watchlist_size": len(WATCHLIST),
        "risk_per_trade": RISK_PER_TRADE,
        "max_positions": MAX_POSITIONS,
    }


# ============================================================
# Local development
# ============================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
'''

path = Path("/mnt/data/main.py")
path.write_text(code, encoding="utf-8")

print(f"Created {path}")
print(f"Lines: {len(code.splitlines())}")
