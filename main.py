import os
import asyncio
from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse

app = FastAPI(title="Indian Algo Trading Engine")

DATABASE_URL = os.getenv("DATABASE_URL")
INITIAL_CASH = 50000.0
WATCHLIST = [
    "SBIN.NS", "TATAMOTORS.NS", "ITC.NS", "INFY.NS", "RELIANCE.NS",
    "HDFCBANK.NS", "ICICIBANK.NS", "BHARTIARTL.NS", "TCS.NS", "AXISBANK.NS"
]

IST = timezone(timedelta(hours=5, minutes=30))

def to_ist_time_str(ts):
    if not ts:
        return "-"
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except Exception:
            return str(ts)[:8]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ist_ts = ts.astimezone(IST)
    return ist_ts.strftime("%H:%M:%S")

def is_market_open():
    now_ist = datetime.now(IST)
    if now_ist.weekday() >= 5:
        return False, f"Market Closed (Weekend - {now_ist.strftime('%A')})"
    start_time = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if start_time <= now_ist <= end_time:
        return True, "Market Open"
    return False, f"Market Closed (NSE/BSE hours: 09:15-15:30 IST)"

def get_db():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    if not DATABASE_URL:
        return
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio (
                    id INT PRIMARY KEY DEFAULT 1,
                    cash REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    action TEXT,
                    qty INT,
                    price REAL,
                    total_value REAL,
                    pnl_pct REAL,
                    balance REAL
                );
                CREATE TABLE IF NOT EXISTS holdings (
                    symbol TEXT PRIMARY KEY,
                    qty INT,
                    buy_price REAL
                );
                CREATE TABLE IF NOT EXISTS decision_logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    symbol TEXT,
                    price REAL,
                    status TEXT,
                    reason TEXT
                );
                CREATE TABLE IF NOT EXISTS valuation_history (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_value REAL
                );
                INSERT INTO portfolio (id, cash) VALUES (1, %s) ON CONFLICT DO NOTHING;
            """, (INITIAL_CASH,))
            conn.commit()

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(trading_loop())

def fetch_live_prices(symbols):
    prices = {}
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="2d")
            if not hist.empty:
                prices[sym] = round(float(hist['Close'].iloc[-1]), 2)
        except Exception as e:
            print(f"Failed to fetch {sym}: {e}")
    return prices

async def trading_loop():
    while True:
        try:
            if DATABASE_URL:
                market_open, market_reason = is_market_open()
                if not market_open:
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, %s, %s);",
                                ("NSE/BSE", 0.0, "MARKET CLOSED", market_reason)
                            )
                            cur.execute("DELETE FROM decision_logs WHERE id NOT IN (SELECT id FROM decision_logs ORDER BY id DESC LIMIT 100);")
                            conn.commit()
                else:
                    prices = fetch_live_prices(WATCHLIST)
                    with get_db() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("SELECT cash FROM portfolio WHERE id=1;")
                            cash_res = cur.fetchone()
                            cash = cash_res['cash'] if cash_res else INITIAL_CASH

                            holdings_value = 0.0

                            for sym, price in prices.items():
                                if not price or price <= 0:
                                    continue

                                cur.execute("SELECT * FROM holdings WHERE symbol=%s;", (sym,))
                                position = cur.fetchone()

                                if position:
                                    buy_price = position['buy_price']
                                    qty = position['qty']
                                    holdings_value += (qty * price)
                                    pnl_pct = ((price - buy_price) / buy_price) * 100.0

                                    if pnl_pct >= 1.5 or pnl_pct <= -1.0:
                                        revenue = round(qty * price, 2)
                                        cash += revenue

                                        cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                        cur.execute("DELETE FROM holdings WHERE symbol=%s;", (sym,))
                                        cur.execute("""
                                            INSERT INTO ledger (symbol, action, qty, price, total_value, pnl_pct, balance)
                                            VALUES (%s, 'SELL', %s, %s, %s, %s, %s);
                                        """, (sym, qty, price, revenue, pnl_pct, cash))
                                        
                                        reason = f"TARGET HIT ({'PROFIT +1.5%' if pnl_pct>=1.5 else 'STOP LOSS -1.0%'})"
                                        cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'EXECUTED SELL', %s);", (sym, price, reason))
                                    else:
                                        reason = f"HOLDING: P&L is {pnl_pct:+.2f}% (Target: +1.5% / -1.0%)"
                                        cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'HOLD', %s);", (sym, price, reason))

                                else:
                                    max_qty = int(cash // price)
                                    if max_qty > 0:
                                        qty = 1
                                        cost = round(qty * price, 2)
                                        cash -= cost

                                        cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                        cur.execute("INSERT INTO holdings (symbol, qty, buy_price) VALUES (%s, %s, %s);", (sym, qty, price))
                                        cur.execute("""
                                            INSERT INTO ledger (symbol, action, qty, price, total_value, balance)
                                            VALUES (%s, 'BUY', %s, %s, %s, %s);
                                        """, (sym, qty, price, cost, cash))

                                        reason = f"APPROVED: Bought {qty} qty at ₹{price:.2f}"
                                        cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'EXECUTED BUY', %s);", (sym, price, reason))
                                    else:
                                        reason = f"REJECTED: Insufficient cash balance (₹{cash:.2f}) for stock price ₹{price:.2f}"
                                        cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'REJECTED', %s);", (sym, price, reason))

                            total_portfolio_valuation = round(cash + holdings_value, 2)
                            cur.execute("INSERT INTO valuation_history (total_value) VALUES (%s);", (total_portfolio_valuation,))

                            cur.execute("DELETE FROM decision_logs WHERE id NOT IN (SELECT id FROM decision_logs ORDER BY id DESC LIMIT 100);")
                            cur.execute("DELETE FROM valuation_history WHERE id NOT IN (SELECT id FROM valuation_history ORDER BY id DESC LIMIT 100);")
                            conn.commit()

        except Exception as e:
            print(f"Trading loop exception: {e}")

        await asyncio.sleep(15)

@app.get("/", response_class=HTMLResponse)
def web_dashboard():
    if not DATABASE_URL:
        return "<h1>Database URL Not Configured</h1>"

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT cash FROM portfolio WHERE id=1;")
            cash_row = cur.fetchone()
            cash = cash_row['cash'] if cash_row else INITIAL_CASH
            
            cur.execute("SELECT * FROM holdings;")
            holdings = cur.fetchall()

            cur.execute("SELECT timestamp, symbol, action AS type, qty, price, pnl_pct, balance, '' AS reason FROM ledger ORDER BY id DESC LIMIT 40;")
            ledger_logs = cur.fetchall()

            cur.execute("SELECT timestamp, symbol, status AS type, 0 AS qty, price, NULL AS pnl_pct, NULL AS balance, reason FROM decision_logs WHERE status IN ('HOLD', 'REJECTED', 'MARKET CLOSED') ORDER BY id DESC LIMIT 40;")
            decision_logs_data = cur.fetchall()

            cur.execute("SELECT timestamp, total_value FROM valuation_history ORDER BY id ASC;")
            history = cur.fetchall()

            cur.execute("SELECT timestamp, pnl_pct, qty, price FROM ledger WHERE action='SELL' ORDER BY id ASC;")
            sell_trades = cur.fetchall()

    # Combine ledger and decision logs into one register sorted by timestamp IST
    unified_logs = sorted(ledger_logs + decision_logs_data, key=lambda x: x['timestamp'], reverse=True)[:35]

    # Chart 1: Valuation
    chart_labels = [to_ist_time_str(h['timestamp']) for h in history] if history else ["Now"]
    chart_values = [h['total_value'] for h in history] if history else [INITIAL_CASH]

    # Chart 2: Cumulative Platform Profit
    cum_profit = 0.0
    pnl_labels = []
    pnl_values = []
    for st in sell_trades:
        time_str = to_ist_time_str(st['timestamp'])
        pnl_pct = st['pnl_pct'] or 0.0
        qty = st['qty'] or 1
        price = st['price'] or 0.0
        buy_price = price / (1 + pnl_pct / 100.0) if (1 + pnl_pct / 100.0) != 0 else price
        trade_pnl = (price - buy_price) * qty
        cum_profit += trade_pnl
        pnl_labels.append(time_str)
        pnl_values.append(round(cum_profit, 2))

    if not pnl_labels:
        pnl_labels = ["Now"]
        pnl_values = [0.0]

    holdings_rows = "".join([f"<tr class='border-b border-slate-800'><td class='p-2 font-semibold'>{h['symbol']}</td><td class='p-2'>{h['qty']}</td><td class='p-2'>₹{h['buy_price']}</td></tr>" for h in holdings])

    unified_rows = []
    for log in unified_logs:
        t_str = to_ist_time_str(log['timestamp'])
        log_type = log['type']
        
        if log_type == 'BUY':
            badge = "<span class='text-emerald-400 font-semibold'>EXECUTED BUY</span>"
            details = f"Bought {log['qty']} qty @ ₹{log['price']:.2f}"
            pnl_disp = "-"
        elif log_type == 'SELL':
            badge = "<span class='text-rose-400 font-semibold'>EXECUTED SELL</span>"
            pnl_val = log['pnl_pct'] or 0.0
            pnl_disp = f"{pnl_val:+.2f}%"
            details = f"Sold {log['qty']} qty @ ₹{log['price']:.2f}"
        elif log_type == 'HOLD':
            badge = "<span class='text-amber-400 font-semibold'>HOLD</span>"
            pnl_disp = "-"
            details = log['reason']
        elif log_type == 'REJECTED':
            badge = "<span class='text-rose-500 font-semibold'>REJECTED</span>"
            pnl_disp = "-"
            details = log['reason']
        else: # MARKET CLOSED
            badge = "<span class='text-slate-400 font-semibold'>MARKET CLOSED</span>"
            pnl_disp = "-"
            details = log['reason']

        price_disp = f"₹{log['price']:.2f}" if log['price'] else "-"
        
        unified_rows.append(
            f"<tr class='border-b border-slate-800'>"
            f"<td class='p-2 text-slate-400'>{t_str}</td>"
            f"<td class='p-2 font-semibold'>{log['symbol']}</td>"
            f"<td class='p-2'>{badge}</td>"
            f"<td class='p-2'>{price_disp}</td>"
            f"<td class='p-2'>{pnl_disp}</td>"
            f"<td class='p-2 text-xs text-slate-300'>{details}</td>"
            f"</tr>"
        )

    unified_rows_html = "".join(unified_rows)

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Live Algo Trading Engine</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <meta http-equiv="refresh" content="10">
    </head>
    <body class="bg-slate-950 text-slate-100 p-6 font-sans">
        <div class="max-w-6xl mx-auto space-y-6">
            <div class="flex justify-between items-center bg-slate-900 p-6 rounded-xl border border-slate-800">
                <div>
                    <h1 class="text-2xl font-bold text-emerald-400">Live Trade Action Register</h1>
                    <p class="text-slate-400 text-sm">Status: IST Market Hours Active (09:15 AM - 03:30 PM) | Live Tracking</p>
                </div>
                <div class="flex items-center gap-6">
                    <div class="text-right">
                        <div class="text-xs text-slate-400">Available Cash</div>
                        <div class="text-3xl font-extrabold text-white">₹{cash:.2f}</div>
                    </div>
                </div>
            </div>

            <div class="flex gap-4">
                <a href="/export/excel" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 px-4 rounded-lg">📊 Download Excel Register</a>
                <a href="/export/csv" class="bg-blue-600 hover:bg-blue-500 text-white font-bold py-2 px-4 rounded-lg">📄 Download CSV Report</a>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-emerald-400">Live Portfolio Valuation Curve (Cash + Stocks)</h2>
                    <canvas id="balanceChart" height="140"></canvas>
                </div>
                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-blue-400">Platform Realized Profit Curve (Cumulative ₹)</h2>
                    <canvas id="pnlChart" height="140"></canvas>
                </div>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-amber-400">Unified Activity & Execution Register (Executions, Holds & Rejections)</h2>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm text-slate-300">
                        <tr class="border-b border-slate-800 text-slate-400">
                            <th>Time (IST)</th><th>Symbol</th><th>Action / Status</th><th>Price</th><th>P&L (%)</th><th>Analysis / Reason</th>
                        </tr>
                        {unified_rows_html if unified_rows_html else "<tr><td colspan='6' class='py-2 text-slate-500'>Evaluating watchlist rules...</td></tr>"}
                    </table>
                </div>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-emerald-400">Open Positions</h2>
                <table class="w-full text-left text-sm text-slate-300">
                    <tr class="border-b border-slate-800 text-slate-400"><th>Symbol</th><th>Qty</th><th>Buy Price</th></tr>
                    {holdings_rows if holdings_rows else "<tr><td colspan='3' class='py-2 text-slate-500'>No open positions</td></tr>"}
                </table>
            </div>

            <div class="mt-8 text-center text-slate-500 text-sm font-medium py-4 border-t border-slate-800">
                Made by Imtex
            </div>
        </div>

        <script>
            const ctx1 = document.getElementById('balanceChart').getContext('2d');
            new Chart(ctx1, {{
                type: 'line',
                data: {{
                    labels: {chart_labels},
                    datasets: [{{
                        label: 'Portfolio Valuation (₹)',
                        data: {chart_values},
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        fill: true,
                        tension: 0.2,
                        pointRadius: 2
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ display: false }} }},
                    scales: {{
                        x: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#94a3b8' }} }},
                        y: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#94a3b8' }} }}
                    }}
                }}
            }});

            const ctx2 = document.getElementById('pnlChart').getContext('2d');
            new Chart(ctx2, {{
                type: 'line',
                data: {{
                    labels: {pnl_labels},
                    datasets: [{{
                        label: 'Realized Profit (₹)',
                        data: {pnl_values},
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        fill: true,
                        tension: 0.2,
                        pointRadius: 2
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ legend: {{ display: false }} }},
                    scales: {{
                        x: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#94a3b8' }} }},
                        y: {{ grid: {{ color: '#1e293b' }}, ticks: {{ color: '#94a3b8' }} }}
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """

@app.get("/export/excel")
def export_excel():
    with get_db() as conn:
        df = pd.read_sql("SELECT * FROM ledger ORDER BY id ASC;", conn)
    file_path = "/tmp/Trade_Register.xlsx"
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Ledger")
    return StreamingResponse(
        open(file_path, "rb"),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=Trade_Register.xlsx"}
    )

@app.get("/export/csv")
def export_csv():
    with get_db() as conn:
        df = pd.read_sql("SELECT * FROM ledger ORDER BY id ASC;", conn)
    file_path = "/tmp/Trade_Register.csv"
    df.to_csv(file_path, index=False)
    return StreamingResponse(
        open(file_path, "rb"),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=Trade_Register.csv"}
    )
