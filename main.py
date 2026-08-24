import os
import asyncio
from datetime import datetime
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
            cur.execute("SELECT * FROM ledger ORDER BY id DESC LIMIT 25;")
            ledger = cur.fetchall()
            cur.execute("SELECT * FROM decision_logs ORDER BY id DESC LIMIT 15;")
            decisions = cur.fetchall()
            cur.execute("SELECT timestamp, total_value FROM valuation_history ORDER BY id ASC;")
            history = cur.fetchall()

    chart_labels = [str(h['timestamp']).split()[1][:8] for h in history] if history else ["Now"]
    chart_values = [h['total_value'] for h in history] if history else [INITIAL_CASH]

    holdings_rows = "".join([f"<tr class='border-b border-slate-800'><td class='p-2 font-semibold'>{h['symbol']}</td><td class='p-2'>{h['qty']}</td><td class='p-2'>₹{h['buy_price']}</td></tr>" for h in holdings])
    ledger_rows = "".join([
        f"<tr class='border-b border-slate-800'><td class='p-2 text-slate-400'>{str(l['timestamp']).split()[1][:8]}</td><td class='p-2 font-semibold'>{l['symbol']}</td><td class='p-2 {('text-emerald-400' if l['action']=='BUY' else 'text-rose-400')}'>{l['action']}</td><td class='p-2'>{l['qty']}</td><td class='p-2'>₹{l['price']}</td><td class='p-2'>{(str(round(l['pnl_pct'],2))+'%') if l['pnl_pct'] is not None else '-'}</td><td class='p-2 font-mono'>₹{l['balance']:.2f}</td></tr>"
        for l in ledger
    ])
    decision_rows = "".join([
        f"<tr class='border-b border-slate-800'><td class='p-2 text-slate-400'>{str(d['timestamp']).split()[1][:8]}</td><td class='p-2 font-semibold'>{d['symbol']}</td><td class='p-2'>₹{d['price']}</td><td class='p-2 {('text-rose-400' if 'REJECTED' in d['status'] else 'text-amber-400' if 'HOLD' in d['status'] else 'text-emerald-400')}'>{d['status']}</td><td class='p-2 text-xs text-slate-300'>{d['reason']}</td></tr>"
        for d in decisions
    ])

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
                    <p class="text-slate-400 text-sm">Status: Scanning Live | Continuous Graph Tracking Active</p>
                </div>
                <div class="flex items-center gap-6">
                    <div class="text-right">
                        <div class="text-xs text-slate-400">Available Cash</div>
                        <div class="text-3xl font-extrabold text-white">₹{cash:.2f}</div>
                    </div>
                    <a href="/reset-cash" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold py-2 px-3 rounded-lg border border-slate-700">🔄 Reset Cash (₹50k)</a>
                </div>
            </div>

            <div class="flex gap-4">
                <a href="/export/excel" class="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 px-4 rounded-lg">📊 Download Excel Register</a>
                <a href="/export/csv" class="bg-blue-600 hover:bg-blue-500 text-white font-bold py-2 px-4 rounded-lg">📄 Download CSV Report</a>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-emerald-400">Live Portfolio Valuation Curve (Cash + Stocks)</h2>
                <canvas id="balanceChart" height="80"></canvas>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-amber-400">Live Decision & Rejection Register</h2>
                <table class="w-full text-left text-sm text-slate-300">
                    <tr class="border-b border-slate-800 text-slate-400"><th>Time</th><th>Symbol</th><th>Price</th><th>Status</th><th>Reason / Analysis</th></tr>
                    {decision_rows if decision_rows else "<tr><td colspan='5' class='py-2 text-slate-500'>Evaluating watchlist rules...</td></tr>"}
                </table>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-emerald-400">Open Positions</h2>
                    <table class="w-full text-left text-sm text-slate-300">
                        <tr class="border-b border-slate-800 text-slate-400"><th>Symbol</th><th>Qty</th><th>Buy Price</th></tr>
                        {holdings_rows if holdings_rows else "<tr><td colspan='3' class='py-2 text-slate-500'>No open positions</td></tr>"}
                    </table>
                </div>

                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-emerald-400">Executed Trade Ledger</h2>
                    <table class="w-full text-left text-sm text-slate-300">
                        <tr class="border-b border-slate-800 text-slate-400"><th>Time</th><th>Symbol</th><th>Action</th><th>Qty</th><th>Price</th><th>P&L</th><th>Balance</th></tr>
                        {ledger_rows if ledger_rows else "<tr><td colspan='7' class='py-2 text-slate-500'>No executed trades yet</td></tr>"}
                    </table>
                </div>
            </div>
        </div>

        <script>
            const ctx = document.getElementById('balanceChart').getContext('2d');
            new Chart(ctx, {{
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
        </script>
    </body>
    </html>
    """

@app.get("/reset-cash")
def reset_cash():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (INITIAL_CASH,))
            cur.execute("DELETE FROM holdings;")
            conn.commit()
    return RedirectResponse(url="/")

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
