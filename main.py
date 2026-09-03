import os
import asyncio
import json
import urllib.request
from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI(title="AI-Supervised Indian Algo Trading Engine")

DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
INITIAL_CASH = 50000.0
WATCHLIST = [
    "SBIN.NS", "TATAMOTORS.NS", "ITC.NS", "INFY.NS", "RELIANCE.NS",
    "HDFCBANK.NS", "ICICIBANK.NS", "BHARTIARTL.NS", "TCS.NS", "AXISBANK.NS"
]

IST = timezone(timedelta(hours=5, minutes=30))

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
    ist_ts = ts.astimezone(IST)
    return ist_ts.strftime("%d-%b %H:%M:%S")

def is_market_open():
    now_ist = datetime.now(IST)
    if now_ist.weekday() >= 5:
        return False, f"Market Closed (Weekend - {now_ist.strftime('%A')})"
    start_time = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    if start_time <= now_ist <= end_time:
        return True, "Market Open"
    return False, f"Market Closed (NSE/BSE hours: 09:15-15:30 IST)"

def calculate_technical_indicators(df):
    if len(df) < 25:
        return None
    close = df['Close']
    ema9 = close.ewm(span=9, adjust=False).mean().iloc[-1]
    ema21 = close.ewm(span=21, adjust=False).mean().iloc[-1]
    
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    rsi = 100 - (100 / (1 + rs)).iloc[-1]
    
    return {
        "price": round(float(close.iloc[-1]), 2),
        "ema9": round(float(ema9), 2),
        "ema21": round(float(ema21), 2),
        "rsi": round(float(rsi), 2)
    }

def consult_ai_supervisor(symbol, price, proposed_action, cash, indicators, pnl_pct=0.0):
    if not GEMINI_API_KEY:
        return True, "AI Supervisor: API Key missing, strategy execution approved."
    
    prompt = f"""
    You are an expert AI Algorithmic Risk Supervisor for Indian Stock Markets (NSE/BSE).
    Evaluate this proposed quantitative trade:
    - Symbol: {symbol}
    - Current Price: ₹{price}
    - Proposed Action: {proposed_action}
    - Technical Context: EMA9=₹{indicators.get('ema9')}, EMA21=₹{indicators.get('ema21')}, RSI={indicators.get('rsi')}
    - Current Position P&L (%): {pnl_pct:.2f}%
    - Available Portfolio Cash: ₹{cash:.2f}

    Evaluate market conditions and risk parameters.
    Reply ONLY in valid JSON format:
    {{"approved": true/false, "reason": "Short concise reason (max 15 words)"}}
    """
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "response_mime_type": "application/json"}
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            res_data = json.loads(response.read().decode("utf-8"))
            text_response = res_data["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(text_response)
            return parsed.get("approved", True), f"AI Supervisor: {parsed.get('reason', 'Approved')}"
    except Exception as e:
        return True, f"AI Supervisor (Fallback): Passed ({str(e)[:25]})"

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

async def trading_loop():
    while True:
        try:
            if DATABASE_URL:
                market_open, market_reason = is_market_open()
                
                # --- FIX: PREVENT MARKET CLOSED LOG SPAM ---
                if not market_open:
                    with get_db() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("SELECT status FROM decision_logs ORDER BY id DESC LIMIT 1;")
                            last_log = cur.fetchone()
                            if not last_log or last_log['status'] != 'MARKET CLOSED':
                                cur.execute(
                                    "INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, %s, %s);",
                                    ("NSE/BSE", 0.0, "MARKET CLOSED", market_reason)
                                )
                                conn.commit()
                    # Sleep 5 minutes when market is closed to save resources
                    await asyncio.sleep(300)
                    continue

                # --- ACTIVE TRADING LOGIC ---
                with get_db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT cash FROM portfolio WHERE id=1;")
                        cash_res = cur.fetchone()
                        cash = cash_res['cash'] if cash_res else INITIAL_CASH
                        holdings_value = 0.0

                        for sym in WATCHLIST:
                            try:
                                ticker = yf.Ticker(sym)
                                hist = ticker.history(period="1mo", interval="15m")
                                if hist.empty or len(hist) < 25:
                                    continue
                                
                                ind = calculate_technical_indicators(hist)
                                if not ind:
                                    continue
                                
                                price = ind['price']
                                cur.execute("SELECT * FROM holdings WHERE symbol=%s;", (sym,))
                                position = cur.fetchone()

                                if position:
                                    buy_price = position['buy_price']
                                    qty = position['qty']
                                    holdings_value += (qty * price)
                                    pnl_pct = ((price - buy_price) / buy_price) * 100.0

                                    # Exit Conditions: Take profit at +2.5%, Stop loss at -1.2%, or Trend Reversal
                                    sell_signal = (pnl_pct >= 2.5) or (pnl_pct <= -1.2) or (ind['ema9'] < ind['ema21'])

                                    if sell_signal:
                                        target_type = "SELL (TAKE PROFIT)" if pnl_pct >= 2.5 else ("SELL (STOP LOSS)" if pnl_pct <= -1.2 else "SELL (TREND REVERSAL)")
                                        ai_approved, ai_reason = consult_ai_supervisor(sym, price, target_type, cash, ind, pnl_pct)

                                        if ai_approved:
                                            revenue = round(qty * price, 2)
                                            cash += revenue
                                            cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                            cur.execute("DELETE FROM holdings WHERE symbol=%s;", (sym,))
                                            cur.execute("""
                                                INSERT INTO ledger (symbol, action, qty, price, total_value, pnl_pct, balance)
                                                VALUES (%s, 'SELL', %s, %s, %s, %s, %s);
                                            """, (sym, qty, price, revenue, pnl_pct, cash))
                                            cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'EXECUTED SELL', %s);", (sym, price, ai_reason))
                                        else:
                                            cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'AI VETOED SELL', %s);", (sym, price, ai_reason))
                                    else:
                                        reason = f"HOLDING: P&L {pnl_pct:+.2f}% | EMA9: ₹{ind['ema9']} / EMA21: ₹{ind['ema21']}"
                                        cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'HOLD', %s);", (sym, price, reason))

                                else:
                                    # Entry Conditions: EMA Crossover + Momentum RSI (< 65)
                                    buy_signal = (ind['ema9'] > ind['ema21']) and (ind['rsi'] < 65)
                                    if buy_signal:
                                        # Allocate up to 25% of available cash per trade for higher compounding growth
                                        trade_allocation = cash * 0.25
                                        qty = int(trade_allocation // price)

                                        if qty >= 1:
                                            ai_approved, ai_reason = consult_ai_supervisor(sym, price, "BUY", cash, ind)
                                            if ai_approved:
                                                cost = round(qty * price, 2)
                                                cash -= cost
                                                cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                                cur.execute("INSERT INTO holdings (symbol, qty, buy_price) VALUES (%s, %s, %s);", (sym, qty, price))
                                                cur.execute("""
                                                    INSERT INTO ledger (symbol, action, qty, price, total_value, balance)
                                                    VALUES (%s, 'BUY', %s, %s, %s, %s);
                                                """, (sym, qty, price, cost, cash))
                                                cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'EXECUTED BUY', %s);", (sym, price, ai_reason))
                                            else:
                                                cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'AI VETOED BUY', %s);", (sym, price, ai_reason))
                                        else:
                                            reason = f"INSUFFICIENT FUNDS: Required ₹{price:.2f}, Cash Available ₹{cash:.2f}"
                                            cur.execute("INSERT INTO decision_logs (symbol, price, status, reason) VALUES (%s, %s, 'REJECTED', %s);", (sym, price, reason))

                            except Exception as ex:
                                print(f"Error processing {sym}: {ex}")

                        total_portfolio_valuation = round(cash + holdings_value, 2)
                        cur.execute("INSERT INTO valuation_history (total_value) VALUES (%s);", (total_portfolio_valuation,))
                        cur.execute("DELETE FROM decision_logs WHERE id NOT IN (SELECT id FROM decision_logs ORDER BY id DESC LIMIT 100);")
                        cur.execute("DELETE FROM valuation_history WHERE id NOT IN (SELECT id FROM valuation_history ORDER BY id DESC LIMIT 100);")
                        conn.commit()

        except Exception as e:
            print(f"Trading loop exception: {e}")

        await asyncio.sleep(20)

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

            cur.execute("SELECT timestamp, symbol, action, qty, price, pnl_pct FROM ledger ORDER BY id ASC;")
            all_trades = cur.fetchall()

            cur.execute("SELECT timestamp, symbol, action AS type, qty, price, pnl_pct, balance, '' AS reason FROM ledger ORDER BY id DESC LIMIT 30;")
            ledger_logs = cur.fetchall()

            cur.execute("SELECT timestamp, symbol, status AS type, 0 AS qty, price, NULL AS pnl_pct, NULL AS balance, reason FROM decision_logs WHERE status NOT IN ('EXECUTED BUY', 'EXECUTED SELL') ORDER BY id DESC LIMIT 30;")
            decision_logs_data = cur.fetchall()

            cur.execute("SELECT timestamp, total_value FROM valuation_history ORDER BY id ASC;")
            history = cur.fetchall()

    unified_logs = sorted(ledger_logs + decision_logs_data, key=x: x['timestamp'], reverse=True)[:35]

    # Date and Time charts formatting
    chart_labels = [to_ist_datetime_str(h['timestamp']) for h in history] if history else ["Now"]
    chart_values = [h['total_value'] for h in history] if history else [INITIAL_CASH]

    # Buy / Sell Chart scatter points setup
    buy_scatter = []
    sell_scatter = []
    cum_profit = 0.0
    pnl_labels = []
    pnl_values = []

    for tr in all_trades:
        ts_str = to_ist_datetime_str(tr['timestamp'])
        point = {"x": ts_str, "y": tr['price'], "symbol": tr['symbol'], "qty": tr['qty']}
        if tr['action'] == 'BUY':
            buy_scatter.append(point)
        else:
            sell_scatter.append(point)
            pnl_pct = tr['pnl_pct'] or 0.0
            qty = tr['qty'] or 1
            price = tr['price'] or 0.0
            buy_price = price / (1 + pnl_pct / 100.0) if (1 + pnl_pct / 100.0) != 0 else price
            trade_pnl = (price - buy_price) * qty
            cum_profit += trade_pnl
            pnl_labels.append(ts_str)
            pnl_values.append(round(cum_profit, 2))

    if not pnl_labels:
        pnl_labels = ["Now"]
        pnl_values = [0.0]

    holdings_rows = "".join([f"<tr class='border-b border-slate-800'><td class='p-2 font-semibold'>{h['symbol']}</td><td class='p-2'>{h['qty']}</td><td class='p-2'>₹{h['buy_price']}</td></tr>" for h in holdings])

    unified_rows = []
    for log in unified_logs:
        t_str = to_ist_datetime_str(log['timestamp'])
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
        elif 'AI VETOED' in log_type:
            badge = "<span class='text-purple-400 font-semibold'>AI VETOED</span>"
            pnl_disp = "-"
            details = log['reason']
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
        <title>Quant Engine - AI Supervised</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <meta http-equiv="refresh" content="10">
    </head>
    <body class="bg-slate-950 text-slate-100 p-6 font-sans">
        <div class="max-w-6xl mx-auto space-y-6">
            <div class="flex justify-between items-center bg-slate-900 p-6 rounded-xl border border-slate-800">
                <div>
                    <h1 class="text-2xl font-bold text-emerald-400 flex items-center gap-2">
                        <span>📈 Quant Algo Engine (EMA + RSI Strategy)</span>
                    </h1>
                    <p class="text-slate-400 text-sm">Trading Hours: 09:15-15:30 IST | Gemini Risk Guardrails Active</p>
                </div>
                <div class="text-right">
                    <div class="text-xs text-slate-400">Available Cash</div>
                    <div class="text-3xl font-extrabold text-white">₹{cash:.2f}</div>
                </div>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-emerald-400">Portfolio Valuation Curve (IST)</h2>
                    <canvas id="balanceChart" height="140"></canvas>
                </div>
                <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                    <h2 class="text-lg font-semibold mb-4 text-blue-400">Realized Cumulative Profit (₹)</h2>
                    <canvas id="pnlChart" height="140"></canvas>
                </div>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-purple-400">Buy & Sell Executions (Date & Time IST)</h2>
                <canvas id="tradesChart" height="110"></canvas>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-amber-400">Activity & Execution Register</h2>
                <div class="overflow-x-auto">
                    <table class="w-full text-left text-sm text-slate-300">
                        <tr class="border-b border-slate-800 text-slate-400">
                            <th>Date & Time (IST)</th><th>Symbol</th><th>Action / Status</th><th>Price</th><th>P&L (%)</th><th>Strategy & AI Analysis</th>
                        </tr>
                        {unified_rows_html if unified_rows_html else "<tr><td colspan='6' class='py-2 text-slate-500'>Evaluating strategy...</td></tr>"}
                    </table>
                </div>
            </div>

            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <h2 class="text-lg font-semibold mb-4 text-emerald-400">Open Positions</h2>
                <table class="w-full text-left text-sm text-slate-300">
                    <tr class="border-b border-slate-800 text-slate-400"><th>Symbol</th><th>Qty</th><th>Buy Price</th></tr>
                    {holdings_rows if holdings_rows else "<tr><td colspan='3' class='py-2 text-slate-500'>No active positions</td></tr>"}
                </table>
            </div>
        </div>

        <script>
            new Chart(document.getElementById('balanceChart').getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {chart_labels},
                    datasets: [{{
                        label: 'Valuation (₹)',
                        data: {chart_values},
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        fill: true,
                        tension: 0.2
                    }}]
                }},
                options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }} }}
            }});

            new Chart(document.getElementById('pnlChart').getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: {pnl_labels},
                    datasets: [{{
                        label: 'Realized Profit (₹)',
                        data: {pnl_values},
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        fill: true,
                        tension: 0.2
                    }}]
                }},
                options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }} }}
            }});

            const buyPoints = {json.dumps(buy_scatter)};
            const sellPoints = {json.dumps(sell_scatter)};

            new Chart(document.getElementById('tradesChart').getContext('2d'), {{
                type: 'scatter',
                data: {{
                    datasets: [
                        {{
                            label: 'BUY Executions',
                            data: buyPoints,
                            backgroundColor: '#10b981',
                            pointRadius: 6
                        }},
                        {{
                            label: 'SELL Executions',
                            data: sellPoints,
                            backgroundColor: '#f43f5e',
                            pointRadius: 6
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{
                        tooltip: {{
                            callbacks: {{
                                label: function(ctx) {{
                                    let p = ctx.raw;
                                    return p.symbol + ' | Qty: ' + p.qty + ' @ ₹' + p.y + ' (' + p.x + ')';
                                }}
                            }}
                        }}
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
