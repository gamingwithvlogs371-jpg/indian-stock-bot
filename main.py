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
from fastapi.responses import HTMLResponse

app = FastAPI(title="30-Stock High Yield Quant Engine")

DATABASE_URL = os.getenv("postgresql://postgres:[YOUR-PASSWORD]@db.wohofakibdlyzmooxhkz.supabase.co:5432/postgres")
GROQ_API_KEY = os.getenv("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")
INITIAL_CASH = 50000.0
# 30 High-Liquidity Stocks
WATCHLIST = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS",
    "BHARTIARTL.NS", "SBIN.NS", "ITC.NS", "LT.NS", "AXISBANK.NS",
    "KOTAKBANK.NS", "HINDUNILVR.NS", "BAJFINANCE.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "TATASTEEL.NS", "NTPC.NS", "M&M.NS", "POWERGRID.NS",
    "ULTRACEMCO.NS", "TECHM.NS", "ADANIENT.NS", "HCLTECH.NS", "WIPRO.NS",
    "ONGC.NS", "COALINDIA.NS", "JSWSTEEL.NS", "TATAMOTORS.NS", "ASIANPAINT.NS"
]

IST = timezone(timedelta(hours=5, minutes=30))

def calculate_indicators(df):
    if len(df) < 15:
        return None
    close = df['Close']
    volume = df['Volume']
    
    ema5 = close.ewm(span=5, adjust=False).mean().iloc[-1]
    ema13 = close.ewm(span=13, adjust=False).mean().iloc[-1]
    
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    rsi = 100 - (100 / (1 + rs)).iloc[-1]
    
    avg_vol = volume.rolling(window=10).mean().iloc[-1]
    volume_surge = volume.iloc[-1] > (1.1 * avg_vol) if avg_vol > 0 else True

    return {
        "price": round(float(close.iloc[-1]), 2),
        "ema5": round(float(ema5), 2),
        "ema13": round(float(ema13), 2),
        "rsi": round(float(rsi), 2),
        "volume_surge": bool(volume_surge)
    }

def consult_ai_groq(symbol, price, proposed_action, cash, indicators, pnl_pct=0.0):
    """Uses Groq API (Free tier: Llama-3.1-8b) - Zero cost, fast execution."""
    if not GROQ_API_KEY:
        return True, "AI Guardrail: No GROQ_API_KEY provided; auto-executed."

    prompt = f"""
    You are an AI Trading Risk Supervisor.
    Evaluate trade: Symbol={symbol}, Price={price}, Action={proposed_action}, Cash=₹{cash:.2f}, PnL={pnl_pct:.2f}%.
    Indicators: EMA5={indicators['ema5']}, EMA13={indicators['ema13']}, RSI={indicators['rsi']}.
    Reply ONLY with JSON: {{"approved": true/false, "reason": "Short reason (max 10 words)"}}
    """
    
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = json.dumps({
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=5) as response:
            res_data = json.loads(response.read().decode("utf-8"))
            parsed = json.loads(res_data["choices"][0]["message"]["content"])
            return parsed.get("approved", True), f"Groq AI: {parsed.get('reason', 'Approved')}"
    except Exception as e:
        return True, f"Groq AI Fallback: Passed ({str(e)[:20]})"

def get_db():
    return psycopg2.connect(DATABASE_URL)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(trading_loop())

async def trading_loop():
    while True:
        try:
            if DATABASE_URL:
                # Batch fetch 30 stocks in a single API call to save bandwidth
                data = yf.download(tickers=WATCHLIST, period="5d", interval="15m", group_by="ticker", progress=False)

                with get_db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT cash FROM portfolio WHERE id=1;")
                        cash_res = cur.fetchone()
                        cash = cash_res['cash'] if cash_res else INITIAL_CASH

                        for sym in WATCHLIST:
                            try:
                                df_sym = data[sym] if sym in data else None
                                if df_sym is None or df_sym.empty or len(df_sym.dropna()) < 15:
                                    continue

                                ind = calculate_indicators(df_sym.dropna())
                                if not ind:
                                    continue

                                price = ind['price']
                                cur.execute("SELECT * FROM holdings WHERE symbol=%s;", (sym,))
                                position = cur.fetchone()

                                if position:
                                    buy_price = position['buy_price']
                                    qty = position['qty']
                                    peak = max(position.get('peak_price') or buy_price, price)
                                    cur.execute("UPDATE holdings SET peak_price=%s WHERE symbol=%s;", (peak, sym))

                                    pnl_pct = ((price - buy_price) / buy_price) * 100.0
                                    drawdown = ((price - peak) / peak) * 100.0

                                    # Exit Signal (+3.5% target or -1.5% trailing stop or trend reversal)
                                    if (pnl_pct >= 3.5) or (drawdown <= -1.5) or (ind['ema5'] < ind['ema13']):
                                        action_type = "SELL (TAKE PROFIT)" if pnl_pct >= 3.5 else "SELL (STOP LOSS)"
                                        
                                        # Call AI API ONLY when sell signal triggers
                                        approved, reason = consult_ai_groq(sym, price, action_type, cash, ind, pnl_pct)

                                        if approved:
                                            cash += (qty * price)
                                            cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                            cur.execute("DELETE FROM holdings WHERE symbol=%s;", (sym,))
                                            cur.execute("INSERT INTO ledger (symbol, action, qty, price, total_value, pnl_pct, balance) VALUES (%s, 'SELL', %s, %s, %s, %s, %s);",
                                                        (sym, qty, price, qty * price, pnl_pct, cash))
                                else:
                                    # Entry Signal: Fast EMA crossover + Momentum RSI + Volume surge
                                    if (ind['ema5'] > ind['ema13']) and (45 <= ind['rsi'] <= 68) and ind['volume_surge']:
                                        allocation = cash * 0.15 # Allocate 15% per stock
                                        qty = int(allocation // price)

                                        if qty >= 1:
                                            # Call AI API ONLY when buy signal triggers
                                            approved, reason = consult_ai_groq(sym, price, "BUY", cash, ind)

                                            if approved:
                                                cost = qty * price
                                                cash -= cost
                                                cur.execute("UPDATE portfolio SET cash=%s WHERE id=1;", (cash,))
                                                cur.execute("INSERT INTO holdings (symbol, qty, buy_price, peak_price) VALUES (%s, %s, %s, %s);",
                                                            (sym, qty, price, price))
                                                cur.execute("INSERT INTO ledger (symbol, action, qty, price, total_value, balance) VALUES (%s, 'BUY', %s, %s, %s, %s);",
                                                            (sym, qty, price, cost, cash))

                        conn.commit()
        except Exception as e:
            print(f"Loop Error: {e}")

        await asyncio.sleep(20)
