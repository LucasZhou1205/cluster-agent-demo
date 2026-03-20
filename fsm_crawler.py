import yfinance as yf
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

def get_fund_data_yf(symbol):
    print(f"尝试从 Yahoo Finance 获取数据 (Symbol: {symbol})...")
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="60d")
        if not df.empty:
            return df
    except Exception as e:
        print(f"Yahoo Finance 获取失败: {e}")
    return None

def get_fund_data_fsm(fcode):
    print(f"尝试从 FSMOne API 获取数据 (FCode: {fcode})...")
    # FSMOne API endpoint for historical prices
    url = f"https://secure.fundsupermart.com/fsmone/rest/funds/historical-prices/{fcode}?period=1M"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": f"https://secure.fundsupermart.com/fsmone/funds/factsheet/{fcode}/schroder-as-commodity-a-acc-usd",
        "X-Requested-With": "XMLHttpRequest"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # FSMOne usually returns a list of objects with 'date' and 'nav'
            if isinstance(data, list):
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                return df
            elif isinstance(data, dict) and 'data' in data:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                return df
    except Exception as e:
        print(f"FSMOne API 获取失败: {e}")
    return None

def main():
    fcode = "SCASCUS"
    # Yahoo symbols to try
    yf_symbols = ["0P000020S6.SI", "0P000020S6"]
    
    df = None
    
    # Try Yahoo Finance first
    for sym in yf_symbols:
        df = get_fund_data_yf(sym)
        if df is not None and not df.empty:
            print(f"成功从 Yahoo Finance ({sym}) 获取数据！")
            break
            
    # Try FSMOne API if Yahoo fails
    if df is None or df.empty:
        df = get_fund_data_fsm(fcode)
        if df is not None and not df.empty:
            print("成功从 FSMOne API 获取数据！")
            
    if df is not None and not df.empty:
        # Remove timezone for Excel support
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
            
        # Keep only the last 60 days
        df = df.sort_index(ascending=False).head(60)
        
        # Save to Excel
        filename = f"fund_data_{fcode}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(filename)
        print(f"数据已保存到 {filename}")
        print("\n最近5天的数据预览:")
        print(df.head())
    else:
        print("未能获取到数据，请检查网络或基金代码。")

if __name__ == "__main__":
    main()
