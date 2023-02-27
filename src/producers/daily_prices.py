import json
import concurrent.futures as cf

from yahoo_fin import stock_info
import yfinance as yf
import pandas as pd

from settings import spawn_channel



def get_price(ticker: str):
    df: pd.DataFrame = yf.download(ticker, period="5d", interval="1d")
    df["Date"] = df.index.values
    df["Date"] = df["Date"].astype(str)
    message = df.to_dict(orient="records")[-1]
    message["Symbol"] = ticker
    del message["Adj Close"]
    return message
    

def main():
    queue_name  = "prices"
    tickers = stock_info.tickers_sp500()
    channel = spawn_channel(queue_name)
    
    with cf.ThreadPoolExecutor(max_workers=100) as executor:
        results  = [executor.submit(get_price, ticker) for ticker in tickers]
        
        for future in cf.as_completed(results):
            data = future.result()
            if data is not None:
                channel.basic_publish(exchange='',
                                      routing_key=queue_name,
                                      body=json.dumps(data))
    
    
if __name__=="__main__":
    main()