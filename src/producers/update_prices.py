import time
import datetime
import json
from yahoo_fin import stock_info
import concurrent.futures as cf

from settings import spawn_channel

def get_live_quote(ticker: str):
    message = stock_info.get_quote_table(ticker)
    message["Symbol"] = ticker
    return message


def send_prices():
    queue_name  = "latest_prices"
    tickers = stock_info.tickers_sp500()
    channel = spawn_channel(queue_name)
    
    with cf.ThreadPoolExecutor(max_workers=100) as executor:
        results  = [executor.submit(get_live_quote, ticker) for ticker in tickers]
        
        for future in cf.as_completed(results):
            data = future.result()
            if data is not None:
                channel.basic_publish(exchange='',
                                      routing_key=queue_name,
                                      body=json.dumps(data))
    
def main():
    while True:
        now = datetime.datetime.now()
        if now.hour >= 9 and now.hour < 16:
            if now.hour == 9 and now.minute < 0:
                continue
            send_prices()
        else:
            time.sleep(60)
    
    
if __name__== "__main__":
    main()