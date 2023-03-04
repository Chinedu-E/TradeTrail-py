import requests
import csv

import numpy as np
import pandas as pd
import yfinance as yf 
from finta import TA

PREDICTORS = ["Open", "High", "Low", "Close", "Volume", "SMA", "RSI", "OBV", "KAMA", "ROC"]

def form_features(ticker: str, df: pd.DataFrame = None):
    if df is None:
        df = yf.download(ticker, interval="1m", period="5d")
    df['SMA'] = TA.SMA(df, 30)
    df['RSI'] = TA.RSI(df)
    df['OBV'] = TA.OBV(df)
    df["KAMA"] = TA.KAMA(df)
    df["ROC"] = TA.ROC(df, 30)
    df.fillna(0, inplace=True)
    df= df[PREDICTORS]
    return df

def get_data(symbol: str):
    main_df = pd.DataFrame()
    for i in reversed(range(1, 3)):
        CSV_URL = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=1min&slice=year2month{i}&apikey='
        with requests.Session() as s:
            download = s.get(CSV_URL)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            df = pd.DataFrame(my_list[1:], columns=my_list[0])
        main_df = pd.concat([df, main_df])
    return main_df

