import requests
import csv
from typing import List

import numpy as np
import pandas as pd
import yfinance as yf 
from finta import TA

PREDICTORS = ["Open", "High", "Low", "Close", "Volume", "SMA", "RSI", "OBV", "KAMA", "ROC"]

def form_features(ticker: str = None, df: pd.DataFrame = None):
    if df is None:
        assert ticker is not None
        df = yf.download(ticker, interval="1m", period="5d")
    df['SMA'] = TA.SMA(df, 30)
    df['RSI'] = TA.RSI(df)
    df['OBV'] = TA.OBV(df)
    df["KAMA"] = TA.KAMA(df)
    df["ROC"] = TA.ROC(df, 30)
    df.fillna(0, inplace=True)
    df= df[PREDICTORS]
    return df


def form_labels(df: pd.DataFrame) -> List[str]:
    df["Tomorrow"] = df["Close"].shift(-1)
    df["Target"] = (df["Tomorrow"] > df["Close"]).astype(int)
    return list(df["Target"].values)
