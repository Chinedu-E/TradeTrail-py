from helpers import Bot, Transaction, Session
import pandas as pd
import yfinance as yf 
from finta import TA
import websockets
import joblib
from dataclasses import asdict
import utilities
import concurrent.futures as cf
from decouple import config


def spawn_bots(n: int, session: Session):
    with cf.ThreadPoolExecutor(max_workers=n) as executor:
        executor.map(Trader.start_bot, session)

class Trader(Bot):
    
    def __init__(self, session: Session):
        self.session = session
        self.is_connected = False
        self.model = joblib.load('xgboost_model.joblib')
        self.scaler = joblib.load('scaler.joblib')
    
    @staticmethod
    def start_bot(symbol: str):
        trader = Trader(symbol)
        trader.connect()
        
    async def connect(self):
        async with websockets.connect(f"ws://localhost:{config('PORT')}/ws/join?session_id={self.session.id}&client_id=-1") as websocket:
            async for message in websocket:
                try:
                    price = float(str(message, encoding="utf-8"))
                    transaction = self.execute(price)
                    await websocket.send(transaction)
                except websockets.ConnectionClosed:
                    break
        
    def execute(self, price):
        df = yf.download(self.session.symbol, interval="1m", period="1d")
        df['SMA'] = TA.SMA(df, 30)
        df['RSI'] = TA.RSI(df)
        df['OBV'] = TA.OBV(df)
        df["KAMA"] = TA.KAMA(df)
        df["ROC"] = TA.ROC(df, 30)
        df.fillna(0, inplace=True)
        df= df[utilities.PREDICTORS]
        df = self.scaler.transform(df)
        pred = self.model.predict(df)[-1]
        if pred == 1:
            t = Transaction("buy", 1.0, price)
        else:
            t = Transaction("sell", 1.0, price)
        
        out = str(asdict(t))
        
        return out