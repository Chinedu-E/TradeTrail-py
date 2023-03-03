from helpers import Bot, Transaction, Session
import websockets
import joblib
import json
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
        self.num_trades = 0
        self.model = joblib.load('xgboost_model.joblib')
        self.scaler = joblib.load('scaler.joblib')
    
    @staticmethod
    def start_bot(symbol: str):
        trader = Trader(symbol)
        trader.connect()
        
    async def connect(self):
        async with websockets.connect(f"ws://localhost:{config('PORT')}/ws/join?\
                                      session_id={self.session.id}&client_id=-1&is_agent=true")\
                                      as websocket:
            async for message in websocket:
                try:
                    price = float(str(message, encoding="utf-8"))
                    transaction = self.execute(price)
                    await websocket.send(transaction)
                except websockets.ConnectionClosed:
                    break
        
    def execute(self, price):
        df = utilities.form_features(self.session.symbol)
        df = self.scaler.transform(df)
        pred = self.model.predict(df)[-1]
        # TODO: Add noise to predictions
        if pred == 1:
            transaction = Transaction("buy", 1.0, price)
        else:
            transaction = Transaction("sell", 1.0, price)
        
        trade = json.dumps(asdict(transaction))
        
        self.num_trades += 1
        return trade