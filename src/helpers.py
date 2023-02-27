import asyncio
import threading
import logging
import time
import random
from typing import Union, Literal, List, Any, Dict
from dataclasses import dataclass, asdict


class DictParser:
    
    def __init__(self, target_fields: List[str],
                 types: List[Any]):
        self.fields = target_fields
        self.types = types
        
    def __call__(self, string: str) -> Dict[str, Any]:
        out = {}
        for i, field in enumerate(self.fields):
            pair = self._parse(field, string)
            out[pair[0]] = self.types[i](pair[1])
        return out
        
    def _parse(self, key: str, string: str) -> Union[str, Any]:
        keyl= len(key)
        idx=string.find(key+":")
        if idx == -1:
            return
        
        rem = string[idx+keyl+1:]
        idx2 = rem.find(",")
        return key, rem[:idx2].strip()
    
    
@dataclass
class Transaction:
    transaction_type: str
    shares: float
    price: float
    
    @staticmethod
    def from_string(string: str):
        parser = DictParser(["type", "shares", "price"], types=[str, float, float])
        dict_= parser(string)
        return Transaction.from_dict(dict_)
        
    @staticmethod
    def from_dict(trans_dict):
        return Transaction(trans_dict["type"], trans_dict["shares"], trans_dict["price"])
    

@dataclass
class Participant:
    session_id: int
    user_id: int
    shares: int
    profit: int
    agent: bool
    
    
@dataclass
class Session:
    id: int
    symbol: str
    starting_balance: float
    max_clients: int
    duration: int
    against_server: bool
    clients: List[tuple[Any, int]]
    
    async def add_client(self, client: Any, client_id: int):
        if len(self.clients) == self.max_clients:
            return False
        self.clients.append((client, client_id))
        return True
   
   
@dataclass
class ParticipantSession:
    starting_balance : float
    num_trades: int = 0
    available_shares: float = 0
    balance: float= 0
    
    def __post_init__(self):
        self.balance = self.starting_balance
        
    async def handle_message(self, client: Any, text: str):
        try:
            transaction = Transaction.from_string(text)
            self.num_trades += 1
            if transaction.transaction_type == "buy":
                self.balance -= transaction.shares * transaction.price
                self.available_shares += transaction.shares
            else:
                self.balance += transaction.shares * transaction.price
                self.available_shares -= transaction.shares
                
            await self.send(client)
        except Exception as e:
            print(e)
            print(text)
            
    async def send(self, client: Any):
        dict_ = asdict(self)
        await client.send_bytes(bytes(str(dict_), encoding="utf-8"))
    
@dataclass
class ClientManager:
    clients: List[tuple[Any, int]]
    
    async def broadcast(self, price: float, prices: List[float]):
        for client in self.clients:
            try:
                await client[0].send_text(str(price))
                await client[0].send_bytes(bytes(str(prices), encoding="utf-8"))
            except RuntimeError:
                ...
            except Exception as e:
                print(e)
    
    
class SessionsManager:
    ids: Dict[int, Session] = dict()
    instance = None
    
    def __init__(self):
        if SessionsManager.instance:
            self = SessionsManager.instance
        else:
            SessionsManager.instance = self
        
    def launch_session(self, id: int):
        threading.Thread(target=self.__handle_session, args=(id,)).start()
        
    def __handle_session(self, id: int):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(self.handle_session(id))
        loop.close()
        
        
    async def handle_session(self, id: int):
        session  = self.ids[id]
        client_manager = ClientManager(session.clients)
        prices = []
        start = time.time()
        
        while time.time() - start < session.duration:
            price = random.random()
            prices.append(price)
            await client_manager.broadcast(price, prices)
        
        try:
            del self.ids[id]
        except KeyError:
            print("Session already deleted or does not exist")
        
    async def add_client(self, client: Any, session_id: int, client_id: int):
        return self.ids[session_id].add_client(client, client_id)
        
    async def add_session(self, session: Session):
        if session.id not in self.ids:
            self.ids[session.id] = session
    
    def is_done(self, id: int):
        if id in self.ids:
            return False
        return True
        
    def is_session_full(self, id: int):
        session = self.ids[id]
        if session.max_clients == len(session.clients):
            return True
        return False