import asyncio
import threading
import logging
import time
import datetime
import random
import requests
from typing import Union, Literal, List, Any, Dict
from dataclasses import dataclass, asdict
import csv
from abc import ABC, abstractmethod
from functools import lru_cache

from fastapi.websockets import WebSocket
import yfinance as yf
import pandas as pd
from decouple import config
from yahoo_fin import stock_info

class DictParser:
    """
    A class that parses a string and returns a dictionary of key-value pairs.

    Parameters
    ----------
    target_fields : List[str]
        A list of field names to be parsed from the input string.
    types : List[Any]
        A list of data types corresponding to the field names.

    Methods
    -------
    __call__(self, string: str) -> Dict[str, Any]:
        Parses the input string and returns a dictionary of key-value pairs.
        Returns an empty dictionary if the target fields are not found.
        
        Parameters
        ----------
        string : str
            The input string to be parsed.
        
        Returns
        -------
        out : Dict[str, Any]
            A dictionary of key-value pairs, where the keys are the target fields
            and the values are the corresponding parsed values.
    
    _parse(self, key: str, string: str) -> Union[str, Any]:
        Parses a specific key-value pair from the input string and returns it.
        
        Parameters
        ----------
        key : str
            The field name to be parsed.
        string : str
            The input string to be parsed.
            
        Returns
        -------
        Union[str, Any]
            A tuple containing the key-value pair. If the key is not found in the string,
            None is returned.
    """
    def __init__(self, target_fields: List[str], types: List[Any]):
        self.fields = target_fields
        self.types = types
        
    def __call__(self, string: str) -> Dict[str, Any]:
        """
        Parses the input string and returns a dictionary of key-value pairs.

        Parameters
        ----------
        string : str
            The input string to be parsed.

        Returns
        -------
        out : Dict[str, Any]
            A dictionary of key-value pairs, where the keys are the target fields
            and the values are the corresponding parsed values.
        """
        out = {}
        for i, field in enumerate(self.fields):
            pair = self._parse(field, string)
            if pair is not None:
                out[pair[0]] = self.types[i](pair[1])
        return out
        
    def _parse(self, key: str, string: str) -> Union[str, Any]:
        """
        Parses a specific key-value pair from the input string and returns it.

        Parameters
        ----------
        key : str
            The field name to be parsed.
        string : str
            The input string to be parsed.

        Returns
        -------
        Union[str, Any]
            A tuple containing the key-value pair. If the key is not found in the string,
            None is returned.
        """
        keyl = len(key)
        idx = string.find(key + ":")
        if idx == -1:
            return None
        
        rem = string[idx + keyl + 1:]
        idx2 = rem.find(",")
        return key, rem[:idx2].strip()   

@dataclass
class Transaction:
    """
    A data class representing a financial transaction.

    Attributes
    ----------
    transaction_type : str
        The type of the transaction, e.g., "buy" or "sell".
    shares : float
        The number of shares involved in the transaction.
    price : float
        The price per share of the transaction.

    Methods
    -------
    from_string(string: str) -> Transaction:
        Parses a string representing a transaction and returns a Transaction object.
        
        Parameters
        ----------
        string : str
            The input string to be parsed.
            
        Returns
        -------
        Transaction
            A Transaction object containing the parsed data.
    
    from_dict(trans_dict: Dict[str, Any]) -> Transaction:
        Creates a Transaction object from a dictionary of key-value pairs.
        
        Parameters
        ----------
        trans_dict : Dict[str, Any]
            A dictionary containing the transaction data.
            
        Returns
        -------
        Transaction
            A Transaction object containing the data from the input dictionary.
    """
    transaction_type: Literal["buy", "sell"]
    shares: float
    price: float
    
    @staticmethod
    def from_string(string: str) -> 'Transaction':
        """
        Parses a string representing a transaction and returns a Transaction object.

        Parameters
        ----------
        string : str
            The input string to be parsed.

        Returns
        -------
        Transaction
            A Transaction object containing the parsed data.
        """
        parser = DictParser(["type", "shares", "price"], types=[str, float, float])
        dict_ = parser(string)
        return Transaction.from_dict(dict_)
        
    @staticmethod
    def from_dict(trans_dict: Dict[str, Any]) -> 'Transaction':
        """
        Creates a Transaction object from a dictionary of key-value pairs.

        Parameters
        ----------
        trans_dict : Dict[str, Any]
            A dictionary containing the transaction data.

        Returns
        -------
        Transaction
            A Transaction object containing the data from the input dictionary.
        """
        return Transaction(trans_dict["type"], trans_dict["shares"], trans_dict["price"])


@dataclass
class Participant:
    """
    A data class representing a participant in a financial simulation.

    Attributes
    ----------
    session_id : int
        The ID of the session that the participant is a part of.
    user_id : int
        The ID of the user associated with the participant.
    shares : int
        The number of shares owned by the participant.
    profit : int
        The profit made by the participant.
    agent : bool
        A boolean value indicating whether the participant is an agent.

    """
    session_id: int
    user_id: int
    shares: int
    profit: int
    agent: bool
    
@dataclass
class Session:
    """
    A data class representing a financial simulation session.

    Attributes
    ----------
    id : int
        The ID of the session.
    symbol : str
        The symbol of the financial instrument being traded in the session.
    starting_balance : float
        The starting balance for each participant in the session.
    max_clients : int
        The maximum number of clients that can participate in the session.
    duration : int
        The duration of the session in seconds.
    against_server : bool
        A boolean value indicating whether the participants are trading against a server.
    clients : List[Tuple[Any, int]]
        A list of tuples representing the clients participating in the session.
        Each tuple contains a websocket object and a client ID.

    Methods
    -------
    add_client(client: WebSocket, client_id: int) -> bool:
        Adds a client to the session.

        Parameters
        ----------
        client : WebSocket
            The websocket object to be added.
        client_id : int
            The ID of the client.

        Returns
        -------
        bool
            True if the client was successfully added, False otherwise.
    """
    id: int
    symbol: str
    starting_balance: float
    max_clients: int
    duration: int
    against_server: bool
    clients: List[tuple[WebSocket, int]]

    async def add_client(self, client: WebSocket, client_id: int) -> bool:
        """
        Adds a client to the session.

        Parameters
        ----------
        client : WebSocket
            The websocket object to be added.
        client_id : int
            The ID of the client.

        Returns
        -------
        bool
            True if the client was successfully added, False otherwise.
        """
        if len(self.clients) == self.max_clients:
            return False
        self.clients.append((client, client_id))
        return True
   
@dataclass
class ParticipantSession:
    """
    A data class representing a participant's session in a financial simulation.

    Attributes
    ----------
    starting_balance : float
        The starting balance for the participant in the session.
    num_trades : int
        The number of trades executed by the participant.
    available_shares : float
        The number of shares available to the participant.
    balance : float
        The current balance of the participant.

    Methods
    -------
    async handle_message(client: WebSocket, text: str) -> None:
        Handles a message received from a client.

        Parameters
        ----------
        client : WebSocket
            The websocket object that sent the message.
        text : str
            The message text.

        Raises
        ------
        Exception
            If there is an error processing the message.

    async send(client: WebSocket) -> None:
        Sends the participant session data to a client.

        Parameters
        ----------
        client : WebSocket
            The websocket object to send the data to.
    """
    starting_balance: float
    is_agent: bool
    num_trades: int = 0
    available_shares: float = 0
    balance: float = 0

    def __post_init__(self):
        self.balance = self.starting_balance

    async def handle_message(self, client: WebSocket, text: str) -> None:
        """
        Handles a message received from a client.

        Parameters
        ----------
        client : WebSocket
            The websocket object that sent the message.
        text : str
            The message text.

        Raises
        ------
        Exception
            If there is an error processing the message.
        """
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

    async def send(self, client: WebSocket) -> None:
        """
        Sends the participant session data to a client.

        Parameters
        ----------
        client : WebSocket
            The websocket object to send the data to.
        """
        dict_ = asdict(self)
        await client.send_bytes(bytes(str(dict_), encoding="utf-8"))
    
@dataclass
class ClientManager:
    """
    Manages a list of clients with their respective ids.

    Attributes
    ----------
    clients: List[Tuple[WebSocket, int]]
        A list of tuples containing the client websocket and its id.

    Methods
    -------
    broadcast(price: float, prices: List[float]) -> None
        Broadcasts the given price and a list of prices to all clients.

        Parameters
        ----------
        price : float
            The current price to be broadcasted.
        prices : List[float]
            A list of historical prices to be broadcasted.

        Raises
        ------
        RuntimeError
            If the client is not connected.
        Exception
            If an error occurs while broadcasting.

        Returns
        -------
        None
    """
    clients: List[tuple[WebSocket, int]]
    
    async def broadcast(self, price: float, prices: List[float]) -> None:
        """
        Broadcasts the given price and a list of prices to all clients.

        Parameters
        ----------
        price : float
            The current price to be broadcasted.
        prices : List[float]
            A list of historical prices to be broadcasted.

        Raises
        ------
        RuntimeError
            If the client is not connected.
        Exception
            If an error occurs while broadcasting.

        Returns
        -------
        None
        """
        for client in self.clients:
            try:
                await client[0].send_text(str(price))
                await client[0].send_bytes(bytes(str(prices), encoding="utf-8"))
            except RuntimeError:
                ...
            except Exception as e:
                print(e)
    
    
class SessionsManager:
    """
    A class that manages sessions and their clients.

    Attributes:
        ids (Dict[int, Session]): A dictionary mapping session IDs to sessions.
        instance (SessionsManager): The singleton instance of the SessionsManager class.
    """

    ids: Dict[int, Session] = dict()
    instance = None

    def __init__(self):
        if SessionsManager.instance:
            self = SessionsManager.instance
        else:
            SessionsManager.instance = self
            
    def __getitem__(self, id: int) -> Session:
        return self.ids[id]

    def launch_session(self, id: int):
        """
        Launches a session with the given ID in a new thread.

        Args:
            id (int): The ID of the session to launch.
        """
        threading.Thread(target=self.__handle_session, args=(id,)).start()

    def __handle_session(self, id: int):
        """
        Handles a session in a new asyncio event loop in a separate thread.

        Args:
            id (int): The ID of the session to handle.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(self.handle_session(id))
        loop.close()

    async def handle_session(self, id: int):
        """
        Handles a session asynchronously.

        Args:
            id (int): The ID of the session to handle.
        """
        session = self.ids[id]
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

    async def add_client(self, client: WebSocket, session_id: int, client_id: int) -> bool:
        """
        Adds a client to a session.

        Args:
            client (WebSocket): The client to add.
            session_id (int): The ID of the session to add the client to.
            client_id (int): The ID of the client to add.

        Returns:
            bool: True if the client was successfully added, False if the session is full.
        """
        return await self.ids[session_id].add_client(client, client_id)

    async def add_session(self, session: Session):
        """
        Adds a session to the manager.

        Args:
            session (Session): The session to add.
        """
        if session.id not in self.ids:
            self.ids[session.id] = session

    def is_done(self, id: int) -> bool:
        """
        Checks whether a session is done.

        Args:
            id (int): The ID of the session to check.

        Returns:
            bool: True if the session is done, False otherwise.
        """
        if id in self.ids:
            return False
        return True

    def is_session_full(self, id: int) -> bool:
        """
        Checks whether a session is full.

        Args:
            id (int): The ID of the session to check.

        Returns:
            bool: True if the session is full, False otherwise.
        """
        session = self.ids[id]
        if session.max_clients == len(session.clients):
            return True
        return False


class Bot(ABC):
    ...
    

class TrainingDataHandler:
    
    def __next__(self):
        ...
        
    def __getitem__(self, mode: str) -> pd.DataFrame:
        ...
    
    @staticmethod
    @lru_cache
    def get_clustering_data(column:  Union[str, List[str]]) -> pd.DataFrame:
        tickers  = stock_info.tickers_sp500()
        start = datetime.datetime(2015, 1, 1)
        end = datetime.datetime.now()
        df = yf.download(tickers, start=start, end=end)
        df = df[column]
        return df
        
    @lru_cache
    def get_trading_data(self, ticker: str) -> pd.DataFrame:
        main_df = pd.DataFrame()
        for year in [1, 2]:
            for i in reversed(range(1, 13)):
                if i % 2 == 0:
                    time.sleep(25)
                csv_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={ticker}&interval=1min&slice=year{year}month{i}&apikey={config('ALPHA_API')}"
                with requests.Session() as s:
                    download = s.get(csv_url)
                    decoded_content = download.content.decode('utf-8')
                    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                    my_list = list(cr)
                    df = pd.DataFrame(my_list[1:], columns=my_list[0])
                main_df = pd.concat([df, main_df])
                time.sleep(1)
        return main_df
    
        
    def get_portfolio_allocation_data(self, tickers: List[str]) -> pd.DataFrame:
        ...
        
        
    def set_mode(self, mode):
        ...