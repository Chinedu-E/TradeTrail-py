import logging
import json
from dataclasses import asdict

from fastapi import FastAPI
from fastapi.websockets import WebSocket

from helpers import Participant, ParticipantSession, SessionsManager, Session
from producers import spawn_channel
from trading import spawn_bots

CHANNEL_NAME = "session"
app = FastAPI()
session_manager = SessionsManager()


@app.get("/")
async def health_check():
    return "Server running"


@app.websocket("/ws/create")
async def create_session(websocket: WebSocket, host_id: int, symbol:str, id: int,
                         max_clients: int, duration: int, starting_balance: float, against_server: bool):
    await websocket.accept()
    
    session = Session(id=id, symbol=symbol, starting_balance=starting_balance, max_clients=max_clients,
                      duration=duration, against_server=against_server, clients=[])
    await session_manager.add_session(session)
    await session_manager.add_client((websocket, host_id))
    
    session_participant = ParticipantSession(starting_balance=starting_balance)
    
    if against_server:
        num_bots = max_clients - 1
        spawn_bots(num_bots, session)
    
    while True:
        if session_manager.is_session_full(session.id):
            break
        
    session_manager.launch_session(session.id)
    
    while not session_manager.is_done(session.id):
        text = await websocket.receive()
        await session_participant.handle_message(websocket, text["text"])
        print(text)
        
    channel = spawn_channel(queue_name=CHANNEL_NAME)
    channel.basic_publish(exchange='',
                          routing_key=CHANNEL_NAME,
                          body=json.dumps(asdict(session_participant)))
        
        
    
@app.websocket("/ws/join")
async def join_session(websocket: WebSocket, session_id: int, client_id: int):
    await websocket.accept()
    
    await session_manager.add_client(websocket, session_id, client_id)
    
    session: Session = session_manager[session_id]
    session_participant = ParticipantSession(starting_balance=session.starting_balance)
    
    while not session_manager.is_done(session_id):
        text = await websocket.receive()
        await session_participant.handle_message(websocket, text["text"])
        print(text)
        
    channel = spawn_channel(queue_name=CHANNEL_NAME)
    channel.basic_publish(exchange='',
                          routing_key=CHANNEL_NAME,
                          body=json.dumps(asdict(session_participant)))