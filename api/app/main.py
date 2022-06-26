import asyncio
import os
from typing import List

import databases
import psycopg2
import sqlalchemy
from fastapi import FastAPI
from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine

MESSAGE_STREAM_DELAY = 2  # seconds
MESSAGE_STREAM_RETRY_TIMEOUT = 15000  # miliseconds
# Materialize connection
DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://materialize:materialize@localhost:6875/materialize"
)

database = databases.Database(DB_URL)
metadata = sqlalchemy.MetaData()
engine = create_engine(DB_URL)

# Convert numbers from postgres to floats instead of Decimal so that we can use serialize them as jsons with the
# defautl Python serializer
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    "DEC2FLOAT",
    lambda value, curs: float(value) if value is not None else None,
)
psycopg2.extensions.register_type(DEC2FLOAT)

app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)


manager = ConnectionManager()


def new_messages(interval="1m"):
    results = engine.execute(f"SELECT count(*) FROM changes_by_server_{interval}")
    return None if results.fetchone()[0] == 0 else True


async def event_generator(interval: str):
    if new_messages(interval=interval):
        print(f"New messages in {interval}")
        connection = engine.raw_connection()
        with connection.cursor() as cur:
            cur.execute(f"DECLARE c CURSOR FOR TAIL changes_by_server_{interval}")
            cur.execute("FETCH ALL c")
            for row in cur:
                yield row

    await asyncio.sleep(MESSAGE_STREAM_DELAY)


@app.websocket("/wikidata/{interval}")
async def websocket_endpoint(websocket: WebSocket, interval: str):
    await manager.connect(websocket)
    print(f"Connected to {interval}")
    try:
        while True:
            async for data in event_generator(interval=interval):
                print(f"Sending {data}")
                payload = {
                    "server_name": data[2],
                    "count": data[3],
                }
                await websocket.send_json(payload)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
