from contextlib import asynccontextmanager
from typing import Optional, Annotated, AsyncGenerator
from sqlmodel import Field, Session, SQLModel, create_engine, select
from fastapi import FastAPI, Depends, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define models
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    email: str
    password: str

# Database connection
connection_string = str(settings.DATABASE_URL).replace("postgresql", "postgresql+psycopg")
engine = create_engine(connection_string, connect_args={}, pool_recycle=300)

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)

async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id="my-group", auto_offset_reset='earliest')
    await consumer.start()
    try:
        async for message in consumer:
            logger.info(f"Received message: {message.value.decode()} on topic {message.topic}")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Creating tables..")
    task = asyncio.create_task(consume_messages('users', 'broker:19092'))
    create_db_and_tables()
    yield
    task.cancel()
    await task

app = FastAPI(lifespan=lifespan, title="Hello World API with DB", version="0.0.1")

def get_session():
    with Session(engine) as session:
        yield session

async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@app.get("/")
def read_root():
    return {"Hello": "user_service"}

@app.post("/users", response_model=User)
async def create_user(
    user: User,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
) -> User:
    try:
        user_json = json.dumps(user.dict()).encode("utf-8")
        await producer.send_and_wait("users", user_json)
        session.add(user)
        session.commit()
        session.refresh(user)
        return user
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/users", response_model=list[User])
def read_users(session: Annotated[Session, Depends(get_session)]):
    return session.exec(select(User)).all()