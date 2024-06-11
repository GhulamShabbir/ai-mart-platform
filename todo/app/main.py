# main.py
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
class Product(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    description: str
    price: float

class Todo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    content: str = Field(index=True)

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
    task = asyncio.create_task(consume_messages('products', 'broker:19092'))
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
    return {"Hello": "product_service"}
@app.post("/products", response_model=Product)
async def create_product(
    product: Product,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
) -> Product:
    try:
        product_json = json.dumps(product.dict()).encode("utf-8")
        await producer.send_and_wait("products", product_json)
        session.add(product)
        session.commit()
        session.refresh(product)
        return product
    except Exception as e:
        logger.error(f"Error creating product: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/products", response_model=list[Product])
def read_products(session: Annotated[Session, Depends(get_session)]):
    return session.exec(select(Product)).all()
