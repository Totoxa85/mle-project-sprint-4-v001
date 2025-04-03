import logging
from contextlib import asynccontextmanager
import pandas as pd
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")

class History:
    def __init__(self):
        self._history = None

    def load(self, path, **kwargs):
        logger.info(f"Loading data, type: {type}")
        self._history = pd.read_parquet(path, **kwargs)
        self._history = self._history.set_index('user_id')
        logger.info(f"Loaded")

    def get(self, user_id: int, k: int = 3):
        try:
            history = self._history.loc[user_id]
            history = history[["track_id", "track_seq"]].to_dict(orient="list")
        except KeyError:
            logger.error("No history found for user")
            history = []
        return history

history_store = History()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    history_store.load(
        "recsys/recommendations/als_recommendations.parquet",  # путь к файлу с историей событий
        columns=["user_id", "track_id", "track_seq"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="history", lifespan=lifespan)

@app.post("/get")
async def get_history(user_id: int, k: int = 3):
    history = history_store.get(user_id, k)
    return history