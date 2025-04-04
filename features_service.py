import logging

from contextlib import asynccontextmanager

import pandas as pd

from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")

class SimilarItems:
    def __init__(self):
        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Функция загрузки файла 
        
        """
        logger.info(f"Loading data, type: {type}")
        self._similar_items = pd.read_parquet(path, **kwargs)
        self._similar_items = self._similar_items.set_index('item_id_1')
        logger.info(f"Loaded")

    def get(self, item_id: int, k: int = 10):
        try:
            similar_items = self._similar_items.loc[item_id]
            similar_items = similar_items[["item_id_2", "track_seq"]].to_dict(orient="list")
        except KeyError:
            logger.error("No similar items found for item")
            similar_items = {"item_id_2": [], "track_seq": []}
        return similar_items

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_items_store.load(
        "recsys/recommendations/similar.parquet",  # путь к файлу с похожими элементами
        columns=["item_id_1", "item_id_2", "track_seq"],
    )
    logger.info("Ready!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_items")
async def similar_items(item_id: int, k: int = 10):
    similar_items = sim_items_store.get(item_id, k)
    return similar_items