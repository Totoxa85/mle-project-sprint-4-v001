import logging

import requests

from contextlib import asynccontextmanager

import pandas as pd

from fastapi import FastAPI, HTTPException

logger = logging.getLogger("uvicorn.error")

features_store_url = "http://127.0.0.1:8010"
history_store_url = "http://127.0.0.1:8020"

class Recommendations:
    def __init__(self):
        self._recs = {}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0
            
        }

    def load(self, rec_type, path, **kwargs):
        logger.info(f"Loading data, type: {rec_type}")
        self._recs[rec_type] = pd.read_parquet(path, **kwargs)
        if rec_type == "personal":
            self._recs[rec_type] = self._recs[rec_type].set_index("user_id")
        logger.info(f"Loaded")

    def get(self, user_id: int, k: int = 100):
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
            
        except KeyError:
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except Exception: 
            logger.error("Неизвестная ошибка - No recommendations found") 
        return recs

    def stats(self):
        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")
    rec_store = Recommendations()
    rec_store.load(
        "personal",
        "recsys/recommendations/recommendations.parquet",  # путь к файлу с персональными рекомендациями
        columns=["user_id", "track_id", "track_seq"],
    )
    rec_store.load(
        "default",
        "recsys/recommendations/top_popular.parquet",  # путь к файлу с дефолтными рекомендациями
        columns=["track_id", "track_seq"],
    )
    app.state.recs = rec_store
    yield
    logger.info("Stopping")
    app.state.recs.stats()

# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    recs = app.state.recs.get(user_id, k)
    return {"recs": recs}

def dedup_ids(ids):
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]
    return ids

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(history_store_url + "/get", headers=headers, params=params)
    events = resp.json()
    events = events.get("track_id", [])
    items = []
    scores = []
    for track_id in events:
        similar_items_params = {"item_id": track_id, "k": k}
        similar_items_resp = requests.post(features_store_url + "/similar_items", headers=headers, params=similar_items_params)
        item_similar_items = similar_items_resp.json()
        items += item_similar_items.get("item_id_2")
        print(items)
        scores += item_similar_items.get("track_seq")
        print(scores)
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]
    recs = dedup_ids(combined)
    return {"recs": recs}

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    # Проверка формата данных
    if not isinstance(recs_offline.get("recs", []), list):
        logger.error(f"recs_offline['recs'] is not a list: {recs_offline['recs']}")
        raise HTTPException(status_code=400, detail="Invalid format of recs_offline['recs']")
    
    if not isinstance(recs_online.get("recs", []), list):
        logger.error(f"recs_online['recs'] is not a list: {recs_online['recs']}")
        raise HTTPException(status_code=400, detail="Invalid format of recs_online['recs']")
    
    recs_blended = []
    min_length = min(len(recs_offline["recs"]), len(recs_online["recs"]))
    for i in range(min_length):
        recs_blended.append(recs_offline["recs"][i])
        recs_blended.append(recs_online["recs"][i])
    recs_blended.extend(recs_offline["recs"][min_length:])
    recs_blended.extend(recs_online["recs"][min_length:])
    recs_blended = dedup_ids(recs_blended)
    recs_blended = recs_blended[:k]
    return {"recs": recs_blended}