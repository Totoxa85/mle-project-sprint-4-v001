from fastapi.testclient import TestClient
import pytest
from recommendations_service import app
import requests
import logging

client = TestClient(app)

# Настройка логирования
file_log = logging.FileHandler("test_service.log", mode="w")
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Устанавливаем уровень логирования на INFO
logger.addHandler(file_log)

@pytest.fixture(scope="session", autouse=True)
def setup_and_teardown():
    logger.info("Starting test session")
    yield
    logger.info("Ending test session")
    file_log.close()  # Закрываем обработчик логов

recomendation_usr = 'http://127.0.0.1:8000'

# Сценарий 1: Пользователь без персональных рекомендаций
def test_user_no_personal_recs():
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    params = {"user_id": 9999, "k": 10}
    response = requests.post(recomendation_usr + "/recommendations", params=params, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["recs"]) == 10
    logger.info(f"Test passed: user_id: {params["user_id"]} test_user_no_personal_recs. Response: {data}")

# Сценарий 2: Пользователь с персональными рекомендациями
def test_user_with_personal_recs():
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    params = {"user_id": 1, "k": 10}
    response = requests.post(recomendation_usr + "/recommendations", params=params, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["recs"]) == 10
    logger.info(f"Test passed: user_id: {params["user_id"]} test_user_with_personal_recs. Response: {data}")

# Сценарий 3: Пользователь с онлайн и оффлайн рекомендациями
def test_user_online_and_offline_recs():
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    params = {"user_id": 2, "k": 10}
    response = requests.post(recomendation_usr + "/recommendations", params=params, headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["recs"]) == 10
    logger.info(f"Test passed: user_id: {params["user_id"]} test_user_online_and_offline_recs. Response: {data}")