import requests

features_store_url = "http://127.0.0.1:8000"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": 17245, "k": 3}

resp = requests.post(features_store_url +"/recommendations_online", headers=headers, params=params)
if resp.status_code == 200:
    similar_items = resp.json()
else:
    similar_items = None
    print(f"status code: {resp.status_code}")
    
print(similar_items)
