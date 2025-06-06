import asyncio
import httpx
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

API_BASE_URL = "http://127.0.0.1:8000" 
ADMIN_EMAIL = "maria.admin@example.com" 
ADMIN_PASSWORD = "testpassword"          

OUTPUT_DIR = Path(__file__).resolve().parent / "data_pipeline_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

async def get_admin_token(client: httpx.AsyncClient) -> Optional[str]:
    """Получает токен администратора."""
    print("Attempting to get admin token...")
    try:
        response = await client.post(
            f"{API_BASE_URL}/token",
            data={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        )
        response.raise_for_status() 
        token_data = response.json()
        print("Successfully obtained admin token.")
        return token_data.get("access_token")
    except httpx.HTTPStatusError as e:
        print(f"Error getting admin token: HTTP {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred while getting admin token: {e}")
    return None

async def fetch_etl_data(client: httpx.AsyncClient, endpoint: str, token: str) -> Optional[Any]:
    """Запрашивает данные с ETL эндпоинта."""
    print(f"Fetching data from {endpoint}...")
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = await client.get(f"{API_BASE_URL}{endpoint}", headers=headers, timeout=60.0) 
        response.raise_for_status()
        print(f"Successfully fetched data from {endpoint}. Status: {response.status_code}")
        return response.json() 
    except httpx.HTTPStatusError as e:
        print(f"Error fetching data from {endpoint}: HTTP {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred while fetching from {endpoint}: {e}")
    return None

def save_to_csv(data: List[Dict[str, Any]], filename: str):
    """Сохраняет список словарей в CSV файл."""
    if not data:
        print(f"No data to save for {filename}.")
        return
    
    filepath = OUTPUT_DIR / filename
    try:
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            print(f"Data for {filename} is not in the expected List[Dict] format. Skipping CSV save for this item.")
            return

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
            print(f"Data successfully saved to {filepath}")

    except Exception as e:
        print(f"Error saving data to {filepath}: {e}")

def save_to_json(data: Any, filename: str):
    """Сохраняет данные в JSON файл."""
    if data is None: 
        print(f"No data provided to save for {filename}.")
        return
        
    filepath = OUTPUT_DIR / filename
    try:
        with open(filepath, "w", encoding="utf-8") as jsonfile:
            json.dump(data, jsonfile, ensure_ascii=False, indent=4, default=str) 
        print(f"Data successfully saved to {filepath}")
    except Exception as e:
        print(f"Error saving data to {filepath}: {e}")

async def run_pipeline():
    """Основная функция для запуска ETL пайплайна."""
    print(f"--- Starting ETL Pipeline: {datetime.now()} ---")
    
    async with httpx.AsyncClient() as client:
        admin_token = await get_admin_token(client)
        if not admin_token:
            print("Failed to obtain admin token. ETL pipeline cannot continue.")
            return

        etl_endpoints = {
            "user_course_interactions": "/etl/user_course_interactions",
            "course_features": "/etl/course_features",
            "user_features": "/etl/user_features"
        }

        for data_key, endpoint_path in etl_endpoints.items():
            data = await fetch_etl_data(client, endpoint_path, admin_token)
            if data is not None: 
                save_to_csv(data if isinstance(data, list) else [data], f"{data_key}.csv")
                save_to_json(data, f"{data_key}.json")
            else:
                print(f"Failed to fetch data for {data_key}, skipping save.")
   
    print(f"--- ETL Pipeline Finished: {datetime.now()} ---")

if __name__ == "__main__":
    asyncio.run(run_pipeline())