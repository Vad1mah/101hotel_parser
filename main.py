import json
import csv
import os
import pandas as pd
import datetime as dt
import logging
import asyncio
import aiohttp
from dotenv import load_dotenv
from telegram import Bot
import sys
from datetime import datetime
from geopy.distance import geodesic
import time
import numpy as np

import ydb
import ydb.iam

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
CITIES = os.getenv("CITIES", "").split(",")
ROOMS_DATA_PATH = os.getenv("ROOMS_DATA_PATH", "./tables/rooms_data")
HOTELS_STATISTICS_PATH = os.getenv("HOTELS_STATISTICS_PATH", "./tables/hotels_statistics")
DATABASE_FILE_PATH = os.getenv("DATABASE_FILE", "./databases/af_all_2024.csv")
DISTANCES_FILE_PATH = os.getenv("CITY_CENTER_AND_SEA_DISTANCES_FILE", "./databases/hotels_city_center_and_sea_distances.csv")
DISTRICTS_FILE_PATH = os.getenv("DISTRICTS_FILE_PATH", "./databases/city_districts.json")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ENDPOINT = os.getenv("YDB_ENDPOINT")
DATABASE = os.getenv("YDB_DATABASE")

AUTH_TOKEN_PATH = os.getenv("AUTHORIZED_KEY_PATH")
CREDENTIALS = ydb.iam.ServiceAccountCredentials.from_file(AUTH_TOKEN_PATH)

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "./logs/logging.log")
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Скрипт запущен")

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID else None
with open(DISTRICTS_FILE_PATH, "r", encoding="utf-8") as f:
    city_to_district = json.load(f)

baykal_districts_lat_lon = {}
with open(DATABASE_FILE_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if "/cities/baykal/" in row.get("url", ""):
            coords = (row.get("lat", 0), row.get("lon", 0))
            baykal_districts_lat_lon[coords] = row.get("district")

def find_nearest_district(lat, lon):
    min_distance = float("inf")
    nearest_district = None
    hotel_coords = (lat, lon)
    
    for coords, district in baykal_districts_lat_lon.items():
        distance = geodesic(hotel_coords, coords).meters
        print(distance, district)
        if distance < min_distance:
            min_distance = distance
            nearest_district = district
    
    return nearest_district         

async def send_telegram_message(message):
    if bot:
        try:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения в Telegram: {e}")
    else:
        logging.error("Telegram токен или Chat ID не заданы в .env")

def update_file_data(new_data, file_path):
    try:
        db = pd.read_csv(file_path)
        column_order = db.columns.tolist()

        db.set_index("id", inplace=True)
        new_data.set_index("id", inplace=True)

        db.update(new_data)
        
        new_rows = new_data.loc[~new_data.index.isin(db.index)].dropna(how="all")
        if not new_rows.empty:
            db = pd.concat([db, new_rows])
        
        db.reset_index(inplace=True)
        db = db[column_order]

        db.to_csv(file_path, index=False)
        
        logging.info(f"Файл {file_path} обновлен. Обновлено/добавлено строк: {len(new_data)}")

    except Exception as e:
        logging.error(f"Ошибка при обновлении файла {file_path}: {e}")

def extract_hotel_data(hotel, city):
    try:
        if city == "baykal":
            district = find_nearest_district(hotel.get("coords", [])[1], hotel.get("coords", [])[0])
        else:
            district = city_to_district.get(city, None)
        return {
            "city": hotel.get("city_name", None),
            "full_name": hotel.get("full_name", None),
            "name": hotel.get("name", None),
            "number_reviews": hotel.get("number_reviews", None),
            "rating": hotel.get("rating", None),
            "min_price": hotel.get("min_price", None),
            "rooms_number": hotel.get("rooms_num", None),
            "rating.number_scores": hotel.get("reviews_summary", {}).get("number_scores", None),
            "rating_total": hotel.get("reviews_summary", {}).get("rating", None),
            "rating.quality_of_sleep": hotel.get("reviews_summary", {}).get("quality_of_sleep", None),
            "rating.location": hotel.get("reviews_summary", {}).get("location", None),
            "rating.service": hotel.get("reviews_summary", {}).get("service", None),
            "rating.value_for_money": hotel.get("reviews_summary", {}).get("value_for_money", None),
            "rating.cleanness": hotel.get("reviews_summary", {}).get("cleanness", None),
            "rating.meal": hotel.get("reviews_summary", {}).get("meal", None),
            "lon": hotel.get("coords", [])[0],
            "lat": hotel.get("coords", [])[1],
            "district": district,
            "id": f"{hotel.get('id', None)}_101hotels",
            "url": hotel.get("url", None),
            "description": hotel.get("description", None),
            "ota_hotel_id": hotel.get("ota_hotel_id", None),
            "rates": hotel.get("rates", None),
            "address": hotel.get("address", None),
            "kind": hotel.get("type_name", None),
            "region_id": hotel.get("region_id", None),
            "region_name": hotel.get("region_name", None),
            "room_groups": hotel.get("room_groups", None),
            "star_rating": hotel.get("stars", None),
            "region.is_beach_region": hotel.get("region_is_beach_region", None),
            "search_region_center_distance": hotel.get("center_distance", None),
            "hotel_lookup_info.looked_up_count": hotel.get("pageviews", None),
            "type": hotel.get("type_name", None),
            "rating.room": hotel.get("reviews_summary", {}).get("room", None),
            "rating.check_in_check_out": hotel.get("reviews_summary", {}).get("check_in_check_out", None),
            "beds_number": hotel.get("beds_number", None),
            "source": "101hotels"
        }
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных для отеля {hotel.get('id', 'Неизвестно')}: {e}")
        return {}

def extract_room_data(date, room, hotel_id):
    try:
        return {
            "id": room.get("id", None),
            "k": room.get("k", None),
            "name": room.get("name", None),
            "capacity": room.get("capacity", None),
            "additional": room.get("additional", None),
            "single_bed": room.get("single_bed", None),
            "bathroom": room.get("bathroom", None),
            "type_name": room.get("type_name", None),
            "type_id": room.get("type_id", None),
            "free": room.get("free", None),
            "bed_categories": room.get("bed_categories", None),
            "children_extras": room.get("children_extras", None),
            "placements": room.get("placements", None),
            "min_price": room.get("min_price", None),
            "miles": room.get("miles", None),
            "hotel_id": f"{hotel_id}_101hotels",
            "date": date
        }
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных для номера отеля {hotel_id}: {e}")
        return {}

def extract_distances_data(hotel):
    try:
        sea_distances = hotel.get("sea_distances", {})
        if not sea_distances:
            sea_distances = {}
        sea_distances = sea_distances.get("all", None)
        city_center_distances = hotel.get("city_center_distances", None)

        return {
            "id": f"{hotel.get('id', None)}_101hotels",
            "city_center_distances": json.dumps(city_center_distances, ensure_ascii=False) if city_center_distances else None,
            "sea_distances": json.dumps(sea_distances, ensure_ascii=False) if sea_distances else None
        }
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных для отеля {hotel.get('id', 'Неизвестно')}: {e}")
        return {}

def extract_statistic_data(date, rooms, hotel_id, min_price, rooms_num):
    try:
        free_rooms_amount = sum(room.get("free", 0) if room.get("single_bed", 0) == 0 else 1 for room in rooms)
        available_rooms_percent = free_rooms_amount * 100 / rooms_num
        max_capacity = sum(room.get("free", 0) * room.get("capacity", 0) for room in rooms)
        return {
            "id": f"{hotel_id}_101hotels",
            "rooms_num": rooms_num,
            "free_rooms_amount": free_rooms_amount,
            "available_rooms_percent": available_rooms_percent,
            "max_capacity": max_capacity,
            "date": date,
            "min_price": min_price,
        }
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных для номера отеля {hotel_id}: {e}")
        return {}

async def get_hotels_info(session, city, extract_date, start_date, end_date, page):
    try:
        url = f"https://ssg.101hotels.com/hotel/available/city/russia/{city}?sort_direction=desc&in={start_date}&out={end_date}&adults=1&page={page}&scenario=desktop"
        
        async with session.get(url) as response:
            if response.status != 200:
                logging.warning(f"Ошибка {response.status} для {city}, страница {page}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0

            data = await response.json()
            if "response" not in data or "hotels" not in data["response"]:
                logging.warning(f"Некорректный формат ответа для {city}, страница {page}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), 0

            hotels_amount = data['response'].get('total', 0)
            hotels = data['response'].get('hotels', [])
            if hotels_amount == 0:
                logging.info(f"Нет данных в ответе API для {city}, страница {page}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), hotels_amount

            hotels_info_database = pd.json_normalize([extract_hotel_data(hotel, city) for hotel in hotels])
            if hotels_info_database.empty:
                logging.warning(f"Нет данных для города {city}, страница {page}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), hotels_amount
            
            hotels_rooms_info = pd.json_normalize([extract_room_data(extract_date, room, hotel.get("id", "")) for hotel in hotels for room in hotel.get("rooms", [])])
            city_distances_info = pd.json_normalize([extract_distances_data(hotel) for hotel in hotels])
            hotels_statistic = pd.json_normalize([extract_statistic_data(extract_date, hotel.get("rooms", [{}]), hotel.get("id", ""), hotel.get("min_price", ""), hotel.get("rooms_num", 0)) for hotel in hotels])
            return hotels_info_database, hotels_rooms_info, city_distances_info, hotels_statistic, hotels_amount

    except Exception as e:
        logging.error(f"Ошибка при обработке {city}, страница {page}: {e}")
        return pd.DataFrame(), 0
    
def create_ydb_table(driver_config, create_table_query, table_name):    
    with ydb.Driver(driver_config) as driver:
        driver.wait(fail_fast=True, timeout=20)
        session = driver.table_client.session().create()

        try:
            session.execute_scheme(create_table_query)
            logging.info(f"Таблица {table_name} успешно создана.")
        except Exception as e:
            logging.error(f"Не удалось создать таблицу {table_name}: {e}")
            
def upsert_data_to_ydb(driver_config, csv_path_to_import, table_name, ydb_dir, date):  
    with ydb.Driver(driver_config) as driver:
        driver.wait(fail_fast=True, timeout=20)
        session = driver.table_client.session().create()

        with open(csv_path_to_import, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        batch = []

        def safe_value(value, is_string=True):
            if value is None or value == '':
                return "NULL" if not is_string else "''"
            return "'{}'".format(value.replace("'", "''")) if is_string else str(value)
        
        if table_name == "af_all_2024":
            query_template = f"""
            PRAGMA TablePathPrefix("{DATABASE}/{ydb_dir}");
            UPSERT INTO {table_name} ( id, city, name, lon, lat, district )
            VALUES
            """
            
            for row in rows:
                if row.get("source") == "101hotels":
                    batch.append(f"""(
                        {safe_value(row['id'])},
                        {safe_value(row['city'])},
                        {safe_value(row['name'])},
                        {float(row['lon'])},
                        {float(row['lat'])},
                        {safe_value(row['district'])}
                    )""")
                
                if len(batch) >= 200:
                    try:
                        query = query_template + ",\n".join(batch) + ";"
                        session.transaction().execute(query, commit_tx=True)
                        logging.info(f"Обновлено {len(batch)} записей в YDB.")
                        time.sleep(5)
                    except Exception as e:
                        logging.error(f"Ошибка при UPSERT в YDB: {e}")
                    batch = []

        elif table_name == "hotels_statistics":
            query_template = f"""
            PRAGMA TablePathPrefix("{DATABASE}/{ydb_dir}");
            UPSERT INTO {table_name} ( id, rooms_num, free_rooms_amount, available_rooms_percent, max_capacity, date, min_price )
            VALUES
            """
            
            for row in rows:
                batch.append(f"""(
                    {safe_value(row['id'])},
                    {f"CAST({np.int16(row['rooms_num'])} AS Int16)" if row['rooms_num'] is not None else 'NULL'},
                    {f"CAST({np.int16(row['free_rooms_amount'])} AS Int16)" if row['free_rooms_amount'] is not None else 'NULL'},
                    {f"CAST({np.float32(row['available_rooms_percent'])} AS Float)" if row['available_rooms_percent'] is not None else 'NULL'},
                    {f"CAST({np.int16(row['max_capacity'])} AS Int16)" if row['max_capacity'] is not None else 'NULL'},
                    CAST(DATE("{date}") AS Date),
                    {f"CAST({np.int32(float(row['min_price']))} AS Int32)" if row['min_price'] is not None else 'NULL'}
                )""")
                
                if len(batch) >= 200:
                    try:
                        query = query_template + ",\n".join(batch) + ";"
                        session.transaction().execute(query, commit_tx=True)
                        logging.info(f"Обновлено {len(batch)} записей в YDB.")
                        time.sleep(5)
                    except Exception as e:
                        logging.error(f"Ошибка при UPSERT в YDB: {e}")
                    batch = []

        if batch:
            try:
                query = query_template + ",\n".join(batch) + ";"
                session.transaction().execute(query, commit_tx=True)
                logging.info(f"Обновлено {len(batch)} записей в YDB.")
            except Exception as e:
                logging.error(f"Ошибка при UPSERT в YDB: {e}")
        
        # else:
            # delete_old_data = f"""
            # PRAGMA TablePathPrefix("{DATABASE}/{ydb_dir}");
            # DELETE FROM {table_name} WHERE date = Date('{date}');
            # """

            # try:
            #     session.transaction().execute(delete_old_data, commit_tx=True)
            #     logging.info(f"Удалены старые данные за {date} в {table_name}.")
            # except Exception as e:
            #     logging.warning(f"Не удалось удалить старые данные за {date}: {e}")

            # command = [
            #     "ydb",
            #     "-e", ENDPOINT,
            #     "-d", DATABASE,
            #     "--sa-key-file", AUTH_TOKEN_PATH,
            #     "import", "file", "csv",
            #     "-p", f"{DATABASE}/{ydb_dir}/{table_name}",
            #     "--header", csv_path_to_import
            # ]
            
            # try:
            #     subprocess.run(command, check=True, text=True, capture_output=True)
            #     logging.info("Импорт CSV в YDB прошёл успешно")
            # except subprocess.CalledProcessError as e:
            #     logging.error("Ошибка импорта CSV в YDB:", e.stderr)

async def parse_101hotels_async():
    try:
        today = dt.date.today()
        next_day = today + dt.timedelta(days=1)
        start_date = today.strftime("%d.%m.%Y")
        end_date = next_day.strftime("%d.%m.%Y")
        extract_date = datetime.today().strftime('%Y-%m-%d')

        cities_combined_data_database = []
        rooms_combined_data_dashboard = []
        hotels_combined_distances_data = []
        cities_combined_statistic = []
        async with aiohttp.ClientSession() as session:
            for city in CITIES:
                try:
                    logging.info(f"Начало парсинга города: {city}")
                    city_hotels_data_database = []
                    city_rooms_data = []
                    hotels_distances_data = []
                    city_hotels_statistic = []
                    page = 1
                    hotels_amount = 1
                    while sum(len(hotel) for hotel in city_hotels_data_database) < hotels_amount:
                        hotels_info_database, hotels_rooms_info, city_distances_info, hotels_statistic, hotels_amount = await get_hotels_info(session, city, extract_date, start_date, end_date, page)
                        if hotels_amount == 0:
                            break
                        
                        city_hotels_data_database.append(hotels_info_database)
                        city_rooms_data.append(hotels_rooms_info)
                        hotels_distances_data.append(city_distances_info)
                        city_hotels_statistic.append(hotels_statistic)
                        logging.info(f"Обработано отелей: {sum(len(hotel) for hotel in city_hotels_data_database)} из {hotels_amount} для города {city}, страница {page}")
                        page += 1

                    if city_hotels_data_database:
                        df_city_database = pd.concat(city_hotels_data_database, ignore_index=True).drop_duplicates(subset='id', keep='first')
                        cities_combined_data_database.append(df_city_database)
                        df_city_dashboard = pd.concat(city_rooms_data, ignore_index=True).drop_duplicates(subset='id', keep='first')
                        rooms_combined_data_dashboard.append(df_city_dashboard)
                        df_distances = pd.concat(hotels_distances_data, ignore_index=True).drop_duplicates(subset='id', keep='first')
                        hotels_combined_distances_data.append(df_distances)
                        df_city_statistic = pd.concat(city_hotels_statistic, ignore_index=True).drop_duplicates(subset='id', keep='first')
                        cities_combined_statistic.append(df_city_statistic)
                    else:
                        await send_telegram_message(f"Нет данных для города {city}")
                except Exception as e:
                    logging.error(f"Ошибка при обработке города {city}: {e}")
                    await send_telegram_message(f"Ошибка при обработке города {city}: {e}")

        if cities_combined_data_database:
            df_all_database = pd.concat(cities_combined_data_database, ignore_index=True).drop_duplicates(subset='id', keep='first')
            df_all_rooms = pd.concat(rooms_combined_data_dashboard, ignore_index=True).drop_duplicates(subset='id', keep='first')
            df_all_distances = pd.concat(hotels_combined_distances_data, ignore_index=True).drop_duplicates(subset='id', keep='first')
            df_all_statistic = pd.concat(cities_combined_statistic, ignore_index=True).drop_duplicates(subset='id', keep='first')
            
            rooms_data_file_path = os.path.join(ROOMS_DATA_PATH, f"rooms_data_{start_date}.csv")
            df_all_rooms.to_csv(rooms_data_file_path, index=False)
            hotels_statistic_file_path = os.path.join(HOTELS_STATISTICS_PATH, f"hotels_statistic_{start_date}.csv")
            df_all_statistic.to_csv(hotels_statistic_file_path, index=False)
            
            update_file_data(df_all_database, DATABASE_FILE_PATH)
            update_file_data(df_all_distances, DISTANCES_FILE_PATH)
            await send_telegram_message(f"Парсинг завершён, файлы базы данных и дистанций обновлены")
            
            hotels_statistic_table_name = "hotels_statistics"
            database_table_name = "af_all_2024"
            
            driver_config = ydb.DriverConfig(ENDPOINT, DATABASE, credentials=CREDENTIALS)
            
            upsert_data_to_ydb(driver_config, hotels_statistic_file_path, hotels_statistic_table_name, "hotels_statistics", extract_date)
            await send_telegram_message(f"Таблица {hotels_statistic_table_name} обновлена/создана")
            upsert_data_to_ydb(driver_config, DATABASE_FILE_PATH, database_table_name, "databases", extract_date)
            await send_telegram_message(f"Таблица {database_table_name} обновлена/создана")
            
    except Exception as e:
        logging.error(f"Ошибка при парсинге всех городов: {e}")
        if bot:
            await send_telegram_message(f"Ошибка при парсинге всех городов: {e}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_101hotels_async())
