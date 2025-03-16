import json
import os
import pandas as pd
import datetime as dt
import logging
import asyncio
import aiohttp
from dotenv import load_dotenv
from telegram import Bot
import sys

import ydb
import ydb.iam
import subprocess

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()
CITIES = os.getenv("CITIES", "").split(",")
ROOMS_DATA_PATH = os.getenv("ROOMS_DATA_PATH", "./tables/rooms_data")
HOTELS_STATISTICS_PATH = os.getenv("HOTELS_STATISTICS_PATH", "./tables/hotels_statistics")
DATABASE_FILE_PATH = os.getenv("DATABASE_FILE", "./databases/af_all_2024.csv")
DISTANCES_FILE_PATH = os.getenv("CITY_CENTER_AND_SEA_DISTANCES_FILE", "./databases/hotels_city_center_and_sea_distances.csv")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "7647110076:AAGCbk8JQ2YlY8OwqcHfGDWEiUHoWNtzOpw")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "813117111")

ENDPOINT = os.getenv("YDB_ENDPOINT", "grpcs://ydb.serverless.yandexcloud.net:2135")
DATABASE = os.getenv("YDB_DATABASE", "/ru-central1/b1gs7dv1mdmlibsrgfcg/etnsktaerd45usdot87m")

SA_KEY_FILE = os.getenv("SA_KEY_FILE")

LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "./logs/logging.log")
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Скрипт запущен")

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID else None

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

def extract_hotel_data(hotel):
    try:
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
            "district": hotel.get("district_id", None),
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

def extract_room_data(room, hotel_id):
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
            "hotel_id": f"{hotel_id}_101hotels"
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

def extract_statistic_data(rooms, hotel_id, rooms_num):
    try:
        free_rooms_amount = sum(room.get("free", 0) if room.get("single_bed", 0) == 0 else 1 for room in rooms)
        rooms_occupancy_percent = free_rooms_amount * 100 / rooms_num
        max_capacity = sum(room.get("free", 0) * room.get("capacity", 0) for room in rooms)
        return {
            "id": f"{hotel_id}_101hotels",
            "free_rooms_amount": free_rooms_amount,
            "rooms_occupancy_percent": rooms_occupancy_percent,
            "max_capacity": max_capacity
        }
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных для номера отеля {hotel_id}: {e}")
        return {}

async def get_hotels_info(session, city, start_date, end_date, page):
    try:
        url = f"https://api.101hotels.com/hotel/available/city/russia/{city}?sort_direction=desc&in={start_date}&out={end_date}&adults=1&page={page}"
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

            hotels_info_database = pd.json_normalize([extract_hotel_data(hotel) for hotel in hotels])
            if hotels_info_database.empty:
                logging.warning(f"Нет данных для города {city}, страница {page}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), hotels_amount
            
            hotels_rooms_info = pd.json_normalize([extract_room_data(room, hotel.get("id", "")) for hotel in hotels for room in hotel.get("rooms", [])])
            city_distances_info = pd.json_normalize([extract_distances_data(hotel) for hotel in hotels])
            hotels_statistic = pd.json_normalize([extract_statistic_data(hotel.get("rooms", [{}]), hotel.get("id", ""), hotel.get("rooms_num", 0)) for hotel in hotels])
            return hotels_info_database, hotels_rooms_info, city_distances_info, hotels_statistic, hotels_amount

    except Exception as e:
        logging.error(f"Ошибка при обработке {city}, страница {page}: {e}")
        return pd.DataFrame(), 0

def get_tables_queries(table_name, dir):
    drop_table_query = f"""
    PRAGMA TablePathPrefix("{DATABASE}/{dir}");
    DROP TABLE {table_name};
    """
        
    if dir == "rooms_data":
        create_table_query = f"""
        PRAGMA TablePathPrefix("{DATABASE}/{dir}");

        CREATE TABLE {table_name} (
            id Int64 NOT NULL,
            k Int64,
            name Utf8,
            capacity Int16,
            additional Int16,
            single_bed Int16,
            bathroom Int16,
            type_name Utf8,
            type_id Int64,
            free Int16,
            bed_categories Utf8,
            children_extras Utf8,
            placements Utf8,
            min_price Float,
            miles Int64,
            hotel_id Utf8 NOT NULL,
            PRIMARY KEY (id)
        );
        """
    elif dir == "hotels_statistics":
        create_table_query = f"""
        PRAGMA TablePathPrefix("{DATABASE}/{dir}");

        CREATE TABLE {table_name} (
            id Utf8 NOT NULL,
            free_rooms_amount Int16,
            rooms_occupancy_percent Float,
            max_capacity Int16,
            PRIMARY KEY (id)
        );
        """
        
    return drop_table_query, create_table_query
    
def create_ydb_table(drop_table_query, create_table_query, table_name):    
    driver_config = ydb.DriverConfig(
        endpoint=ENDPOINT,
        database=DATABASE,
        credentials=ydb.iam.ServiceAccountCredentials.from_file(SA_KEY_FILE)
    )
    
    with ydb.Driver(driver_config) as driver:
        driver.wait(fail_fast=True, timeout=20)
        session = driver.table_client.session().create()
        
        try:
            session.execute_scheme(drop_table_query)
            logging.info(f"Таблица {table_name} удалена перед созданием новой.")
        except Exception as e:
            logging.warning(f"Не удалось удалить таблицу {table_name} (возможно, её не существует): {e}")

        try:
            session.execute_scheme(create_table_query)
            logging.info(f"Таблица {table_name} успешно создана.")
        except Exception as e:
            logging.error(f"Не удалось создать таблицу {table_name}: {e}")
            
def import_csv_to_ydb(csv_file_to_import, table_name, ydb_dir):
    command = [
        "ydb",
        "-e", ENDPOINT,
        "-d", DATABASE,
        "--sa-key-file", AUTH_TOKEN_PATH,
        "import", "file", "csv",
        "-p", f"{DATABASE}/{ydb_dir}/{table_name}",
        "--header", csv_file_to_import
    ]
    try:
        subprocess.run(command, check=True, text=True, capture_output=True)
        logging.info("Импорт CSV В YDB произошёл успешно")
    except subprocess.CalledProcessError as e:
        logging.error("Не удалось импортировать CSV В YDB:", e.stderr)

async def parse_101hotels_async():
    try:
        today = dt.date.today()
        next_day = today + dt.timedelta(days=1)
        start_date = today.strftime("%d.%m.%Y")
        end_date = next_day.strftime("%d.%m.%Y")

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
                        hotels_info_database, hotels_rooms_info, city_distances_info, hotels_statistic, hotels_amount = await get_hotels_info(session, city, start_date, end_date, page)
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
            
            rooms_data_table_name = f"rooms_data_{today.strftime('%d_%m_%Y')}"
            hotels_statistic_table_name = f"hotels_statistic_{today.strftime('%d_%m_%Y')}"
            
            drop_table_query, create_table_query = get_tables_queries(rooms_data_table_name, dir="rooms_data")
            create_ydb_table(drop_table_query, create_table_query, table_name=rooms_data_table_name)
            
            drop_table_query, create_table_query = get_tables_queries(hotels_statistic_table_name, dir="hotels_statistics")
            create_ydb_table(drop_table_query, create_table_query, table_name=hotels_statistic_table_name)
 
            import_csv_to_ydb(rooms_data_file_path, rooms_data_table_name, ydb_dir="rooms_data")
            await send_telegram_message(f"Таблица {rooms_data_table_name} создана")
            import_csv_to_ydb(hotels_statistic_file_path, hotels_statistic_table_name, ydb_dir="hotels_statistics")
            await send_telegram_message(f"Таблица {hotels_statistic_table_name} создана")
            
    except Exception as e:
        logging.error(f"Ошибка при парсинге всех городов: {e}")
        if bot:
            await send_telegram_message(f"Ошибка при парсинге всех городов: {e}")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_101hotels_async())
