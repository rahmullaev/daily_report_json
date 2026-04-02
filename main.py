#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import logging
import requests
import json
import argparse
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict
from zoneinfo import ZoneInfo
from prettytable import PrettyTable

# ───────── НАСТРОЙКИ ─────────
USE_LIVE = True
MODE = "FULL_REPORT"
PARIS = ZoneInfo("Europe/Paris")

# API endpoints
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
CURRENT_URL = "https://api.openweathermap.org/data/2.5/weather"

# 🔑 Секретные переменные из окружения
OW_API_KEY = os.getenv("OW_API_KEY", "")
CITY_ID = os.getenv("CITY_ID", "2994160")

# Telegram
USER_IDS = [int(uid) for uid in os.getenv("TELEGRAM_USER_IDS", "").split(",") if uid]
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Kaspi курсы
KASPI_URL = "https://guide.kaspi.kz/client/api/v2/intgr/currency/rate/aggregate"
CURRENCIES = ("USD", "EUR")
TIMEOUT_API = 10

# 📅 Настройки хранения истории - храним последние 10 записей
HISTORY_MAX_ENTRIES = 10

# Пути к файлам
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, "data", "weather_data.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "Log.log")

BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "gIsAuthorized": "false",
    "gIsInnerRequest": "false",
    "gIsMobileApp": "false",
    "gLanguage": "ru",
    "gRole": "1",
    "gSystem": "kkz",
}

ENABLE_ALERTS = True
GITHUB_JSON_URL = "https://rahmullaev.github.io/daily_report_json/weather_data.json"

# Проверка переменных
print("=" * 60)
print("🔍 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
print(f"OW_API_KEY: {'✅ УСТАНОВЛЕН' if OW_API_KEY else '❌ НЕ УСТАНОВЛЕН'} (длина: {len(OW_API_KEY)})")
print(f"TELEGRAM_BOT_TOKEN: {'✅ УСТАНОВЛЕН' if TELEGRAM_BOT_TOKEN else '❌ НЕ УСТАНОВЛЕН'}")
print(f"TELEGRAM_USER_IDS: {USER_IDS if USER_IDS else '❌ НЕ УСТАНОВЛЕН'}")
print(f"CITY_ID: {CITY_ID}")
print(f"📊 Хранение истории: последние {HISTORY_MAX_ENTRIES} записей")
print("=" * 60)

# Инициализация бота Telegram

try:
    from telegram import Bot, ParseMode
    bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
except Exception as e:
    print(f"Ошибка инициализации бота: {e}")
    bot = None

# ─── ЛОГИРОВАНИЕ ──────────────────────────────────────────────────────────────
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s]-%(levelname)s-%(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

log = setup_logging()

# =============================================================================================

def load_data() -> dict:
    """Загрузка данных из локального JSON"""
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data:
                    temp_count = len(data.get('temp_history', []))
                    curr_count = len(data.get('currency_history', []))
                    log.info(f"✅ Загружено {temp_count} записей температуры, {curr_count} записей курсов")
                return data
        log.info("📁 Файл данных не найден, создаю новый")
        return {}
    except Exception as e:
        log.error(f"Ошибка загрузки данных: {e}")
        return {}

def save_data(data: dict):
    """Сохраняет данные в локальный JSON"""
    try:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        temp_count = len(data.get('temp_history', []))
        curr_count = len(data.get('currency_history', []))
        log.info(f"💾 Данные сохранены: {temp_count} записей температуры, {curr_count} записей курсов")
    except Exception as e:
        log.error(f"Ошибка сохранения: {e}")

def reindex_history(history_list: List[dict]) -> List[dict]:
    """Пересчет индексов в истории (1 - самая старая, N - самая новая)"""
    for idx, entry in enumerate(history_list, 1):
        entry["index"] = idx
    return history_list

def get_weather(use_live: bool = USE_LIVE) -> Optional[Dict[str, Any]]:
    """Получение данных о погоде"""
    if not use_live:
        log.info("📂 Использую сохраненные данные погоды")
        data = load_data().get("weather", {})
        return data if data else None

    try:
        if not OW_API_KEY:
            log.error("❌ OW_API_KEY не установлен!")
            return None
            
        params = {"id": CITY_ID, "units": "metric", "lang": "ru", "APPID": OW_API_KEY}
        log.info(f"🌤️ Запрос погоды для города {CITY_ID}")
        
        cur = requests.get(CURRENT_URL, params=params, timeout=10)
        if cur.status_code != 200:
            log.error(f"Ошибка погоды: {cur.status_code}")
            return None
            
        cur_data = cur.json()
        
        fc = requests.get(FORECAST_URL, params=params, timeout=10)
        if fc.status_code != 200:
            log.error(f"Ошибка прогноза: {fc.status_code}")
            return None
            
        fc_data = fc.json()

        # Прогноз на 3 дня
        today = datetime.now(PARIS).date()
        buckets = defaultdict(list)
        for itm in fc_data.get("list", []):
            dt = datetime.utcfromtimestamp(itm["dt"]).date()
            buckets[dt.toordinal()].append(itm)

        forecast = {}
        day_names = ["Пн.", "Вт.", "Ср.", "Чт.", "Пт.", "Сб.", "Вс."]
        for off in range(1, 4):
            tgt = today + timedelta(days=off)
            bucket = buckets.get(tgt.toordinal(), [])
            if bucket:
                mid = min(bucket, key=lambda x: abs(datetime.utcfromtimestamp(x["dt"]).hour - 12))
                loc = datetime.fromtimestamp(mid["dt"], PARIS)
                forecast.update({
                    f"day_name_{off}": day_names[loc.weekday()],
                    f"temp_{off}": f"{mid['main']['temp']:+.0f}",
                    f"icon_{off}": mid["weather"][0]["icon"],
                    f"descr_{off}": mid["weather"][0]["description"],
                })

        weather_data = {
            "cur_temp": cur_data["main"]["temp"],
            "cur_icon": cur_data["weather"][0]["icon"],
            "cur_descr": cur_data["weather"][0]["description"],
            "last_upd": datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S"),
            **forecast
        }

        log.info(f"✅ Погода: {weather_data['cur_temp']}°C, {weather_data['cur_descr']}")
        return weather_data
        
    except Exception as e:
        log.error(f"Ошибка получения погоды: {e}")
        return None

def get_namaz(use_live: bool = True) -> Dict[str, str]:
    """Получение времени намазов"""
    if not use_live:
        data = load_data().get("namaz", {})
        return data if data else {}

    try:
        params = {"latitude": 49.1193, "longitude": 6.1757, "method": 3}
        r = requests.get("https://api.aladhan.com/v1/timings", params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        timings = data.get("data", {}).get("timings", {})

        namaz_mapping = {
            "Fajr": "Фаджр", "Sunrise": "Восход", "Dhuhr": "Зухр",
            "Asr": "Аср", "Maghrib": "Магриб", "Isha": "Иша"
        }

        result = {rus: timings.get(eng, "") for eng, rus in namaz_mapping.items()}
        result["last_upd"] = datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")

        log.info(f"✅ Намазы получены")
        return result

    except Exception as e:
        log.error(f"Ошибка получения намазов: {e}")
        return {}

def get_rates(use_live: bool = USE_LIVE) -> Dict[str, Any]:
    """Получение курсов валют"""
    if not use_live:
        data = load_data().get("currency_rates", {})
        return data if data else {}

    try:
        headers = {**BASE_HEADERS, "User-Agent": "Mozilla/5.0"}
        payload = {
            "use_type": "32",
            "currency_codes": list(CURRENCIES),
            "rate_types": ["SALE", "BUY"]
        }

        response = requests.post(KASPI_URL, headers=headers, json=payload, timeout=TIMEOUT_API)
        
        if response.status_code != 200:
            log.error(f"Kaspi API ошибка: {response.status_code}")
            return {}
            
        body = response.json().get("body", [])
        
        if not body:
            log.warning("Kaspi API вернул пустой ответ")
            return {}

        rates = {}
        for it in body:
            if it["currency"] in CURRENCIES:
                rates[it["currency"]] = {
                    "buy": float(it["buy"]),
                    "sale": float(it["sale"])
                }
        
        current_time = datetime.now(PARIS)
        rates["last_upd"] = current_time.strftime("%d.%m.%Y %H:%M:%S")
        
        log.info(f"✅ Курсы: USD={rates.get('USD', {}).get('buy')}, EUR={rates.get('EUR', {}).get('buy')}")
        return rates

    except Exception as e:
        log.error(f"Ошибка получения курсов: {e}")
        return {}

def load_remote_json() -> dict:
    """Загрузка данных с GitHub"""
    try:
        r = requests.get(GITHUB_JSON_URL, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"GitHub недоступен: {e}")
        return {}

def get_previous_currency() -> dict:
    """Получение старых курсов с GitHub для алертов"""
    try:
        remote = load_remote_json()
        if remote and remote.get("currency_rates"):
            return remote["currency_rates"]
        return {}
    except Exception:
        return {}

def check_currency_changes(new_rates: dict, old_rates: dict) -> dict:
    """Проверка изменений курсов"""
    if not old_rates or not new_rates:
        return {}

    alerts = {}
    THRESHOLD = 0.1

    for cur in CURRENCIES:
        new_data = new_rates.get(cur, {})
        old_data = old_rates.get(cur, {})
        
        if not old_data:
            continue

        for typ in ["buy", "sale"]:
            try:
                new_val = float(new_data.get(typ, 0))
                old_val = float(old_data.get(typ, 0))
                
                if new_val == 0 or old_val == 0:
                    continue
                    
                diff = new_val - old_val
                
                if abs(diff) >= THRESHOLD:
                    if cur not in alerts:
                        alerts[cur] = []
                    alerts[cur].append({
                        "type": typ,
                        "old": old_val,
                        "new": new_val,
                        "diff": diff
                    })
                    log.info(f"📊 Изменение {cur} {typ}: {old_val:.2f} -> {new_val:.2f} (Δ={diff:+.2f})")
            except Exception:
                continue

    return alerts

def send_currency_alerts(alerts: dict):
    """Отправка алертов"""
    if not alerts or not ENABLE_ALERTS or not bot:
        return

    for cur, changes in alerts.items():
        table = PrettyTable()
        table.field_names = ["Операция", "Было", "Стало", "Δ"]
        table.align = "r"
        table.align["Операция"] = "l"

        for ch in changes:
            emoji = "📈" if ch["diff"] > 0 else "📉"
            op = "Покупка" if ch["type"] == "buy" else "Продажа"
            table.add_row([f"{op} {emoji}", f"{ch['old']:.2f}", f"{ch['new']:.2f}", f"{ch['diff']:+.2f}"])

        msg = (
            f"💱 <b>Изменение курса {cur}</b>\n"
            f"<pre>{table}</pre>\n"
            f"🕒 {datetime.now(PARIS).strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        try:
            for user_id in USER_IDS:
                # Используем синхронный вызов
                bot.send_message(
                    chat_id=user_id, 
                    text=msg, 
                    parse_mode=ParseMode.HTML, 
                    timeout=15
                )
            log.info(f"✅ Алерт для {cur} отправлен")
        except Exception as e:
            log.error(f"Ошибка отправки алерта: {e}")

def main():
    log.info(f"🚀 Запуск, хранение: последние {HISTORY_MAX_ENTRIES} записей")
    
    try:
        if MODE == "FULL_REPORT":
            log.info("📡 Получение свежих данных...")
            
            # Получаем данные
            weather = get_weather(use_live=True)
            namaz = get_namaz(use_live=True)
            rates = get_rates(use_live=True)
            
            # Загружаем существующие данные
            data = load_data()
            
            # Обновляем погоду
            if weather:
                data["weather"] = weather
                
                # Добавляем в историю температуры (FIFO)
                if "temp_history" not in data:
                    data["temp_history"] = []
                
                # Если достигнут лимит, удаляем самую старую
                if len(data["temp_history"]) >= HISTORY_MAX_ENTRIES:
                    removed = data["temp_history"].pop(0)
                    log.info(f"🗑️ Удалена запись температуры от {removed.get('timestamp', '?')}")
                
                # Добавляем новую
                data["temp_history"].append({
                    "temp": weather["cur_temp"],
                    "icon": weather["cur_icon"],
                    "descr": weather["cur_descr"],
                    "timestamp": weather["last_upd"]
                })
                log.info(f"➕ Добавлена температура: {weather['cur_temp']}°C (всего: {len(data['temp_history'])})")
            
            # Обновляем намазы
            if namaz:
                data["namaz"] = namaz
            
            # Обновляем курсы и историю
            if rates:
                data["currency_rates"] = rates
                
                # Добавляем в историю курсов (FIFO)
                if "currency_history" not in data:
                    data["currency_history"] = []
                
                # Если достигнут лимит, удаляем самую старую
                if len(data["currency_history"]) >= HISTORY_MAX_ENTRIES:
                    removed = data["currency_history"].pop(0)
                    log.info(f"🗑️ Удалена запись курсов от {removed.get('timestamp', '?')}")
                
                # Добавляем новую
                history_entry = {
                    "timestamp": rates["last_upd"],
                    "rates": {}
                }
                for cur in CURRENCIES:
                    if cur in rates:
                        history_entry["rates"][cur] = {
                            "buy": rates[cur]["buy"],
                            "sale": rates[cur]["sale"]
                        }
                data["currency_history"].append(history_entry)
                log.info(f"➕ Добавлены курсы (всего: {len(data['currency_history'])})")
            
            # Пересчитываем индексы
            if "temp_history" in data:
                data["temp_history"] = reindex_history(data["temp_history"])
            if "currency_history" in data:
                data["currency_history"] = reindex_history(data["currency_history"])
            
            # Формируем финальный JSON
            result = {
                "weather": data.get("weather", {}),
                "namaz": data.get("namaz", {}),
                "currency_rates": data.get("currency_rates", {}),
                "temp_history": data.get("temp_history", []),
                "currency_history": data.get("currency_history", [])
            }
            
            # Сохраняем
            save_data(result)
            
            # Алерты
            if rates and rates.get("USD"):
                old_rates = get_previous_currency()
                if old_rates:
                    alerts = check_currency_changes(rates, old_rates)
                    if alerts:
                        send_currency_alerts(alerts)
            
            log.info(f"✅ Отчет готов: {len(result['temp_history'])} записей температуры, {len(result['currency_history'])} записей курсов")
            
        elif MODE == "UPD_NAMAZ":
            namaz = get_namaz(use_live=True)
            if namaz:
                data = load_data()
                data["namaz"] = namaz
                save_data(data)
                log.info("✅ Намазы обновлены")
                
        elif MODE == "UPD_CURRENCY":
            rates = get_rates(use_live=True)
            if rates:
                data = load_data()
                data["currency_rates"] = rates
                
                if "currency_history" not in data:
                    data["currency_history"] = []
                
                if len(data["currency_history"]) >= HISTORY_MAX_ENTRIES:
                    data["currency_history"].pop(0)
                
                history_entry = {
                    "timestamp": rates["last_upd"],
                    "rates": {}
                }
                for cur in CURRENCIES:
                    if cur in rates:
                        history_entry["rates"][cur] = {
                            "buy": rates[cur]["buy"],
                            "sale": rates[cur]["sale"]
                        }
                data["currency_history"].append(history_entry)
                data["currency_history"] = reindex_history(data["currency_history"])
                save_data(data)
                log.info("✅ Курсы обновлены")
                
                old_rates = get_previous_currency()
                if old_rates:
                    alerts = check_currency_changes(rates, old_rates)
                    if alerts:
                        send_currency_alerts(alerts)
        
        log.info("✅ Завершено")
        
    except Exception as e:
        log.error(f"❌ Ошибка: {e}")
        log.error(traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["FULL_REPORT", "UPD_NAMAZ", "UPD_CURRENCY"], 
                       default="FULL_REPORT", help="Режим работы")
    parser.add_argument("--use-live", action="store_true", help="Использовать живые данные")
    parser.add_argument("--max-entries", type=int, help="Максимальное количество записей")
    
    args = parser.parse_args()
    
    MODE = args.mode
    
    if args.use_live:
        USE_LIVE = True
    
    if args.max_entries:
        HISTORY_MAX_ENTRIES = args.max_entries
        log.info(f"📊 Установлено хранение: {HISTORY_MAX_ENTRIES} записей")
    
    main()