#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, platform, logging, requests, json, argparse, time
from io import BytesIO
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont  # ← Здесь используется Pillow
from telegram import Bot, ParseMode, InputMediaPhoto  # ParseMode здесь!
from telegram.error import TelegramError
from prettytable import PrettyTable
import re


# ───────── НАСТРОЙКИ ─────────
USE_LIVE = True
MODE = "FULL_REPORT"
PARIS = ZoneInfo("Europe/Paris")
DAY_NAME_RU = ["Пн.", "Вт.", "Ср.", "Чт.", "Пт.", "Сб.", "Вс."]

# API endpoints
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
CURRENT_URL = "https://api.openweathermap.org/data/2.5/weather"

# 🔑 Секретные переменные теперь из окружения
OW_API_KEY = os.getenv("OW_API_KEY", "")
CITY_ID = os.getenv("CITY_ID", "2994160")  # дефолт — Париж

# Графика
WIDTH = 500
ICON_SIZE = 32

# Telegram
USER_IDS = [int(uid) for uid in os.getenv("TELEGRAM_USER_IDS", "883019358").split(",")]
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Kaspi курсы
KASPI_URL = "https://guide.kaspi.kz/client/api/v2/intgr/currency/rate/aggregate"
MONITOR_CURRENCY = "USD"
OPERATION_TYPE = "sale"
CURRENCIES = ("USD", "EUR")
TIMEOUT_API = 10

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

# Инициализация бота Telegram
try:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
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



def send_error_to_telegram(error_msg: str):
    """Отправка ошибок в Telegram"""
    try:
        log.error(error_msg)
        tg_msg(f"🚨 Ошибка в скрипте:\n{error_msg}")
    except Exception as e:
        log.error(f"Ошибка при отправке ошибки в Telegram: {e}")

# ─── ОСНОВНЫЕ ФУНКЦИИ ────────────────────────────────────────────────────────
def should_run(hour: int, update_only: bool) -> bool:
    """Проверяет можно ли выполнять запросы в текущее время"""
    if not update_only:
        return True
    return 5 <= hour <= 15

def get_weather(use_live: bool = USE_LIVE) -> Optional[Dict[str, Any]]:
    """Получение данных о погоде"""
    if not use_live:
        data = load_data().get("weather", {})
        if data:
            last_upd = data.get("last_upd_weather", "Из файла (нет даты)")
            return data | {"last_upd_weather": last_upd}
        return None

    try:
        params = {"id": CITY_ID, "units": "metric", "lang": "ru", "APPID": OW_API_KEY}
        cur = requests.get(CURRENT_URL, params=params, timeout=10).json()
        fc = requests.get(FORECAST_URL, params=params, timeout=10).json()

        today = datetime.now(PARIS).date()
        buckets = defaultdict(list)
        for itm in fc.get("list", []):
            buckets[datetime.utcfromtimestamp(itm["dt"]).date().toordinal()].append(itm)

        forecast = {}
        for off in range(1, 4):
            tgt = today + timedelta(days=off)
            bucket = buckets.get(tgt.toordinal(), [])
            if not bucket: continue
            mid = min(bucket, key=lambda x: abs(datetime.utcfromtimestamp(x["dt"]).hour-12))
            loc = datetime.fromtimestamp(mid["dt"], PARIS)
            suf = str(off)
            forecast |= {
                f"day_name_{suf}": DAY_NAME_RU[loc.weekday()],
                f"temp_{suf}": f"{mid['main']['temp']:+.0f}",
                f"icon_{suf}": mid["weather"][0]["icon"],
                f"descr_{suf}": mid["weather"][0]["description"],
            }

        new_data = {
            "cur_temp": cur["main"]["temp"],
            "cur_icon": cur["weather"][0]["icon"],
            "cur_descr": cur["weather"][0]["description"],
            "last_upd_weather": datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")
        } | forecast

        store = load_data()
        store["weather"] = new_data
        save_data(store)
        return new_data
    except Exception as e:
        error_msg = f"Ошибка получения погоды: {e}"
        send_error_to_telegram(error_msg)
        return None

def get_namaz(use_live: bool = USE_LIVE) -> Dict[str, str]:
    """Получение времени намазов с Mawaqit"""
    if not use_live:
        data = load_data().get("namaz", {})
        if data:
            last_upd = data.get("last_upd_namaz", "Из файла (нет даты)")
            return data | {"last_upd_namaz": last_upd}
        return {}

    try:
        url = "https://mawaqit.net/ru/grande-mosquee-de-metz-metz"
        r = requests.get(url, timeout=10)
        html = r.text

        match = re.search(r'var\s+confData\s*=\s*({.*?});', html, re.DOTALL)
        if not match:
            raise Exception("Не удалось найти confData на странице Mawaqit")

        data_json = json.loads(match.group(1))
        times = data_json.get("times", {})
        namaz_times = {}

        if isinstance(times, dict):
            # Сопоставление с русскими названиями как в оригинале
            namaz_mapping = {
                "fajr": "Фаджр",
                "shuruq": "Восход", 
                "dhuhr": "Зухр",
                "asr": "Аср",
                "maghrib": "Магриб",
                "isha": "Иша"
            }
            
            for eng_name, rus_name in namaz_mapping.items():
                if eng_name in times:
                    namaz_times[rus_name] = times[eng_name]
                    
        elif isinstance(times, list) and len(times) >= 5:
            # Для формата списка
            namaz_times = {
                "Фаджр": times[0],
                "Зухр": times[1],
                "Аср": times[2],
                "Магриб": times[3],
                "Иша": times[4],
                "Восход": data_json.get("shuruq", "")
            }

        # Убедимся, что все обязательные поля есть
        required_names = ["Фаджр", "Восход", "Зухр", "Аср", "Магриб", "Иша"]
        for name in required_names:
            if name not in namaz_times:
                namaz_times[name] = ""

        result = {
            **namaz_times,
            "last_upd_namaz": datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")
        }

        store = load_data()
        store["namaz"] = result
        save_data(store)
        return result
        
    except Exception as e:
        error_msg = f"Ошибка получения намазов с Mawaqit: {e}"
        send_error_to_telegram(error_msg)
        return {}

def get_rates(force_update: bool = False, use_live: bool = USE_LIVE) -> Dict[str, Dict[str, str]]:
    """Получение курсов валют"""
    if not use_live:
        data = load_data().get("currency_rates", {})
        if data:
            last_upd = data.get("last_upd_currency", "Из файла (нет даты)")
            return data | {"last_upd_currency": last_upd}
        return {"USD": {"buy": "470.5", "sale": "475.0"}, "EUR": {"buy": "490.0", "sale": "495.0"}}

    try:
        hdrs = {**BASE_HEADERS, "User-Agent": "Mozilla/5.0"}
        payload = {"use_type": "32", "currency_codes": list(CURRENCIES), "rate_types": ["SALE", "BUY"]}
        body = requests.post(KASPI_URL, headers=hdrs, json=payload, timeout=TIMEOUT_API).json()["body"]
        
        res = {it["currency"]: {"buy": it["buy"], "sale": it["sale"]} for it in body if it["currency"] in CURRENCIES}
        res["last_upd_currency"] = datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")
        
        store = load_data()
        store["currency_rates"] = res
        save_data(store)
        
        return res
    except Exception as e:
        error_msg = f"Ошибка получения курсов: {e}"
        log.error(error_msg)
        return {}

def process_currency_history(rates: Dict[str, Dict[str, str]]) -> Dict[str, List[Dict[str, float]]]:
    """Обработка истории курсов с табличным выводом через prettytable"""
    try:
        if not rates or MONITOR_CURRENCY not in rates:
            return {}

        data = load_data()
        now = datetime.now(PARIS)
        current_date = now.strftime("%d.%m.%Y")
        full_datetime = now.strftime("%d.%m.%Y %H:%M:%S")

        if "currency_history" not in data:
            data["currency_history"] = {}

        if "last_history_update" not in data or data["last_history_update"] != current_date:
            data["currency_history"] = {MONITOR_CURRENCY: []}
            data["last_history_update"] = current_date
            log.info(f"История курсов сброшена для нового дня {current_date}")

        current_buy = float(rates[MONITOR_CURRENCY]["buy"])
        current_sale = float(rates[MONITOR_CURRENCY]["sale"])
        history = data["currency_history"].get(MONITOR_CURRENCY, [])

        # Проверяем, есть ли предыдущая запись
        if history:
            last_entry = history[-1]
            # Если курсы не изменились - пропускаем запись
            if (last_entry.get("buy") == current_buy and 
                last_entry.get("sale") == current_sale):
                log.debug("Курсы не изменились, пропускаем запись")
                return data["currency_history"]

        # Создаем новую запись
        new_entry = {
            "t": full_datetime,
            "buy": current_buy,
            "sale": current_sale
        }

        # Если есть предыдущие данные - формируем таблицу изменений
        if history:
            prev = history[-1]
            prev_buy = prev.get("buy", current_buy)
            prev_sale = prev.get("sale", current_sale)
            
            # Создаем таблицу
            table = PrettyTable()
            table.field_names = ["Операция", "Предыдущий", "Текущий", "Изменение"]
            table.align = "r"
            table.align["Операция"] = "l"
            table.header = True
            
            # Добавляем строки с эмодзи
            buy_change = current_buy - prev_buy
            buy_emoji = "📈" if buy_change > 0 else "📉" if buy_change < 0 else "➖"
            table.add_row([
                f"Покупка {buy_emoji}",
                f"{prev_buy:.2f}", 
                f"{current_buy:.2f}",
                f"{'+' if buy_change > 0 else ''}{buy_change:.2f}"
            ])
            
            sale_change = current_sale - prev_sale
            sale_emoji = "📈" if sale_change > 0 else "📉" if sale_change < 0 else "➖"
            table.add_row([
                f"Продажа {sale_emoji}",
                f"{prev_sale:.2f}",
                f"{current_sale:.2f}",
                f"{'+' if sale_change > 0 else ''}{sale_change:.2f}"
            ])
            
            # Формируем сообщение
            message = (
                f"<b>🔔 Изменение курса {MONITOR_CURRENCY}</b>\n"
                f"<pre>{table}</pre>\n"
                f"🕒 {now.strftime('%d.%m.%Y %H:%M:%S')}"
            )
            
            # Отправляем сообщение
            tg_msg(message, parse_mode=ParseMode.HTML)
            log.info(f"Обновление курса:\n{table}")

        # Добавляем новую запись в историю
        history.append(new_entry)
        data["currency_history"][MONITOR_CURRENCY] = history[-24:]  # Сохраняем только последние 24 записи
        save_data(data)

        return data["currency_history"]

    except Exception as e:
        error_msg = f"Ошибка обработки истории: {e}"
        send_error_to_telegram(error_msg)
        log.error(error_msg, exc_info=True)
        return {}

def tg_msg(txt: str, parse_mode: str = None, additional_ids: list = None):
    """Отправка сообщений в Telegram"""
    try:
        if not bot:
            log.error("Бот Telegram не инициализирован")
            return 0
            
        recipients = USER_IDS.copy()
        if additional_ids:
            recipients.extend(additional_ids)
            
        if not recipients:
            log.error("Список получателей пуст")
            return 0
            
        max_attempts = 3
        success_count = 0
        
        for user_id in recipients:
            for attempt in range(max_attempts):
                try:
                    bot.send_message(
                        chat_id=user_id,
                        text=txt,
                        parse_mode=parse_mode,
                        timeout=15
                    )
                    success_count += 1
                    break
                except TelegramError as e:
                    if attempt == max_attempts - 1:
                        log.error(f"Ошибка отправки пользователю {user_id} после {max_attempts} попыток: {e}")
                    else:
                        time.sleep(2)
                except Exception as e:
                    log.error(f"Неожиданная ошибка при отправке пользователю {user_id}: {e}")
                    break
        
        log.info(f"Сообщение отправлено {success_count} из {len(recipients)} пользователей")
        return success_count
        
    except Exception as e:
        log.error(f"Критическая ошибка в функции tg_msg: {e}")
        return 0

def tg_photo(path: str, caption: str = None, additional_ids: list = None):
    """Отправка фото в Telegram"""
    try:
        if not bot:
            log.error("Бот Telegram не инициализирован")
            return 0
            
        recipients = USER_IDS.copy()
        if additional_ids:
            recipients.extend(additional_ids)
            
        if not recipients:
            log.error("Список получателей пуст")
            return 0
            
        max_attempts = 3
        success_count = 0
        file_processed = False
        
        try:
            with open(path, "rb") as photo_file:
                photo_data = photo_file.read()
                
                for user_id in recipients:
                    for attempt in range(max_attempts):
                        try:
                            bot.send_photo(
                                chat_id=user_id,
                                photo=BytesIO(photo_data),
                                caption=caption,
                                timeout=20
                            )
                            success_count += 1
                            break
                        except TelegramError as e:
                            if attempt == max_attempts - 1:
                                log.error(f"Ошибка отправки фото пользователю {user_id} после {max_attempts} попыток: {e}")
                            else:
                                time.sleep(2)
                        except Exception as e:
                            log.error(f"Неожиданная ошибка при отправке фото пользователю {user_id}: {e}")
                            break
                
                file_processed = True
                
        except Exception as e:
            log.error(f"Ошибка обработки файла фото: {e}")
            return success_count
            
        finally:
            if file_processed:
                try:
                    os.remove(path)
                    log.debug(f"Временный файл {path} удален")
                except Exception as e:
                    log.error(f"Ошибка удаления временного файла: {e}")
        
        log.info(f"Фото отправлено {success_count} из {len(recipients)} пользователей")
        return success_count
        
    except Exception as e:
        log.error(f"Критическая ошибка в функции tg_photo: {e}")
        return 0

def save_data(data: dict):
    try:
        os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        error_msg = f"Ошибка сохранения: {e}"
        send_error_to_telegram(error_msg)

def load_data() -> dict:
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        error_msg = f"Ошибка загрузки: {e}"
        send_error_to_telegram(error_msg)
        return {}

def font_path():
    paths = {
        "Windows": ["C:\\Windows\\Fonts\\consola.ttf", "C:\\Windows\\Fonts\\cour.ttf"],
        "Darwin": ["/System/Library/Fonts/Menlo.ttc"],
        "Linux": ["/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"]
    }
    for p in paths.get(platform.system(), []):
        if os.path.exists(p): return p
    raise FileNotFoundError

def get_icon(code: str):
    if not code: return None
    try:
        img = Image.open(BytesIO(requests.get(
            f"https://openweathermap.org/img/wn/{code}@2x.png", timeout=5
        ).content)).convert("RGBA")
        return img.resize((ICON_SIZE, ICON_SIZE), Image.Resampling.LANCZOS)
    except Exception:
        return None

def draw_currency_chart(d, x, y, w, h, hist, font, currency):
    if not hist: return y
    vals = [p[OPERATION_TYPE] for p in hist if OPERATION_TYPE in p]
    if not vals: return y
    
    v_min, v_max = min(vals), max(vals)
    if v_min == v_max:
        rng = v_max * 0.02
        v_min = v_max - rng
    else:
        rng = v_max - v_min
    
    bw = max(2, int(w / (len(vals)*3)))
    gap = bw
    total = len(vals)*(bw+gap) - gap
    
    if total > w:
        vals = vals[-w//(bw+gap):]
        total = len(vals)*(bw+gap)-gap
    
    ox = x + (w-total)//2
    oy = y + h
    
    d.line((ox, y, ox, oy), fill=0)
    d.line((ox, oy, ox+total, oy), fill=0)
    
    for i, v in enumerate(vals):
        px = ox + i*(bw+gap)
        ph = int((v - v_min)/rng*h)
        d.rectangle((px, oy-ph, px+bw, oy), fill=0)
    
    current_val = vals[-1] if vals else 0
    operation_name = "buy" if OPERATION_TYPE == "buy" else "sell"
    d.text((ox+total+5, y), f"■ {currency} ({operation_name}) {current_val}", font=font, fill=0)
    return oy + 20

def draw_pretty_table(draw, x, y, headers, rows, font, max_width=WIDTH - 20):
    table = PrettyTable()
    table.field_names = headers
    for row in rows:
        table.add_row(row)

    table_str = table.get_string()

    for line in table_str.splitlines():
        draw.text((x, y), line, font=font, fill=0)
        bbox = font.getbbox(line)
        line_height = bbox[3] - bbox[1]
        y += line_height + 4

    return y

def make_receipt(weather, curr, hist, namaz, fname="receipt.png"):
    img = Image.new("L", (WIDTH, 1200), 255)
    d = ImageDraw.Draw(img)
    fp = font_path()
    f24 = ImageFont.truetype(fp, 24)
    f18 = ImageFont.truetype(fp, 18)
    f14 = ImageFont.truetype(fp, 14)
    f12 = ImageFont.truetype(fp, 12)

    y = 8
    now = datetime.now(PARIS)
    d.text((10, y), ["Понедельник", "Вторник", "Среда", "Чт.", "Пт.", "Сб.", "Вс."][now.weekday()], font=f24, fill=0)

    y += 30
    d.text((10, y), now.strftime("%d.%m.%Y %H:%M"), font=f24, fill=0)
    y += 25

    # Текущая погода
    temp = round(weather["cur_temp"])
    ico = get_icon(weather["cur_icon"])
    if ico:
        icon_rgba = ico.convert("RGBA")
        mask = icon_rgba.split()[3]
        icon_l = icon_rgba.convert("L")
        img.paste(icon_l, (10, y), mask)
    d.text((50, y + 5), f"{temp}°C🌡️, {weather['cur_descr'][:20]}", font=f24, fill=0)
    y += ICON_SIZE + 15

    # Прогноз
    d.text((10, y), f"Прогноз на 3 дня {weather.get('last_upd_weather', '?')}:", font=f14, fill=0)
    y += 25

    heads = ["#", "День", "Темп", "Описание"]
    rows = []
    for i in range(1, 4):
        rows.append(["", weather[f"day_name_{i}"], f"{weather[f'temp_{i}']}°C", weather[f'descr_{i}'][:25]])

    y_table_start = y
    y = draw_pretty_table(d, 10, y, heads, rows, f18)

    # Иконки для прогноза
    row_height = f18.getbbox(rows[0][1])[3] - f18.getbbox(rows[0][1])[1] + 4
    icon_x = 10 + 4
    for i in range(3):
        icon = get_icon(weather.get(f"icon_{i+1}", ""))
        if icon:
            icon_rgba = icon.convert("RGBA")
            mask = icon_rgba.split()[3]
            icon_l = icon_rgba.convert("L")
            icon_y = y_table_start + i * int(row_height * 1.5) + 45
            img.paste(icon_l, (icon_x, icon_y), mask)

    y += 15

    # Курсы валют
    d.text((10, y), f"Курс валют {curr.get('last_upd_currency', '?')}:", font=f14, fill=0)
    y += 25
    currency_rows = [[c, curr[c]["buy"], curr[c]["sale"]] for c in CURRENCIES]
    y = draw_pretty_table(d, 10, y, ["Валюта", "Покупка", "Продажа"], currency_rows, f18)
    y += 15

    # Время намаза
    d.text((10, y), f"Время намаза {namaz.get('last_upd_namaz', '?')}:", font=f14, fill=0)
    y += 25
    namaz_rows = [[n, namaz.get(n, '?')] for n in ["Фаджр", "Восход", "Зухр", "Аср", "Магриб", "Иша"]]
    y = draw_pretty_table(d, 10, y, ["Намаз", "Время"], namaz_rows, f18)
    y += 15

    # График курса
    operation_name = "покупки" if OPERATION_TYPE == "buy" else "продажи"
    d.text((10, y), f"График {MONITOR_CURRENCY} ({operation_name}):", font=f14, fill=0)
    y += 25
    y = draw_currency_chart(d, 10, y, WIDTH - 20, 80, hist.get(MONITOR_CURRENCY, []), f14, MONITOR_CURRENCY)

    # Футер
    footer_text = "© Generated by rakhmullaev"
    text_width = f18.getbbox(footer_text)[2]
    d.text(((WIDTH - text_width) // 2, y), footer_text, font=f18, fill=0)
    y += 30

    img.crop((0, 0, WIDTH, y)).save(fname)
    return fname

def send_text_report(weather, curr, hist, namaz):
    """Отправка текстового отчета с использованием PrettyTable"""
    try:
        now = datetime.now(PARIS)
        
        # Основная таблица с погодой
        weather_table = PrettyTable()
        weather_table.field_names = ["День", "Темп"]
        weather_table.align = "l"
        weather_table._max_width = {"День": 20, "Темп": 8}
        weather_table.add_row([weather.get('last_upd_weather', '?'), f"{round(weather['cur_temp'])} °C🌡️"])
        
        # Таблица прогноза
        forecast_table = PrettyTable()
        forecast_table.field_names = ["День", "Темп", "Погода"]
        forecast_table.align = "l"
        for i in range(1, 4):
            forecast_table.add_row([
                weather[f"day_name_{i}"],
                weather[f"temp_{i}"],
                weather[f'descr_{i}'].capitalize()
            ])
        
        # Таблица курсов валют
        currency_table = PrettyTable()
        currency_table.field_names = ["Валюта", "Покупка", "Продажа"]
        currency_table.align = "r"
        currency_table.align["Валюта"] = "l"
        for currency in CURRENCIES:
            currency_table.add_row([
                currency,
                curr[currency]["buy"],
                curr[currency]["sale"]
            ])
        
        # Таблица намаза
        namaz_table = PrettyTable()
        namaz_table.field_names = ["Намаз", "Время"]
        namaz_table.align = "l"
        for name in ["Фаджр", "Восход", "Зухр", "Аср", "Магриб", "Иша"]:
            namaz_table.add_row([name, namaz.get(name, '?')])
        
        # Формируем сообщение
        message = (
            f"<b>📅 Отчет на {now.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"<b>🌤 {weather['cur_descr'].capitalize()}:</b>\n<pre>{weather_table}</pre>\n\n"
            f"<b>📊 Прогноз на 3 дня:</b>\n<pre>{forecast_table}</pre>\n\n"
            f"<b>💱 Курсы валют ({curr.get('last_upd_currency', '?')}):</b>\n<pre>{currency_table}</pre>\n\n"
            f"<b>🕌 Время намаза ({namaz.get('last_upd_namaz', '?')}):</b>\n<pre>{namaz_table}</pre>\n\n"
            f"<i>Данные обновляются автоматически</i>"
        )
        
        tg_msg(message, parse_mode=ParseMode.HTML)
        return True
    
    except Exception as e:
        error_msg = f"Ошибка формирования текстового отчета: {e}"
        log.error(error_msg)
        send_error_to_telegram(error_msg)
        return False

def main():
    current_hour = datetime.now().hour
    log.info(f"Запуск в режиме {MODE}")

    try:
        if MODE == "FULL_REPORT":
            log.info("Получение свежих данных...")
            weather = get_weather(use_live=True)
            namaz = get_namaz(use_live=True)
            rates = get_rates(use_live=True)
            
            if not all([weather, namaz, rates]):
                log.error(f"Не удалось получить необходимые данные: погода={bool(weather)}, намаз={bool(namaz)}, курсы={bool(rates)}")
                # Попробуем использовать файловые данные как fallback
                weather = weather or get_weather(use_live=False)
                namaz = namaz or get_namaz(use_live=False)
                rates = rates or get_rates(use_live=False)
                
                if not all([weather, namaz, rates]):
                    log.error("Не удалось получить данные даже из файла")
                    return
            
            log.info("Данные успешно получены, формируем отчет...")
            data = load_data()
            history = data.get("currency_history", {})
            
            send_text_report(weather, rates, history, namaz)
            
        elif MODE == "UPD_NAMAZ":
            namaz = get_namaz(use_live=True)
            if namaz:
                log.info(f"Время намаза успешно обновлено: {namaz.get('last_upd_namaz')}")
            else:
                log.error("Не удалось обновить время намаза")
                
        elif MODE == "UPD_CURRENCY":
            if not should_run(current_hour, update_only=True):
                log.info("Обновление курсов валют временно отключено (работает только с 5 до 15)")
                return
                
            rates = get_rates(force_update=True, use_live=True)
            if rates:
                history = process_currency_history(rates)
                log.info(f"Курсы валют успешно обновлены: {rates.get('last_upd_currency')}")
            else:
                log.error("Не удалось обновить курсы валют")
                
        log.info("Завершено успешно")
    except Exception as e:
        error_msg = f"Критическая ошибка: {e}"
        send_error_to_telegram(error_msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["FULL_REPORT", "UPD_NAMAZ", "UPD_CURRENCY"], 
                       default="FULL_REPORT", help="Режим работы скрипта")
    parser.add_argument("--use-live", action="store_true", help="Использовать живые данные (API)")
    parser.add_argument("--currency", type=str, help="Валюта для мониторинга (USD/EUR)")
    parser.add_argument("--operation", type=str, choices=["buy", "sale"], help="Тип операции (buy/sale)")
    
    args = parser.parse_args()
    
    MODE = args.mode
    USE_LIVE = args.use_live
    
    if args.currency and args.currency.upper() in CURRENCIES:
        MONITOR_CURRENCY = args.currency.upper()
    
    if args.operation:
        OPERATION_TYPE = args.operation
    
    main()