#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, platform, logging, requests, json, argparse, time
from io import BytesIO
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont
from telegram import Bot, InputMediaPhoto, ParseMode
from telegram.error import TelegramError
from prettytable import PrettyTable
import re

# Binance client
from binance.client import Client

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
USER_IDS = [int(uid) for uid in os.getenv("TELEGRAM_USER_IDS", "883019358").split(",") if uid]
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
        log.error(f"Ошибка получения погоды: {e}")
        return None



def get_namaz(use_live: bool = True) -> Dict[str, str]:
    """Получение времени намазов через Aladhan API (Метц, Франция)"""
    if not use_live:
        data = load_data().get("namaz", {})
        if data:
            last_upd = data.get("last_upd_namaz", "Из файла (нет даты)")
            return data | {"last_upd_namaz": last_upd}
        return {}

    try:
        params = {
            "latitude": 49.1193,
            "longitude": 6.1757,
            "method": 3  # ISNA (приближено к MuslimPro)
        }
        r = requests.get("https://api.aladhan.com/v1/timings", params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        timings = data.get("data", {}).get("timings", {})

        namaz_mapping = {
            "Fajr": "Фаджр",
            "Sunrise": "Восход",
            "Dhuhr": "Зухр",
            "Asr": "Аср",
            "Maghrib": "Магриб",
            "Isha": "Иша"
        }

        namaz_times = {rus: timings.get(eng, "") for eng, rus in namaz_mapping.items()}
        result = {
            **namaz_times,
            "last_upd_namaz": datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")
        }

        store = load_data()
        store["namaz"] = result
        save_data(store)

        log.info("Время намазов успешно обновлено с Aladhan API.")
        return result

    except Exception as e:
        log.error(f"Ошибка получения намазов: {e}")
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
        log.error(f"Ошибка получения курсов: {e}")
        return {}



def tg_photo(path: str, caption: str = None, additional_ids: list = None):
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
        log.error(f"Ошибка сохранения: {e}")

def load_data() -> dict:
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        log.error(f"Ошибка загрузки: {e}")
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

def make_receipt(weather: dict, curr: dict, hist: dict, namaz: dict, fname="receipt.png") -> str:
    """
    Генерация изображения-отчета с погодой, курсами, намазами и графиком.
    Иконки будут корректно вставлены с прозрачностью.
    """
    img = Image.new("RGBA", (WIDTH, 1600), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)
    fp = font_path()
    f24 = ImageFont.truetype(fp, 24)
    f18 = ImageFont.truetype(fp, 18)
    f14 = ImageFont.truetype(fp, 14)

    y = 10
    now = datetime.now(PARIS)
    draw.text((10, y), f"{DAY_NAME_RU[now.weekday()]}, {now.strftime('%d.%m.%Y %H:%M')}", font=f24, fill=0)
    y += 40

    # --- Текущая погода ---
    cur_temp = round(weather.get("cur_temp", 0))
    cur_descr = weather.get("cur_descr", "?")
    ico = get_weather_icon(weather.get("cur_icon"))
    if ico:
        img.paste(ico, (10, y), ico)
    draw.text((50, y + 5), f"{cur_temp}°C, {cur_descr}", font=f24, fill=0)
    y += ICON_SIZE + 20

    # --- Прогноз на 3 дня ---
    heads = ["#", "День", "Темп", "Погода"]
    rows = []
    icons = []
    for i in range(1, 4):
        rows.append([
            i,
            weather.get(f"day_name_{i}", "?"),
            f"{weather.get(f'temp_{i}', '?')}°C",
            weather.get(f"descr_{i}", "?")[:20]
        ])
        icons.append(get_weather_icon(weather.get(f"icon_{i}", "")))

    draw.text((10, y), f"Прогноз на 3 дня (обновлено {weather.get('last_upd_weather', '?')}):", font=f14, fill=0)
    y += 25
    y = draw_table_with_icons(img, draw, 10, y, heads, rows, f18, icons=icons)

    # --- Курсы валют ---
    draw.text((10, y), f"Курсы валют (обновлено {curr.get('last_upd_currency', '?')}):", font=f14, fill=0)
    y += 25
    currency_rows = [[c, curr.get(c, {}).get("buy", "?"), curr.get(c, {}).get("sale", "?")] for c in CURRENCIES]
    y = draw_table_with_icons(img, draw, 10, y, ["Валюта", "Покупка", "Продажа"], currency_rows, f18)

    # --- Время намаза ---
    draw.text((10, y), f"Время намаза (обновлено {namaz.get('last_upd_namaz', '?')}):", font=f14, fill=0)
    y += 25
    namaz_rows = [[n, namaz.get(n, "?")] for n in ["Фаджр", "Восход", "Зухр", "Аср", "Магриб", "Иша"]]
    y = draw_table_with_icons(img, draw, 10, y, ["Намаз", "Время"], namaz_rows, f18)

    # --- График курса ---
    operation_name = "покупки" if OPERATION_TYPE == "buy" else "продажи"
    draw.text((10, y), f"График {MONITOR_CURRENCY} ({operation_name}):", font=f14, fill=0)
    y += 25
    draw_currency_chart(draw, 10, y, WIDTH - 20, 80, hist.get(MONITOR_CURRENCY, []), f14, MONITOR_CURRENCY)
    y += 100

    # --- Футер ---
    footer_text = "© Generated by rakhmullaev"
    text_width = f18.getbbox(footer_text)[2]
    draw.text(((WIDTH - text_width)//2, y), footer_text, font=f18, fill=0)

    img.crop((0, 0, WIDTH, y + 30)).save(fname)
    log.info(f"Отчет сохранен: {fname}")
    return fname



def draw_table_with_icons(img: Image.Image, draw: ImageDraw.Draw, x: int, y: int,
                          headers: list, rows: list, font: ImageFont.ImageFont,
                          icons: list[Image.Image] | None = None,
                          line_height: int = None, padding: int = 4) -> int:
    """
    Рисует таблицу ASCII с возможностью вставки иконок в первый столбец.
    img — основной объект Image
    draw — ImageDraw.Draw
    icons — список объектов PIL.Image или None.
    """
    if line_height is None:
        line_height = max(ICON_SIZE, font.getbbox("A")[3] - font.getbbox("A")[1]) + 4

    columns = list(zip(*([headers] + rows)))
    col_widths = [max(len(str(item)) for item in col) + padding*2 for col in columns]

    def make_line(left, mid, right, fill):
        line = left
        for i, w in enumerate(col_widths):
            line += fill * w
            if i < len(col_widths) -1:
                line += mid
        line += right
        return line

    # Верхняя граница
    draw.text((x, y), make_line("+", "+", "+", "-"), font=font, fill=0)
    y += line_height

    # Заголовки
    header_line = "|" + "|".join(f" {h.center(col_widths[i]-2)} " for i, h in enumerate(headers)) + "|"
    draw.text((x, y), header_line, font=font, fill=0)
    y += line_height

    # Средняя линия
    draw.text((x, y), make_line("+", "+", "+", "-"), font=font, fill=0)
    y += line_height

    # Строки
    for i, row in enumerate(rows):
        row_line = "|" + "|".join(f" {str(cell).ljust(col_widths[j]-2)} " for j, cell in enumerate(row)) + "|"
        draw.text((x, y), row_line, font=font, fill=0)

        # Вставка иконки в первый столбец
        if icons and i < len(icons) and icons[i]:
            icon_img = icons[i]
            cell_x = x + 2
            cell_y = y + (line_height - icon_img.size[1]) // 2
            img.paste(icon_img, (cell_x, cell_y), icon_img)

        y += line_height

    # Нижняя граница
    draw.text((x, y), make_line("+", "+", "+", "-"), font=font, fill=0)
    y += line_height
    return y



def tg_msg(txt: str, parse_mode: str = None, additional_ids: list = None):
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


def get_weather_icon(icon_code: str) -> tuple[Image.Image | None, str]:
    """
    Скачивает иконку погоды по коду OpenWeather и возвращает:
    (Image с прозрачностью RGBA или None, эмодзи строки)
    Выводит отладочные сообщения через print.
    """
    
    # Словарь соответствия иконок и эмодзи
    OW_ICON_EMOJI = {
        "01d": "☀️", "01n": "🌙",
        "02d": "🌤️", "02n": "☁️",
        "03d": "☁️", "03n": "☁️",
        "04d": "☁️", "04n": "☁️",
        "09d": "🌧️", "09n": "🌧️",
        "10d": "🌦️", "10n": "🌦️",
        "11d": "⛈️", "11n": "⛈️",
        "13d": "❄️", "13n": "❄️",
        "50d": "🌫️", "50n": "🌫️",
    }

    emoji = OW_ICON_EMOJI.get(icon_code, "")
    
    if not icon_code:
        print("[DEBUG] Нет кода иконки, возвращаем None и пустое эмодзи")
        return None, emoji

    url = f"https://openweathermap.org/img/wn/{icon_code}@2x.png"
    print(f"[DEBUG] Загружаем иконку с URL: {url}")

    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        print(f"[DEBUG] Иконка загружена, размер данных: {len(resp.content)} байт")

        img = Image.open(BytesIO(resp.content)).convert("RGBA")
        print(f"[DEBUG] Иконка конвертирована в RGBA, размер: {img.size}")

        img = img.resize((ICON_SIZE, ICON_SIZE), Image.Resampling.LANCZOS)
        print(f"[DEBUG] Иконка изменена до размера: {img.size}")

        return img, emoji

    except Exception as e:
        print(f"[DEBUG] Не удалось загрузить иконку {icon_code}: {e}")
        log.error(f"Не удалось загрузить иконку {icon_code}: {e}")
        return None, emoji

    

def send_text_report(weather: dict, curr: dict, namaz: dict) -> bool:
    """
    Формирует текстовый отчет с эмодзи и отправляет его в Telegram через PrettyTable.
    Эмодзи соответствуют кодам иконок OpenWeather.
    Возвращает True при успехе, False при ошибке.
    """
    try:
        now = datetime.now(PARIS)

        

        # --- Таблица текущей погоды ---
        weather_table = PrettyTable()
        weather_table.field_names = ["Время", "Темп", "Погода"]
        weather_table.align = "l"

        cur_icon_code = weather.get('cur_icon', '')
        cur_icon_img, cur_icon_emoji = get_weather_icon(cur_icon_code)

        log.info(f"[DEBUG] Текущая иконка: {cur_icon_code}, эмодзи: {cur_icon_emoji}")

        cur_descr = weather.get('cur_descr', '')[:15].capitalize()
        cur_temp = f"{round(weather.get('cur_temp', 0))}°C"
        weather_table.add_row([weather.get('last_upd_weather', '?'), cur_temp, cur_descr])

        # --- Таблица прогноза на 3 дня ---
        forecast_table = PrettyTable()
        forecast_table.field_names = ["День", "Темп", "Погода"]
        forecast_table.align = "l"

        for i in range(1, 4):
            day = weather.get(f"day_name_{i}", "?")
            temp = weather.get(f"temp_{i}", "?")
            icon_code = weather.get(f"icon_{i}", "")
            
            # Получаем иконку и эмодзи через функцию
            _, icon_emoji = get_weather_icon(icon_code)
            log.info(f"[DEBUG] Прогноз день {i}, иконка: {icon_code}, эмодзи: {icon_emoji}")
            
            descr = weather.get(f"descr_{i}", "")[:15].capitalize()
            forecast_table.add_row([day, temp, f"{icon_emoji} {descr}"])


        # --- Таблица курсов валют ---
        currency_table = PrettyTable()
        currency_table.field_names = ["Валюта", "Курс Покупки", "Курс Продажи"]
        currency_table.align = "l"
        for c in CURRENCIES:
            currency_table.add_row([c, curr.get(c, {}).get("buy", "?"), curr.get(c, {}).get("sale", "?")])

        # --- Таблица времени намаза ---
        namaz_table = PrettyTable()
        namaz_table.field_names = ["Намаз", "Время"]
        namaz_table.align = "l"
        for name in ["Фаджр", "Восход", "Зухр", "Аср", "Магриб", "Иша"]:
            namaz_table.add_row([name, namaz.get(name, "?")])

        # --- Формируем сообщение ---
        message = (
            f"<b>📅 Отчет на {now.strftime('%d.%m.%Y %H:%M')}</b>\n\n"
            f"<b>{cur_icon_emoji}Текущая погода ({weather.get('last_upd_weather', '?')}):</b>\n<pre>{weather_table}</pre>\n\n"
            f"<b>📊 Прогноз на 3 дня:</b>\n<pre>{forecast_table}</pre>\n\n"
            f"<b>💱 Курсы валют ({curr.get('last_upd_currency', '?')}):</b>\n<pre>{currency_table}</pre>\n\n"
            f"<b>🕌 Время намаза ({namaz.get('last_upd_namaz', '?')}):</b>\n<pre>{namaz_table}</pre>\n\n"
            f"<i>Данные обновляются автоматически</i>"
        )

        # --- Отправка в Telegram ---
        tg_msg(message, parse_mode=ParseMode.HTML)
        return True

    except Exception as e:
        log.error(f"Ошибка формирования или отправки отчета: {e}")
        return False





def generate_json_report(weather, curr, hist, namaz):
    """Генерация JSON отчета"""
    report = {
        "weather": weather,
        "currency_rates": curr,
        #"currency_history": hist,
        "namaz_times": namaz,
        "generated_at": datetime.now(PARIS).strftime("%d.%m.%Y %H:%M:%S")
    }
    
    json_file = os.path.join(SCRIPT_DIR, "data", "report.json")
    os.makedirs(os.path.dirname(json_file), exist_ok=True)
    
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    return json_file

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
                weather = weather or get_weather(use_live=False)
                namaz = namaz or get_namaz(use_live=False)
                rates = rates or get_rates(use_live=False)
                
                if not all([weather, namaz, rates]):
                    log.error("Не удалось получить данные даже из файла")
                    return
            
            log.info("Данные успешно получены, формируем отчет...")
            data = load_data()
            history = data.get("currency_history", {})
            
            # Отправляем текстовый отчет
            #send_text_report(weather, rates, namaz)
            
            # Генерируем JSON отчет
            json_report = generate_json_report(weather, rates, history, namaz)
            log.info(f"JSON отчет сохранен: {json_report}")
            
            # Создаем и отправляем изображение
            #receipt_file = make_receipt(weather, rates, history, namaz)
            #tg_photo(receipt_file, caption="📊 Ежедневный отчет")
            
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
                
        log.info("Завершено успешно")
    except Exception as e:
        log.error(f"Критическая ошибка: {e}")

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