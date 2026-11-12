"""Текущая погода и прогноз OpenWeather."""
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from collections import defaultdict
import requests
from settings import USE_LIVE, PARIS, FORECAST_URL, CURRENT_URL, OW_API_KEY, CITY_ID
from storage import load_data
from logger import log

def get_weather(use_live: bool = USE_LIVE) -> Optional[Dict[str, Any]]:
    """Получение текущей погоды и прогноза на 3 дня"""
    if not use_live:
        log.info("📂 Использую сохраненные данные погоды")
        data = load_data().get("weather", {})
        return data if data else None

    try:
        if not OW_API_KEY:
            log.error("❌ OW_API_KEY не установлен!")
            return None

        params = {
            "id": CITY_ID,
            "units": "metric",
            "lang": "ru",
            "APPID": OW_API_KEY
        }

        log.info(f"🌤️ Запрос погоды для города {CITY_ID}")

        # ─────────────────────────────────────────────
        # ТЕКУЩАЯ ПОГОДА
        # ─────────────────────────────────────────────
        cur = requests.get(
            CURRENT_URL,
            params=params,
            timeout=10
        )

        if cur.status_code != 200:
            log.error(
                f"Ошибка текущей погоды: "
                f"{cur.status_code} - {cur.text}"
            )
            return None

        cur_data = cur.json()

        # ─────────────────────────────────────────────
        # ПРОГНОЗ
        # ─────────────────────────────────────────────
        fc = requests.get(
            FORECAST_URL,
            params=params,
            timeout=10
        )

        if fc.status_code != 200:
            log.error(
                f"Ошибка прогноза: "
                f"{fc.status_code} - {fc.text}"
            )
            return None

        fc_data = fc.json()

        # Сегодня по времени Парижа
        today = datetime.now(PARIS).date()

        # ─────────────────────────────────────────────
        # РАЗБИВАЕМ ПРОГНОЗ ПО ДНЯМ
        # ВАЖНО: используем локальное время Europe/Paris,
        # а не UTC, чтобы около полуночи дни не съезжали.
        # ─────────────────────────────────────────────
        buckets = defaultdict(list)

        for itm in fc_data.get("list", []):
            try:
                local_dt = datetime.fromtimestamp(
                    itm["dt"],
                    PARIS
                )

                local_date = local_dt.date()

                buckets[local_date].append(itm)

            except Exception as e:
                log.warning(
                    f"Не удалось обработать элемент прогноза: {e}"
                )

        forecast = {}

        day_names = [
            "Пн.",
            "Вт.",
            "Ср.",
            "Чт.",
            "Пт.",
            "Сб.",
            "Вс."
        ]

        # ─────────────────────────────────────────────
        # ПРОГНОЗ НА СЛЕДУЮЩИЕ 3 ДНЯ
        # Берём запись, ближайшую к 12:00 местного времени
        # ─────────────────────────────────────────────
        for off in range(1, 4):

            tgt = today + timedelta(days=off)

            bucket = buckets.get(tgt, [])

            if not bucket:
                log.warning(
                    f"⚠️ Нет прогноза на {tgt.strftime('%d.%m.%Y')}"
                )
                continue

            # Выбираем прогноз максимально близкий к 12:00
            mid = min(
                bucket,
                key=lambda x: abs(
                    datetime.fromtimestamp(
                        x["dt"],
                        PARIS
                    ).hour - 12
                )
            )

            loc = datetime.fromtimestamp(
                mid["dt"],
                PARIS
            )

            main_data = mid.get("main", {})
            weather_list = mid.get("weather", [])

            weather_item = (
                weather_list[0]
                if weather_list
                else {}
            )

            # ───────────────────────────────────────
            # Сохраняем старые поля БЕЗ ИЗМЕНЕНИЙ
            # + добавляем humidity и pressure
            # ───────────────────────────────────────
            forecast.update({

                f"day_name_{off}":
                    day_names[loc.weekday()],

                f"temp_{off}":
                    f"{main_data.get('temp', 0):+.0f}",

                f"icon_{off}":
                    weather_item.get("icon", ""),

                f"descr_{off}":
                    weather_item.get(
                        "description",
                        ""
                    ),

                # НОВОЕ:
                f"humidity_{off}":
                    int(main_data.get("humidity", 0)),

                # НОВОЕ:
                # OpenWeather возвращает давление в hPa
                f"pressure_{off}":
                    int(main_data.get("pressure", 0)),
            })

        # ─────────────────────────────────────────────
        # ТЕКУЩИЕ ДАННЫЕ
        # ─────────────────────────────────────────────
        current_main = cur_data.get("main", {})
        current_weather_list = cur_data.get(
            "weather",
            []
        )

        current_weather = (
            current_weather_list[0]
            if current_weather_list
            else {}
        )

        weather_data = {

            # Старые поля
            "cur_temp":
                current_main.get("temp", 0),

            "cur_icon":
                current_weather.get("icon", ""),

            "cur_descr":
                current_weather.get(
                    "description",
                    ""
                ),

            # ───────────────────────────────────────
            # НОВЫЕ ПОЛЯ НА СЕГОДНЯ
            # ───────────────────────────────────────

            # Влажность %
            "cur_humidity":
                int(current_main.get("humidity", 0)),

            # Давление hPa
            "cur_pressure":
                int(current_main.get("pressure", 0)),

            # Время обновления
            "last_upd":
                datetime.now(PARIS).strftime(
                    "%d.%m.%Y %H:%M:%S"
                ),

            # Прогноз 1-3 дня
            **forecast
        }

        log.info(
            "✅ Погода: "
            f"{weather_data['cur_temp']}°C, "
            f"{weather_data['cur_descr']}, "
            f"💧 {weather_data['cur_humidity']}%, "
            f"🌡️ {weather_data['cur_pressure']} hPa"
        )

        # Дополнительный лог прогноза
        for off in range(1, 4):

            if f"temp_{off}" in weather_data:

                log.info(
                    f"📅 День {off}: "
                    f"{weather_data.get(f'day_name_{off}', '')} "
                    f"{weather_data.get(f'temp_{off}', '')}°C, "
                    f"💧 {weather_data.get(f'humidity_{off}', 0)}%, "
                    f"🌡️ {weather_data.get(f'pressure_{off}', 0)} hPa"
                )

        return weather_data

    except requests.exceptions.Timeout:
        log.error("❌ Таймаут при запросе погоды")
        return None

    except requests.exceptions.RequestException as e:
        log.error(f"❌ Ошибка HTTP при получении погоды: {e}")
        return None

    except Exception as e:
        log.error(f"❌ Ошибка получения погоды: {e}")
        log.error(traceback.format_exc())
        return None
