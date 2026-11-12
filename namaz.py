"""Расписание намазов Aladhan."""
from datetime import datetime
from typing import Dict
import requests
from settings import PARIS
from storage import load_data
from logger import log

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
