"""Чтение и публикация полного и двух компактных JSON без изменения схемы."""
import os
import json
from settings import DATA_FILE
from logger import log

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
        # Publish two smaller JSON files alongside the unchanged full report.
        # Compact separators avoid unnecessary network transfer to ESP8266.
        current_data = {
            "weather": data.get("weather", {}),
            "namaz": data.get("namaz", {}),
            "currency_rates": data.get("currency_rates", {}),
        }
        history_data = {
            "temp_history": data.get("temp_history", []),
            "currency_history": data.get("currency_history", []),
        }
        for filename, payload in (
            ("weather_current.json", current_data),
            ("weather_history.json", history_data),
        ):
            target = os.path.join(os.path.dirname(DATA_FILE), filename)
            with open(target, "w", encoding="utf-8") as output:
                json.dump(payload, output, ensure_ascii=False, separators=(",", ":"))
        log.info(f"💾 Данные сохранены: {temp_count} записей температуры, {curr_count} записей курсов")
    except Exception as e:
        log.error(f"Ошибка сохранения: {e}")
