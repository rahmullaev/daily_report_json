"""История температуры и курсов: FIFO, максимум N, индексы с 1."""
from typing import List
from settings import CURRENCIES
from logger import log

def reindex_history(history_list: List[dict]) -> List[dict]:
    """Пересчет индексов в истории (1 - самая старая, N - самая новая)"""
    for idx, entry in enumerate(history_list, 1):
        entry["index"] = idx
    return history_list


def add_temp_history(data: dict, weather: dict, max_entries: int) -> None:
    if "temp_history" not in data:
        data["temp_history"] = []
    if len(data["temp_history"]) >= max_entries:
        removed = data["temp_history"].pop(0)
        log.info(f"🗑️ Удалена запись температуры от {removed.get('timestamp', '?')}")
    data["temp_history"].append({
        "temp": weather["cur_temp"],
        "icon": weather["cur_icon"],
        "descr": weather["cur_descr"],
        "timestamp": weather["last_upd"],
    })
    log.info(f"➕ Добавлена температура: {weather['cur_temp']}°C (всего: {len(data['temp_history'])})")


def add_currency_history(data: dict, rates: dict, max_entries: int, *, log_removal: bool = True) -> None:
    if "currency_history" not in data:
        data["currency_history"] = []
    if len(data["currency_history"]) >= max_entries:
        removed = data["currency_history"].pop(0)
        if log_removal:
            log.info(f"🗑️ Удалена запись курсов от {removed.get('timestamp', '?')}")
    history_entry = {"timestamp": rates["last_upd"], "rates": {}}
    for cur in CURRENCIES:
        if cur in rates:
            history_entry["rates"][cur] = {
                "buy": rates[cur]["buy"],
                "sale": rates[cur]["sale"],
            }
    data["currency_history"].append(history_entry)
    if log_removal:
        log.info(f"➕ Добавлены курсы (всего: {len(data['currency_history'])})")


def reindex_all(data: dict) -> None:
    if "temp_history" in data:
        data["temp_history"] = reindex_history(data["temp_history"])
    if "currency_history" in data:
        data["currency_history"] = reindex_history(data["currency_history"])
