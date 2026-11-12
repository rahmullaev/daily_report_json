"""Курсы Kaspi и вычисление изменений."""
from datetime import datetime
from typing import Any, Dict
import requests
from settings import USE_LIVE, KASPI_URL, CURRENCIES, TIMEOUT_API, BASE_HEADERS, PARIS, GITHUB_JSON_URL
from storage import load_data
from logger import log

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
