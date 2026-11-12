#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Точка входа: прежние режимы и формат JSON сохранены."""
import argparse
import traceback
from settings import OW_API_KEY, TELEGRAM_BOT_TOKEN, USER_IDS, CITY_ID
from logger import log
from storage import load_data, save_data
from weather import get_weather
from namaz import get_namaz
from currency import get_rates, get_previous_currency, check_currency_changes
from history import add_temp_history, add_currency_history, reindex_history, reindex_all
from alerts import send_currency_alerts

USE_LIVE = True
MODE = "FULL_REPORT"
HISTORY_MAX_ENTRIES = 10


def show_settings():
    print("=" * 60)
    print("🔍 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
    print(f"OW_API_KEY: {'✅ УСТАНОВЛЕН' if OW_API_KEY else '❌ НЕ УСТАНОВЛЕН'} (длина: {len(OW_API_KEY)})")
    print(f"TELEGRAM_BOT_TOKEN: {'✅ УСТАНОВЛЕН' if TELEGRAM_BOT_TOKEN else '❌ НЕ УСТАНОВЛЕН'}")
    print(f"TELEGRAM_USER_IDS: {USER_IDS if USER_IDS else '❌ НЕ УСТАНОВЛЕН'}")
    print(f"CITY_ID: {CITY_ID}")
    print(f"📊 Хранение истории: последние {HISTORY_MAX_ENTRIES} записей")
    print("=" * 60)


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

                add_temp_history(data, weather, HISTORY_MAX_ENTRIES)

            # Обновляем намазы
            if namaz:
                data["namaz"] = namaz

            # Обновляем курсы и историю
            if rates:
                data["currency_rates"] = rates

                add_currency_history(data, rates, HISTORY_MAX_ENTRIES)

            # Пересчитываем индексы
            reindex_all(data)

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

                add_currency_history(data, rates, HISTORY_MAX_ENTRIES, log_removal=False)
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

    show_settings()
    main()
