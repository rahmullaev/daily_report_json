"""Telegram-оповещения об изменении курсов."""
from datetime import datetime
from prettytable import PrettyTable
from settings import ENABLE_ALERTS, USER_IDS, TELEGRAM_BOT_TOKEN, PARIS
from logger import log

try:
    from telegram import Bot, ParseMode
    bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
except Exception as exc:
    log.warning(f"Ошибка инициализации бота: {exc}")
    bot = None

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
