"""Настройки проекта (секреты только из переменных окружения)."""
import os
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Local .env is optional: GitHub Actions still uses environment/secrets.
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env", override=False)

USE_LIVE = True
MODE = "FULL_REPORT"
PARIS = ZoneInfo("Europe/Paris")
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
CURRENT_URL = "https://api.openweathermap.org/data/2.5/weather"
OW_API_KEY = os.getenv("OW_API_KEY", "")
CITY_ID = os.getenv("CITY_ID", "2994160")
USER_IDS = [int(uid.strip()) for uid in os.getenv("TELEGRAM_USER_IDS", "").split(",") if uid.strip()]
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
KASPI_URL = "https://guide.kaspi.kz/client/api/v2/intgr/currency/rate/aggregate"
CURRENCIES = ("USD", "EUR")
TIMEOUT_API = 10
HISTORY_MAX_ENTRIES = 10
DATA_FILE = SCRIPT_DIR / "data" / "weather_data.json"
LOG_FILE = SCRIPT_DIR / "Log.log"
BASE_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "gIsAuthorized": "false", "gIsInnerRequest": "false",
    "gIsMobileApp": "false", "gLanguage": "ru", "gRole": "1", "gSystem": "kkz",
}
ENABLE_ALERTS = True
GITHUB_JSON_URL = "https://rahmullaev.github.io/daily_report_json/weather_data.json"
