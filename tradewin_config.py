import os
import yaml

import requests
import logging

from datetime import datetime, date
from dataclasses import dataclass

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pathlib import Path


class TelegramLogHandler(logging.Handler):
    """
    Class: Telegram Log Handler
    Description: Class that enables logging messages to Telegram BOT for viewing on phone
    """

    def __init__(self, bot_token, chat_id):
        super().__init__()
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def emit(self, record):
        try:
            session = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("https://", adapter)

            message = self.format(record)
            response = session.post(self.url, data={"chat_id": self.chat_id, "text": message}, timeout=5)
            response.raise_for_status()
            if not response.ok:
                print(f"Telegram API failure: {response.status_code} - {response.text}")
        except Exception as e:
            print("🔥 EXCEPTION CAUGHT IN EMIT")
            print(f"Telegram handler exception: {e}")


class TradewinLogger:
    """
    Class: Tradewin Logger
    Description: Class that enables logging messages to multiple devices - e.g. screen, file, telegram
    """

    def __init__(self, log_dir="logs"):
        self._loggers = {}
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def get_logger(self, name="TradeWin", enable_telegram=False, bot_token=None, chat_id=None):
        if name in self._loggers:
            return self._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(os.path.join(self.log_dir, f"{name}.log"))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        if enable_telegram and bot_token and chat_id:
            telegram_handler = TelegramLogHandler(bot_token, chat_id)
            telegram_handler.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(telegram_handler)

        self._loggers[name] = logger
        return logger


class TradewinConfig:
    """
    Class: Tradewin Config
    Description: Class that enables loading all configuration values from yaml file and make it available to the
    application
    """

    def __init__(self, config_path):
        self.TRAIL_AMOUNT = None
        self.PAPER_TRADING = None
        self.TRADE_QTY = None
        self.INTERVAL = None
        self.SYMBOL = None
        self.API_SECRET = None
        self.API_KEY = None
        self.WEEKEND_TESTING = None
        self.SLEEP_INTERVAL = None
        self.DB_USER = None
        self.DB_PASS = None
        self.DB_NAME = None
        self.DB_HOST = None
        self.DB_PORT = None
        self.vwap_dev = None
        self.vwap_sl_mult = None
        self.vwap_target_mult = None
        self.vwap_rr_threshold = None
        self.entry_buffer = None
        self.orb_sl_factor = None
        self.orb_target_factor = None
        self.COOLDOWN_MINUTES = None
        self.MAX_DAILY_LOSS = None
        self.strategy_mode = None
        self.ANNUAL_HOLIDAYS = None

        # Try resolving relative to caller’s path first
        config_file = Path(config_path)

        # If not found, try relative to this file (i.e., algo/tradewin_config.yaml)
        if not config_file.is_file():
            config_file = Path(__file__).resolve().parent / config_path

        # If still not found, try relative to project root (2 levels up from algo/)
        if not config_file.is_file():
            project_root = Path(__file__).resolve().parents[1]
            config_file = project_root / config_path

        if not config_file.is_file():
            raise FileNotFoundError(f"❌ Config file not found: {config_path}")

        if Path(config_file).is_file():
            with open(config_file, 'r') as f:
                self._config = yaml.safe_load(f) or {}

                self.orb_sl_factor = self._config.get("orb", {}).get("sl_factor", 1.5)
                self.orb_target_factor = self._config.get("orb", {}).get("target_factor", 4.0)

                self.vwap_sl_mult = self._config.get("vwap_rev", {}).get("sl_mult", 0.8)
                self.vwap_target_mult = self._config.get("vwap_rev", {}).get("target_mult", 4.0)
                self.vwap_rr_threshold = self._config.get("vwap_rev", {}).get("rr_threshold", 1.2)

                self.strategy_mode = self._config.get("strategy_mode", "adaptive").upper()

                telegram_cfg = self._config.get("telegram", {})
                self.TELEGRAM_ENABLED = telegram_cfg.get("enabled", False)
                self.TELEGRAM_BOT_TOKEN = telegram_cfg.get("bot_token", "")
                self.TELEGRAM_CHAT_ID = telegram_cfg.get("chat_id", "")

                self.ANNUAL_HOLIDAYS = {
                    d if isinstance(d, date) else datetime.strptime(d, "%Y-%m-%d").date()
                    for d in self._config.get("annual_holidays", [])
                }

        else:
            self._config = {}  # ← no exception raised!

        for key, value in self._config.items():
            setattr(self, key, value)

    def get(self, key, default=None):
        return self._config.get(key, default)

    def all(self):
        return self._config

    def get_db_config(self):
        return {
            "user": self.DB_USER,
            "password": self.DB_PASS,
            "database": self.DB_NAME,
            "host": self.DB_HOST,
            "port": self.DB_PORT,
        }


@dataclass
class TradeDecision:
    """
    Class: TradeDecision
    Description: Class that holds the decision of a Trade based on conditions applied
    """
    date: datetime | None
    signal: str | None
    entry: float | None
    sl: float | None
    target: float | None
    valid: bool
    strategy: str | None
    reason: str = ""  # optional debug information


@dataclass
class TradeState:
    def __init__(self):
        self.stop_loss = None
        self.trade_direction = None
        self.last_sl_update_time = None
        self.last_exit_time = None
        self.last_exit_price = None
        self.trade_id = None
        self.strategy = None
        self.open_trade = None
        self.entry_time = None
        self.target_price = None
        self.entry_price = None
        self.position = None
        self.date = None
        self.qty = 0
        self.trade_type = None
        self.checked_post_entry = False

    def reset(self):
        self.trade_direction = None
        self.position = None
        self.stop_loss = 0.0
        self.entry_price = 0.0
        self.target_price = 0.0
        self.entry_time = None
        self.open_trade = False
        self.strategy = None
        self.trade_id = None
        self.last_exit_price = None
        self.last_exit_time = None
        self.last_sl_update_time = None
        self.date = None
        self.checked_post_entry = False
        self.qty = 0
        self.trade_type = None
