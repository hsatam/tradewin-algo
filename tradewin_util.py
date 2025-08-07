import os
import sys
import uuid
import psycopg2

from kiteconnect import KiteConnect
from psycopg2.extras import Json, RealDictCursor
from algo.tradewin_config import TradewinLogger, TradewinConfig
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

config = TradewinConfig("tradewin_config.yaml")

logger = TradewinLogger().get_logger(
    enable_telegram=config.TELEGRAM_ENABLED,
    bot_token=config.TELEGRAM_BOT_TOKEN,
    chat_id=config.TELEGRAM_CHAT_ID
)


class TradewinDBConfig:
    """
    Class: Tradewin Database Configuration
    Description: Handles all database activities including connection, insertion, updates and retrievals
    """
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self._truncate_table()

    def _truncate_table(self):
        try:
            self.cur.execute("TRUNCATE TABLE trades;")
            self.conn.commit()
            logger.info("✅ Trades table truncated at startup.")
        except Exception as e:
            logger.error(f"❌ Failed to truncate trades: {e}")
            self.conn.rollback()

    def record_trade(self, trade_data):
        query = """
            INSERT INTO trades (trade_id, time, type, price, sl, exited, pnl, strategy,
                                meta_data, symbol, exitprice, exittime, lots)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cur.execute(query, (
                trade_data["trade_id"], trade_data["time"], trade_data["type"], trade_data["price"],
                trade_data["sl"], trade_data["exited"], trade_data["pnl"], trade_data["strategy"],
                Json(trade_data["meta_data"]), trade_data["symbol"], trade_data["exitprice"],
                trade_data["exittime"], trade_data["lots"]
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to record trade: {e}")
            self.conn.rollback()

    def fetch_summary(self):
        self.cur.execute("""
            SELECT COUNT(*) as total_trades,
                   SUM(pnl) as total_pnl,
                   AVG(pnl) FILTER (WHERE pnl > 0) as avg_win,
                   AVG(pnl) FILTER (WHERE pnl < 0) as avg_loss,
                   SUM(pnl) FILTER (WHERE pnl > 0) as wins_pnl,
                   SUM(pnl) FILTER (WHERE pnl < 0) as losses_pnl,
                   COUNT(*) FILTER (WHERE pnl > 0)::numeric / NULLIF(COUNT(*), 0) * 100.0 as win_pct
            FROM trades
        """)
        return self.cur.fetchone() or {}

    def fetch_pnl_today(self):
        self.cur.execute("SELECT SUM(pnl) as pnl_today FROM trades WHERE time::date = CURRENT_DATE;")
        result = self.cur.fetchone()
        if not result or result["pnl_today"] is None:
            return 0.0
        return result["pnl_today"]

    def populate_logs(self):
        try:
            self.cur.execute("""
                INSERT INTO trade_log (tr_date, action, entry_price, exit_price, pnl, lots)
                SELECT exittime::date, type, price, exitprice, pnl, lots
                FROM trades WHERE exited = TRUE AND time::date = CURRENT_DATE
            """)
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to populate logs: {e}")
            self.conn.rollback()

    def close(self):
        self.cur.close()
        self.conn.close()
        logger.info("🔚 Database connection closed.")


class TradewinKite:
    """
    Class: Kite Connectivity
    Description: Class used to connect to Kite API and make all necessary calls like:
        1. Get margin details
        2. Place order
        3. Set Stop Loss
    """
    def __init__(self, api_key, api_secret, token_file='tradewin_token'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token_file = token_file
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_session_expiry_hook(lambda: logger.info("🔒 Kite Session expired."))

    def _get_saved_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                return f.read().strip()
        return None

    def _save_token(self, token):
        with open(self.token_file, 'w') as f:
            f.write(token)

    def authenticate(self):
        access_token = self._get_saved_token()
        if access_token:
            self.kite.set_access_token(access_token)
            try:
                self.kite.profile()  # ping to verify
                logger.info("✅ Reused existing token.")
                return self.kite
            except Exception as e:
                logger.warning(f"⚠️ Access token invalid or expired: {e}")

        # Step 1: Ask for request token
        login_url = self.kite.login_url()
        logger.info("🔐 Please visit the following URL to login and obtain your request token:")
        logger.info(login_url)
        sys.stdout.flush()
        print("\nPaste the REQUEST_TOKEN here: ", end="")
        request_token = input().strip()

        try:
            session_data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            access_token = session_data['access_token']
            self.kite.set_access_token(access_token)
            self._save_token(access_token)
            logger.info("✅ New access token generated.")
            return self.kite
        except Exception as e:
            logger.error(f"❌ Failed to generate session: {e}")
            raise


class TradeWinUtils:
    """
    Class: Tradewin Utility
    Description: Utility class to host common functions being used across Algo
    """

    def __init__(self, tradewin_config):
        self.config = tradewin_config

    @staticmethod
    def log_trade(action, price, pnl=None):
        """Logs trade details including action (buy/sell), price, and optional P&L."""
        price = round(price, 2)
        if pnl is not None:
            logger.info(f"\U0001f6aa EXIT {action} at {round(price, 2):.2f}, P&L: {pnl:.2f}")
        else:
            logger.info(f"\U0001f680 ENTER {action} at {round(price, 2):.2f}")

    def is_market_open(self):

        if self.config.WEEKEND_TESTING:
            return True

        now = datetime.now()
        today = now.date()
        current_time = now.time()

        market_open = dtime(9, 15)
        market_close = dtime(15, 30)

        # Market is open on weekdays (Mon–Fri), not on holidays
        is_weekday = now.weekday() < 5
        not_holiday = today not in self.config.ANNUAL_HOLIDAYS
        is_within_hours = market_open <= current_time <= market_close

        return is_weekday and not_holiday and is_within_hours

    @staticmethod
    def generate_id():
        """Generate a unique trade ID."""
        return str(uuid.uuid4())

    def prepare_trade_data(self, state, exit_price=0.0, pnl=0.0, exited=False):
        now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        return {
            "trade_id": state.trade_id,
            "time": state.entry_time.isoformat() if state.entry_time else now.isoformat(),
            "type": state.position,
            "price": round(state.entry_price, 2),
            "sl": round(state.stop_loss, 2),
            "exited": exited,
            "pnl": round(pnl, 2),
            "strategy": state.strategy,
            "meta_data": {
                "source": "TradeExecutor",
                "notes": "Exited" if exited else "Order placed"
            },
            "symbol": self.config.SYMBOL,
            "exitprice": round(exit_price, 2),
            "exittime": now.isoformat(),
            "lots": 1  # you may override if lots are dynamic
        }
