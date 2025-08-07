# migrated_SLManager.py
from datetime import datetime
from zoneinfo import ZoneInfo
from algo.tradewin_config import TradewinLogger
from algo.tradewin_util import TradeWinUtils

logger = TradewinLogger().get_logger()


class SLManager:
    def __init__(self, config):
        self.cooldown_minutes = config.COOLDOWN_MINUTES

    def check_and_update_sl(self, state, trade_date, current_price, atr, db):
        """
        Apply trailing SL logic and update SL if needed.
        """
        age_seconds = SLManager._age_seconds(state, trade_date)

        if age_seconds < 120:
            logger.debug("⏳ Skipping SL trail — trade age under 2 min")
            return

        near_target = abs(current_price - state.target_price) <= 0.25 * atr
        if near_target:
            logger.info("📌 Near target — tightening SL aggressively")
            new_sl = current_price - 30 if state.position == "BUY" else current_price + 30
            self._maybe_update_sl(state, trade_date, new_sl, current_price, db)
            return

        if state.position == "BUY":
            self._handle_buy_sl(state, trade_date, current_price, atr, db)
        elif state.position == "SELL":
            self._handle_sell_sl(state, trade_date, current_price, atr, db)

    def _handle_buy_sl(self, state, trade_date, price, atr, db):
        move = price - state.entry_price
        fallback_sl = price - (min(50, atr) if self._age_seconds(state, trade_date) > 1800 else atr)
        new_sl = price - atr * 0.6

        if move >= atr:
            logger.debug("Trailing SL for BUY due to ATR move")
            candidate_sl = new_sl if new_sl > state.stop_loss else (
                fallback_sl if fallback_sl > state.stop_loss else None)
            if candidate_sl:
                self._maybe_update_sl(state, trade_date, candidate_sl, price, db)

    def _handle_sell_sl(self, state, trade_date, price, atr, db):
        move = state.entry_price - price
        fallback_sl = price + (min(50, atr) if self._age_seconds(state, trade_date) > 1800 else atr)
        new_sl = price + atr * 0.6

        if move >= atr:
            logger.debug("Trailing SL for SELL due to ATR move")
            candidate_sl = new_sl if new_sl < state.stop_loss else (
                fallback_sl if fallback_sl < state.stop_loss else None)
            if candidate_sl:
                self._maybe_update_sl(state, trade_date, candidate_sl, price, db)

    @staticmethod
    def _maybe_update_sl(state, trade_date, new_sl, price, db):
        new_sl = round(new_sl, 2)
        if abs(new_sl - state.stop_loss) < 0.01:
            logger.debug(f"SL unchanged — {state.stop_loss:.2f}")
            return

        if state.position == "BUY" and new_sl <= state.stop_loss:
            return
        if state.position == "SELL" and new_sl >= state.stop_loss:
            return

        state.stop_loss = new_sl
        state.last_sl_update_time = trade_date
        logger.info(f"📉 Price: {price:.2f} | SL: {new_sl:.2f}")
        TradeWinUtils().prepare_trade_data(state=state, exit_price=0.0, pnl=0.0, exited=False)

    @staticmethod
    def _age_seconds(state, trade_date):
        if state.entry_time is None or not isinstance(trade_date, datetime):
            return 0

        return int((trade_date - state.entry_time).total_seconds())
