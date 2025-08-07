import time

from datetime import datetime
from zoneinfo import ZoneInfo

from algo.tradewin_config import TradewinConfig, TradewinLogger, TradeState
from algo.tradewin_util import TradewinDBConfig, TradeWinUtils
from algo.tradewin_sl_manager import SLManager


class TradeExecutor:
    def __init__(self, kite, trade_state, db_handler=None, logger=None):
        self.logger = logger or TradewinLogger().get_logger()
        self.kite = kite
        self.config = TradewinConfig('tradewin_config.yaml')
        self.state = trade_state

        self.db = db_handler or TradewinDBConfig(self.config.get_db_config())
        self.sl_manager = SLManager(self.config)

        self.atr = 0.0
        self.atr_history = []  # @TODO: Usage to be reviewed
        self.margins = 0
        self.lots = 1

        self.last_exit_time = None
        self.last_exit_price = None
        self.cooldown_secs = self.config.COOLDOWN_MINUTES * 60

    def place_order(self, trade_date, action, price, stoploss, strategy, lots):

        self.state.trade_direction = action
        self.state.entry_price = price
        self.state.stop_loss = stoploss
        self.state.strategy = strategy

        if self.state.open_trade:
            self.logger.warning("Trade already open. Skipping.")
            return

        self._update_trade_state(trade_date, action, price, stoploss, strategy, lots)

        # Persist to DB
        self.db.record_trade(TradeWinUtils(self.config).prepare_trade_data(self.state, exited=False))

        if not self.config.PAPER_TRADING:
            self._place_kite_order(action)

        self.state.open_trade = True

        self.logger.info("🆕 %s: %.2f | SL: %.2f", action, price, stoploss)

    def _place_kite_order(self, action):
        side = self.kite.TRANSACTION_TYPE_BUY if action == "BUY" else self.kite.TRANSACTION_TYPE_SELL
        self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=self.kite.EXCHANGE_NFO,
            tradingsymbol=self.config.SYMBOL,
            transaction_type=side,
            quantity=self.config.TRADE_QTY * self.lots,
            product=self.kite.PRODUCT_MIS,
            order_type=self.kite.ORDER_TYPE_SLM,
            price=0,
            trigger_price=round(self.state.stop_loss, 1)
        )

    def _update_trade_state(self, date, action, price, stoploss, strategy, lots):
        self.state.trade_id = TradeWinUtils.generate_id()
        self.state.entry_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.state.position = action
        self.state.entry_price = round(price, 2)
        self.state.stop_loss = round(stoploss, 2)
        self.state.strategy = strategy
        self.state.open_trade = True
        self.state.last_sl_update_time = None
        self.state.target_price = self._adjust_target_price(action)
        self.state.date = date
        self.lots = lots
        self.state.qty = self.config.TRADE_QTY * lots
        self.state.trade_type = action  # BUY or SELL

    def _adjust_target_price(self, action):
        atr = self.atr or 20
        self.atr_history.append(atr)
        median_atr = sorted(self.atr_history)[len(self.atr_history) // 2] if self.atr_history else atr
        multiplier = 1.8 if atr < median_atr else 2.5
        return self.state.entry_price + multiplier * atr if action == "BUY" else \
            self.state.entry_price - multiplier * atr

    def check_trailing_sl(self, trade_date, current_price):
        if not self.state.open_trade:
            return
        self.sl_manager.check_and_update_sl(self.state, trade_date, current_price, self.atr, self.db)

    def exit_trade(self, price, reason="Manual exit"):
        if not self.state.open_trade:
            self.logger.warning("No open trade to exit.")
            return

        pnl = self._calculate_pnl(price)
        self.margins += pnl

        self.logger.info("💰 Exiting trade at %.2f with P&L: %.2f — Reason: %s", price, pnl, reason)

        self.db.record_trade(TradeWinUtils(self.config).prepare_trade_data(
            self.state, exit_price=price, pnl=pnl, exited=True))
        self._update_exit_state(price)

        # update state: mark trade closed and record exit time
        self.state.open_trade = False
        self.state.last_exit_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.state.entry_price = None
        self.state.stop_loss = None
        self.state.trade_direction = None
        self.state.strategy = None

    def _update_exit_state(self, price):
        self.state.reset()
        self.state.last_exit_price = price
        self.state.last_exit_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))

    def _calculate_pnl(self, exit_price):
        entry = self.state.entry_price
        qty = self.state.qty
        trade_type = self.state.trade_type
        direction = self.state.trade_direction

        # Gross PnL
        gross_pnl = (entry - exit_price) * qty if direction == "SELL" else (exit_price - entry) * qty

        # Charges calculation
        turnover = (entry + exit_price) * qty
        brokerage = min(20, 0.0003 * turnover) * 2  # max ₹20 per leg
        stt = 0.00025 * exit_price * qty if trade_type == "SELL" else 0
        gst = 0.18 * brokerage
        sebi = 0.000001 * turnover
        stamp = 0.00003 * entry * qty if trade_type == "BUY" else 0

        total_charges = brokerage + stt + gst + sebi + stamp
        net_pnl = gross_pnl - total_charges

        self.logger.debug(f"""
            🧾 Charge Breakdown:
            ➖ Gross PnL: {gross_pnl:.2f}
            ➖ Brokerage: {brokerage:.2f}
            ➖ STT: {stt:.2f}
            ➖ GST: {gst:.2f}
            ➖ SEBI: {sebi:.2f}
            ➖ Stamp Duty: {stamp:.2f}
            💰 Net PnL: {net_pnl:.2f}
        """)

        return round(net_pnl, 2)

    def in_cooldown(self) -> bool:
        """Return True if within the cooldown period since last exit."""
        last = self.state.last_exit_time

        if last is None or not isinstance(last, datetime):
            return False

        # Use the same tzinfo as last_exit_time to compute elapsed
        now = datetime.now(tz=last.tzinfo) if last.tzinfo else datetime.now()
        elapsed = now - last

        return elapsed.total_seconds() < self.cooldown_secs

    def reached_cutoff_time(self):
        return not self.config.WEEKEND_TESTING and datetime.now().time() >= datetime.strptime("15:25", "%H:%M").time()

    def fetch_pnl_today(self):
        return self.db.fetch_pnl_today()

    def close(self):
        self.db.close()

    def populate_trade_logs(self):
        self.db.populate_logs()

    def monitor_trade(self, get_data_func, prepare_func, interval=60):
        """
        Monitor an active trade. Fetch price data using `get_data_func`,
        enrich with indicators via `prepare_func`, and update trade status.
        """

        try:
            while True:
                df = get_data_func()
                if df is None or df.empty:
                    self.logger.warning("⚠️ No data during monitor_trade. Retrying in %d seconds...", interval)
                    time.sleep(interval)
                    continue

                df = prepare_func(df)

                try:
                    self.atr = df['ATR'].iloc[-1] or self.atr
                except Exception as e:
                    self.logger.warning(f"⚠️ Could not update ATR from data: {e}")

                # Example: check SL or target
                current_price = df['close'].iloc[-1]
                if self.state.stop_loss is not None:
                    self.logger.info(f"📈 Price: {current_price:.2f} | SL: {self.state.stop_loss:.2f}")
                else:
                    self.logger.info(f"📈 Price: {current_price:.2f} | SL: None")

                # Call trailing SL manager
                self.check_trailing_sl(df.index[-1], current_price)

                if self.state.trade_direction == "SELL" and current_price > self.state.stop_loss:
                    self.logger.info(f"❌ SELL: SL hit at {current_price:.2f}")
                    pnl = self._calculate_pnl(current_price)
                    self.margins += pnl
                    self.logger.info("💸 P&L: %.2f (incl. charges)", pnl)
                    self.db.record_trade(
                        TradeWinUtils(self.config).prepare_trade_data(
                            self.state, exit_price=current_price, pnl=pnl, exited=True))
                    self._update_exit_state(current_price)
                    self.state.last_exit_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
                    break

                elif self.state.trade_direction == "BUY" and current_price < self.state.stop_loss:
                    self.logger.info(f"❌ BUY: SL hit at {current_price:.2f}")
                    pnl = self._calculate_pnl(current_price)
                    self.margins += pnl
                    self.logger.info("💸 P&L: %.2f (incl. charges)", pnl)
                    self.db.record_trade(
                        TradeWinUtils(self.config).prepare_trade_data(
                            self.state, exit_price=current_price, pnl=pnl, exited=True))
                    self._update_exit_state(current_price)
                    self.state.last_exit_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
                    break

                elif self.state.entry_time and not self.state.checked_post_entry:
                    df = get_data_func()
                    df = prepare_func(df)
                    if df is not None and len(df) > 5:
                        valid, passed = self.post_entry_health_check(df, self.state.entry_time)
                        self.state.checked_post_entry = True
                        if valid == "valid" and not passed:
                            self.logger.info("⚠️ Weak follow-through detected after entry. Exiting early.")
                            self.exit_trade(price=self.state.entry_price, reason="Weak post-entry momentum")
                            break

                time.sleep(interval)

        except KeyboardInterrupt:
            self.logger.info("🔁 Monitor loop interrupted manually.")
        except Exception as e:
            self.logger.error(f"❌ Error in get_data_func during monitoring: {e}")
            return

    @staticmethod
    def post_entry_health_check(df, entry_time, lookahead=3, threshold_pct=0.15):
        """
        Check if after entry_time, price moved in expected direction by threshold.
        """
        if entry_time not in df.index:
            return "invalid", False

        entry_idx = df.index.get_loc(entry_time)
        if isinstance(entry_idx, slice):
            return "invalid", False

        if entry_idx + lookahead >= len(df):
            return "invalid", False  # Not enough candles

        direction = "BUY" if df.iloc[entry_idx]['close'] > df.iloc[entry_idx]['open'] else "SELL"
        entry_price = df.iloc[entry_idx]['close']

        future = df.iloc[entry_idx + 1:entry_idx + 1 + lookahead]['close']
        max_move = future.max() if direction == "BUY" else future.min()
        move_pct = abs((max_move - entry_price) / entry_price) * 100

        return "valid", move_pct >= threshold_pct  # True = strong move → keep trade
