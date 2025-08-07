import requests

import pandas as pd
from zoneinfo import ZoneInfo
import time

from algo.tradewin_config import TradewinLogger
from algo.tradewin_strategy import IndicatorCalculator, StrategyApplier, VWAPStrategy, ORBStrategy

logger = TradewinLogger().get_logger()


class MarketData:
    """
    Class: MarketData
    Description : MarketData class for fetching, processing, and preparing technical indicators and strategy-specific
    signals. Supports adaptive switching between ORB and VWAP_REV strategies.
    """

    def __init__(self, api_engine, config, state):
        self.api_engine = api_engine
        self.config = config
        self.state = state

        # Indicator attributes
        self.entry_buffer = self.config.entry_buffer
        self.sl_factor = self.config.orb_sl_factor
        self.target_factor = self.config.orb_target_factor
        self.adaptive_mode = self.config.strategy_mode
        self.active_strategy = self.config.strategy_name

        self.recent_df = None

        # TODO: Needs to be moved to config
        self.retries = 10
        self.backoff = 3

        # Map to hold selected strategy per day
        # TODO: Creation needs to be validated
        self.daily_strategy_map = {}

    def retry_with_backoff(self, func):
        """Utility to retry a function with exponential backoff."""
        for attempt in range(self.retries):
            try:
                return func()
            except Exception as e:
                logger.warning(f"Retry {attempt + 1}/{self.retries} failed: {e}")
                time.sleep(self.backoff ** attempt)

        logger.error("All retry attempts failed.")
        return None

    def get_data(self, config, days=4):
        """Fetch market data using Kite API or local mock server based on config."""

        def fetch():
            try:
                if config.WEEKEND_TESTING:
                    retry_count = 0
                    max_retries = 3

                    while retry_count < max_retries:
                        response = requests.get("http://localhost:8000/historical_data", params={
                            "symbol": "NIFTY_BANK",
                            "interval": "5minute"
                        })

                        if response.status_code == 200:
                            trade_data = pd.DataFrame(response.json())
                            if trade_data.empty or len(trade_data) < 15:
                                logger.info(f"Waiting for sufficient data...{len(trade_data)}")
                                retry_count += 1
                                continue
                            else:
                                trade_data = trade_data.reset_index(drop=True)
                                trade_data['datetime'] = pd.to_datetime(trade_data['date'])
                                trade_data.set_index('datetime', inplace=True)
                                return trade_data
                        else:
                            logger.warning(f"Simulator error: {response.status_code}")
                            retry_count += 1
                            time.sleep(1)

                    logger.warning("❌ No more data from simulator after multiple attempts — aborting.")
                    return pd.DataFrame()  # Return empty DataFrame instead of None

                else:
                    instrument = \
                        self.api_engine.ltp([f"NFO:{config.SYMBOL}"])[f"NFO:{config.SYMBOL}"]["instrument_token"]
                    trade_data = pd.DataFrame(self.api_engine.historical_data(
                        instrument, pd.Timestamp.now() - pd.Timedelta(days=days), pd.Timestamp.now(), config.INTERVAL))

                # ✅ Common post-processing for both simulator and Kite
                trade_data['datetime'] = pd.to_datetime(trade_data['date'])
                trade_data.set_index('datetime', inplace=True)
                trade_data = trade_data[~trade_data.index.duplicated(keep='first')]
                trade_data['date'] = trade_data.index
                return trade_data[['open', 'high', 'low', 'close', 'volume']]

            except Exception as e:
                logger.error(f"❌ Failed to fetch data: {e}", exc_info=True)
                return None

        if config.WEEKEND_TESTING:
            data = fetch()
            if data.empty:
                raise ValueError("❌ Failed to fetch data: simulator did not return enough data.")

            data = IndicatorCalculator.initialize_date_column(data)
            return IndicatorCalculator.add_technical_indicators(data)
        else:
            return self.retry_with_backoff(fetch)

    def prepare_indicators(self, df):
        """
        Prepares indicators for both each strategy. Sets entry, stop loss, and target levels per strategy.
        """

        df = df.copy()
        df = IndicatorCalculator.initialize_date_column(df)
        df = IndicatorCalculator.add_technical_indicators(df)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = StrategyApplier.assign_strategy_levels(
            df,
            self.entry_buffer,
            self.sl_factor,
            self.target_factor,
            self.adaptive_mode,
            self.active_strategy,
            self.daily_strategy_map
        )

        self.recent_df = df

        return df

    def decide_trade_from_row(self, row):
        """Evaluates trade signal for a given row based on active or daily strategy."""

        # @TODO: Determine how this map is being populated
        strategy = self.daily_strategy_map.get(row['date'].date(), "VWAP_REV") \
            if self.config.strategy_mode == "adaptive" else self.config.strategy_name

        result = VWAPStrategy(self).evaluate(row) if strategy == "VWAP_REV" else ORBStrategy(self).evaluate(row)

        # Skip if not valid
        if not result.valid:
            logger.debug(f"Decision rejected — {result.reason}")
            return result

        # ----- Inject custom filters here -----
        index = row.name  # timestamp

        # Momentum filter
        parent_df = self.recent_df  # set by prepare_indicators()

        loc = parent_df.index.get_loc(index)
        current_index = loc.start if isinstance(loc, slice) else loc
        current_row = parent_df.iloc[current_index]

        volume = current_row['volume']
        avg_vol = parent_df['volume'].rolling(window=14).mean().iloc[current_index]
        last_exit_time = self.state.last_exit_time
        last_exit_price = self.state.last_exit_price
        atr = row['ATR']
        row_date = row['date']

        # Ensure volume is greater than 1.2 times avg volume
        if pd.isna(avg_vol) or volume < 1.2 * avg_vol:
            result.valid = False
            result.reason = f"Volume too low: {volume:.0f} < 1.2x avg ({avg_vol:.0f})"
            return result

        # Ensure there is enough momentum across multiple candles
        if not self.is_momentum_confirmed(parent_df, current_index, result.signal):
            result.valid = False
            result.reason = "Weak momentum across last 3 candles"
            return result

        # Weak candle post cooldown
        if self.state.last_exit_time:
            if self.is_post_trade_candle_weak(row, row_date, last_exit_time):
                result.valid = False
                result.reason = "Weak post-cooldown candle"
                return result

        if isinstance(row_date, pd.Timestamp) and row_date.tzinfo is None:
            row_date = row_date.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        # Same-zone reentry
        if self.is_reentry_in_same_zone(
                result.entry, last_exit_price, last_exit_time, atr, row_date):
            result.valid = False
            result.reason = "Same-zone reentry"
            return result

        # Require pullback before re-entry
        if last_exit_price and not self.has_price_moved_enough_after_pullback(
                result.entry, last_exit_price, result.signal, atr):
            result.valid = False
            result.reason = "No pullback for re-entry"
            return result

        return result

    # --- Add more functions below this point

    @staticmethod
    def is_momentum_confirmed(df, current_index, direction):
        """
        Check previous 3 candles for consistent momentum.
        """
        if current_index < 3:
            return False

        prev = df.iloc[current_index - 3:current_index]
        if direction.upper() == "SELL":
            return all(prev['close'].iloc[i] < prev['open'].iloc[i] for i in range(len(prev)))  # 3 bearish candles
        else:
            return all(prev['close'].iloc[i] > prev['open'].iloc[i] for i in range(len(prev)))  # 3 bullish candles

    def is_reentry_in_same_zone(self, price, last_exit_price, last_exit_time, atr, current_time):
        """
        Avoid trades in same zone post cooldown min and < 0.5 * ATR distance.
        """
        if not last_exit_price or not last_exit_time:
            return False

        # Set both entry_time and trade_time to be in the same timezone / format for comparison
        if last_exit_time.tzinfo is None:
            last_exit_time = last_exit_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        time_diff = (current_time - last_exit_time).total_seconds() if last_exit_time else None
        price_diff = abs(price - last_exit_price)

        return (price_diff < 0.5 * atr) and (time_diff < (self.config.COOLDOWN_MINUTES * 60))

    def is_post_trade_candle_weak(self, row, trade_time, entry_time):
        """
        Skip trades if < 5 candles passed since cooldown and body is weak.
        """

        # Set both entry_time and trade_time to be in the same timezone / format for comparison
        if trade_time.tzinfo is None:
            trade_time = trade_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        # Compute time between trade and entry to determine number of candles
        age = (trade_time - entry_time).total_seconds() if entry_time else None

        body = abs(row['close'] - row['open'])
        candle_range = row['high'] - row['low']

        return (age is not None and age < (self.config.COOLDOWN_MINUTES * 60)) and \
               (candle_range < 5 or body < 0.25 * candle_range)

    @staticmethod
    def has_price_moved_enough_after_pullback(price, last_exit_price, direction, atr):
        """
        Permit re-entry only if price has moved >= 0.5 ATR from last exit in same direction.
        """
        if not last_exit_price:
            return False

        if direction == "SELL" and price < last_exit_price - 0.5 * atr:
            return True
        if direction == "BUY" and price > last_exit_price + 0.5 * atr:
            return True

        return False
