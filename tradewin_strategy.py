import numpy as np
import pandas as pd
from datetime import time as dt_time
from algo.tradewin_config import TradeDecision, TradewinLogger

logger = TradewinLogger().get_logger()

ORB_WINDOW_START = dt_time(9, 15)
ORB_WINDOW_END = dt_time(9, 30)


class IndicatorCalculator:
    @staticmethod
    def initialize_date_column(df):
        if 'date' not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df['date'] = df.index
            else:
                raise ValueError("Missing datetime index or 'date' column.")
        return df[~df.index.isna()]

    @staticmethod
    def add_technical_indicators(df):
        df['open_prev_1'] = df['open'].shift(1)
        df['close_prev_1'] = df['close'].shift(1)
        df['open_prev_2'] = df['open'].shift(2)
        df['close_prev_2'] = df['close'].shift(2)

        df['EMA5'] = df['close'].ewm(span=5, adjust=False).mean()
        df['EMA20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['RSI14'] = 100 - (100 / (1 + df['close'].pct_change().add(1).rolling(window=14).mean()))
        df['ATR'] = (df['high'] - df['low']).rolling(window=14).mean()
        df['MACD'] = df['close'].ewm(span=12, adjust=False).mean() - df['close'].ewm(span=26, adjust=False).mean()
        df['VWAP_REV'] = (df['high'] + df['low'] + df['close']) / 3
        df['prev_close'] = df['close'].shift(1)
        return df


class StrategyApplier:
    @staticmethod
    # TODO: Review needed for this code
    def assign_strategy_levels(df, entry_buffer, sl_factor, target_factor, adaptive_mode, active_strategy,
                               strategy_map):
        df = df.copy()
        for col in ['orb_long_entry', 'orb_short_entry', 'orb_sl', 'orb_target']:
            if col not in df.columns:
                df[col] = float('nan')

        if adaptive_mode or active_strategy == "ORB":
            df['date_only'] = df['date'].dt.date
            for date_val, group in df.groupby('date_only'):
                morning_range = group[
                    (group['date'].dt.time >= ORB_WINDOW_START) & (group['date'].dt.time <= ORB_WINDOW_END)]

                if (morning_range['high'].max() - morning_range['low'].min()) < 25:
                    logger.warning(f"Skipping strategy assignment on {date_val} due to narrow morning range.")
                    continue

                strategy = StrategyApplier.choose_strategy(group, adaptive_mode, active_strategy)
                strategy_map[date_val] = strategy

                if strategy == "ORB":
                    high = morning_range['high'].max()
                    low = morning_range['low'].min()
                    mask = df['date'].dt.date == date_val

                    df.loc[mask, 'orb_long_entry'] = high + entry_buffer
                    df.loc[mask, 'orb_short_entry'] = low - entry_buffer
                    atr_series = df.loc[mask, 'ATR'].fillna(20)  # default fallback
                    orb_sl = np.maximum(20, atr_series * sl_factor)
                    df.loc[mask, 'orb_sl'] = orb_sl
                    df.loc[mask, 'orb_target'] = orb_sl * target_factor
                else:
                    df.loc[df['date'].dt.date == date_val, ['orb_long_entry', 'orb_short_entry', 'orb_sl',
                                                            'orb_target']] = 0
            df.drop(columns=['date_only'], inplace=True)

        if active_strategy == "VWAP_REV":
            df[['orb_long_entry', 'orb_short_entry', 'orb_sl', 'orb_target']] = 0

        return df

    @staticmethod
    def choose_strategy(df_day, adaptive_mode, active_strategy):
        if not adaptive_mode:
            return active_strategy

        morning_df = df_day[(df_day['date'].dt.time >= ORB_WINDOW_START) & (df_day['date'].dt.time <= ORB_WINDOW_END)]
        atr_morning = (morning_df['high'] - morning_df['low']).mean()
        return "ORB" if atr_morning and atr_morning > 15 else "VWAP_REV"


class VWAPStrategy:
    def __init__(self, parent):
        self.dev = parent.vwap_dev
        self.sl_mult = parent.sl_mult
        self.target_mult = parent.target_mult
        self.rr_threshold = parent.rr_threshold

    def evaluate(self, row):
        try:
            entry = row['close']
            vwap = row['VWAP_REV']
            atr = row['ATR']
            ema20 = row['EMA20']
            prev_close = row['prev_close']
            body = abs(row['close'] - row['open'])
            candle_range = row['high'] - row['low']

            if candle_range < 5 or body < 0.25 * candle_range:
                # Skip weak candles
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason=f"Weak candle {entry}")

            if pd.isna(entry) or pd.isna(atr):
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason="Missing entry or ATR")

            if any(pd.isna([vwap, atr, row['RSI14'], ema20, prev_close])):
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason="Missing indicator value(s)")

            if atr / entry < 0.0001 or atr < 5:
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason=f"ATR too low {atr} < 5")

            threshold_above = vwap + self.dev * entry
            threshold_below = vwap - self.dev * entry

            if entry > threshold_above >= prev_close and entry > ema20:
                dt = row['date']
                sl = row['orb_sl']
                target = row['orb_target']
                ok = (target - entry) >= self.rr_threshold * (entry - sl)

                if (target - entry) < self.rr_threshold * (entry - sl):
                    return TradeDecision(
                        date=None, signal=None, entry=None, sl=None, target=None,
                        valid=ok, strategy=None, reason=f"Risk/reward too low {(target - entry)} < "
                                                        f"{self.rr_threshold * (entry - sl)}")

                return TradeDecision(
                    date=dt, signal="BUY", entry=entry, sl=sl, target=target, valid=ok, strategy="VWAP_REV")

            if entry < threshold_below <= prev_close and entry < ema20:
                dt = row['date']
                sl = row['orb_sl']
                target = row['orb_target']
                ok = (target - entry) < self.rr_threshold * (entry - sl)

                if (entry - target) < self.rr_threshold * (sl - entry):
                    return TradeDecision(
                        date=None, signal=None, entry=None, sl=None, target=None,
                        valid=ok, strategy=None, reason=f"Risk/reward too low {(entry - target)} < "
                                                        f"{self.rr_threshold * (sl - entry)}")

                return TradeDecision(
                    date=dt, signal="SELL", entry=entry, sl=sl, target=target, valid=ok, strategy="VWAP_REV")

            return TradeDecision(
                date=None, signal=None, entry=None, sl=None, target=None,
                valid=False, strategy=None, reason="No VWAP_REV signal conditions met")

        except Exception as e:
            logger.error(f"VWAP strategy error: {e}")

            return TradeDecision(
                date=None, signal=None, entry=None, sl=None, target=None,
                valid=False, strategy=None, reason=f"Exception: {e}")


class ORBStrategy:
    def __init__(self, parent):
        self.sl_factor = parent.sl_factor
        self.target_factor = parent.target_factor

    @staticmethod
    def evaluate(row):
        try:
            body = abs(row['close'] - row['open'])
            candle_range = row['high'] - row['low']

            current_time = row['date'].time()
            if not (dt_time(9, 30) <= current_time <= dt_time(15, 25)):
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason="Outside trading window")

            if candle_range < 5 or body < 0.25 * candle_range:
                # Skip weak candles
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason=f"Weak candle {row['close']}")

            atr = row['ATR']
            if pd.isna(atr) or atr < 10:  # Lower the threshold
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason=f"ATR {round(atr, 2)} < 10 or missing")

            bullish = row['close_prev_1'] > row['open_prev_1']
            bearish = row['close_prev_1'] < row['open_prev_1']

            long_entry = row['orb_long_entry']
            short_entry = row['orb_short_entry']

            if pd.isna(long_entry) or pd.isna(short_entry):
                return TradeDecision(
                    date=None, signal=None, entry=None, sl=None, target=None,
                    valid=False, strategy=None, reason="Missing ORB levels")

            # --- LONG ---
            if row['high'] >= long_entry and bullish:
                dt = row['date']
                entry = row['close']
                sl = entry - row['orb_sl']
                target = entry + row['orb_target']

                return TradeDecision(
                    date=dt, signal="BUY", entry=entry, sl=sl, target=target, valid=True, strategy="ORB")

            # --- SHORT ---
            if row['low'] <= short_entry and bearish:
                dt = row['date']
                entry = row['close']
                sl = entry + row['orb_sl']
                target = entry - row['orb_target']

                return TradeDecision(
                    date=dt, signal="SELL", entry=entry, sl=sl, target=target, valid=True, strategy="ORB")

            return TradeDecision(
                date=None, signal=None, entry=None, sl=None, target=None,
                valid=False, strategy=None, reason="No ORB conditions met")

        except Exception as e:
            logger.error(f"ORB strategy error: {e}")

            return TradeDecision(
                date=None, signal=None, entry=None, sl=None, target=None,
                valid=False, strategy=None, reason=f"Exception: {e}")
