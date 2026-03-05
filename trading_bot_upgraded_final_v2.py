import os
import requests
import time
import pandas as pd
import numpy as np
import traceback
import datetime
import json
import random
from typing import List, Dict, Optional, Tuple
import firebase_admin
from firebase_admin import credentials, firestore

# ============================================================
# CONFIGURATION & INFRASTRUCTURE
# ============================================================

class Config:
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # TwelveData API Keys
    TWELVEDATA_API_KEY_1 = os.getenv("TWELVEDATA_API_KEY_1")
    TWELVEDATA_API_KEY_2 = os.getenv("TWELVEDATA_API_KEY_2")
    TWELVEDATA_API_KEY_3 = os.getenv("TWELVEDATA_API_KEY_3")
    
    FIREBASE_SERVICE_ACCOUNT_KEY = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY")
    
    # TwelveData Limits per Key
    MINUTE_CALL_LIMIT = 7 # STRICT HARD LIMIT (Buffer safety)
    DAILY_CALL_LIMIT = 800
    
    CONFIDENCE_THRESHOLD = 65.0
    
    SYMBOLS = [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD",
        "AUD/USD", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY",
        "EUR/AUD", "EUR/CAD", "EUR/CHF", "GBP/AUD", "GBP/CAD",
        "GBP/CHF", "AUD/JPY", "AUD/CAD", "AUD/CHF", "NZD/JPY",
        "NZD/CAD", "NZD/CHF", "CAD/JPY", "CAD/CHF", "CHF/JPY",
        "USD/SGD", "USD/HKD", "EUR/SGD", "GBP/SEK", "EUR/NZD"
    ]
    
    # Weights for the scoring model (Hierarchical)
    WEIGHT_MACRO = 0.40      # 35-45%
    WEIGHT_STRUCTURAL = 0.35 # 25-35%
    WEIGHT_EXECUTION = 0.25  # 20-30%

    # Initial preference weights for toolbox components
    TOOLBOX_PREFERENCES = {
        "macro_trend_alignment": 1.0,
        "macro_volatility_expansion": 1.0,
        "structural_bos_choch": 1.0,
        "structural_volatility_contraction": 1.0,
        "structural_aoi_durability": 1.0,
        "structural_aoi_entropy": 1.0,
        "execution_impulse_reaction": 1.0,
        "execution_liquidity_sweep": 1.0,
        "liquidity_sweep_confirmation": 1.0,
        "multi_timeframe_support_resistance_mapping": 1.0,
        "pattern_intelligence_toolbox": 1.0,
        "order_block_confluence": 1.0,
        "fvg_confluence": 1.0,
        "structural_true_sr_weight": 1.0
    }

class APIKeyManager:
    def __init__(self):
        self.keys = [
            Config.TWELVEDATA_API_KEY_1,
            Config.TWELVEDATA_API_KEY_2,
            Config.TWELVEDATA_API_KEY_3
        ]
        # Filter out None keys
        self.keys = [k for k in self.keys if k]
        
        if not self.keys:
            raise ValueError("No TwelveData API keys configured.")
            
        self.current_index = 0
        
        # 1️⃣ Create Per-Key State Structure
        self.usage = {
            key: {
                "calls_used": 0,
                "daily_calls": 0,
                "minute_window_start": time.time(),
                "daily_start": time.time()
            } for key in self.keys
        }

    def get_active_key(self) -> Optional[str]:
        if not self.keys:
            return None
            
        start_index = self.current_index
        while True:
            key = self.keys[self.current_index]
            stats = self.usage[key]
            now = time.time()
            
            # 2️⃣ Reset Logic (MUST RUN BEFORE EVERY REQUEST)
            if now - stats["minute_window_start"] >= 60:
                stats["calls_used"] = 0
                stats["minute_window_start"] = now
            if now - stats["daily_start"] >= 86400:
                stats["daily_calls"] = 0
                stats["daily_start"] = now
                
            # 3️⃣ Hard Ceiling Enforcement (PRE-REQUEST BLOCK)
            if stats["calls_used"] < Config.MINUTE_CALL_LIMIT and stats["daily_calls"] < Config.DAILY_CALL_LIMIT:
                # Immediately AFTER selecting a valid key (and BEFORE sending request): increment calls_used += 1
                stats["calls_used"] += 1
                stats["daily_calls"] += 1
                return key
            
            # Rotate to next key
            self.current_index = (self.current_index + 1) % len(self.keys)
            
            # 4️⃣ If All Keys Exhausted
            if self.current_index == start_index:
                # find earliest reset time
                now = time.time()
                wait_times = []
                for k in self.keys:
                    s = self.usage[k]
                    if s["daily_calls"] < Config.DAILY_CALL_LIMIT:
                        wait_times.append((s["minute_window_start"] + 60) - now)
                
                if wait_times:
                    sleep_duration = max(0, min(wait_times))
                    if sleep_duration > 0:
                        print(f"All keys rate-limited. Sleeping for {sleep_duration:.2f}s...")
                        time.sleep(sleep_duration)
                    continue # After sleep, re-evaluate keys again.
                else:
                    print("All keys reached daily limit.")
                    return None

class RequestHandler:
    def __init__(self, key_manager: APIKeyManager):
        self.key_manager = key_manager
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/118.0"
        ]
        self.proxies = os.getenv("PROXY_LIST", "").split(",")
        self.proxies = [p.strip() for p in self.proxies if p.strip()]

    def _get_request_config(self) -> Dict:
        config = {
            "headers": {
                "User-Agent": random.choice(self.user_agents),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
            "timeout": 15
        }
        if self.proxies:
            proxy = random.choice(self.proxies)
            config["proxies"] = {"http": proxy, "https": proxy}
        return config

    # 5️⃣ Mandatory Architectural Rule: ALL TwelveData API calls must go through ONE single wrapper function
    def get_data(self, symbol: str, interval: str, outputsize: int = 500) -> Optional[pd.DataFrame]:
        api_key = self.key_manager.get_active_key()
        if not api_key:
            return None
            
        try:
            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={api_key}"
            config = self._get_request_config()
            response = self.session.get(url, **config)
            data = response.json()
            
            # 6️⃣ Remove Reactive Logic (Removed retry loops and reactive sleeps)
            if "values" not in data:
                print(f"Error fetching {interval} data for {symbol}: {data.get('message', 'Unknown error')}")
                return None
                
            df = pd.DataFrame(data["values"])
            if 'volume' not in df.columns:
                df['volume'] = 0
            cols_to_convert = ["open", "high", "low", "close", "volume"]
            df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric)
            df["datetime"] = pd.to_datetime(df["datetime"])
            return df.set_index("datetime").sort_index()
        except Exception as e:
            print(f"Request error for {symbol} ({interval}): {e}")
            return None

class ToolboxPreference:
    def __init__(self):
        self.preferences = Config.TOOLBOX_PREFERENCES.copy()
        self.component_states = {comp: "ACTIVE" for comp in self.preferences.keys()}

    def get_preference(self, component_name: str) -> float:
        if self.component_states.get(component_name) == "SUSPENDED":
            return 0.0
        return self.preferences.get(component_name, 1.0)

    def adjust_preference(self, component_name: str, adjustment: float, smoothing_factor: float = 0.02):
        current_pref = self.preferences.get(component_name, 1.0)
        delta = adjustment * smoothing_factor
        delta = max(-0.03, min(0.03, delta))
        new_pref = current_pref + delta
        self.preferences[component_name] = max(0.1, min(new_pref, 2.0))
        print(f"Adjusted preference for {component_name}: {current_pref:.4f} -> {self.preferences[component_name]:.4f} (Delta: {delta:.4f})")

    def update_state(self, component_name: str, win_ratio: float):
        current_state = self.component_states.get(component_name, "ACTIVE")
        if current_state == "ACTIVE":
            if win_ratio < 0.35:
                self.component_states[component_name] = "SUSPENDED"
                print(f"Component {component_name} SUSPENDED (WR: {win_ratio:.2f})")
        elif current_state == "SUSPENDED":
            if win_ratio > 0.45:
                self.component_states[component_name] = "ACTIVE"
                print(f"Component {component_name} REACTIVATED (WR: {win_ratio:.2f})")

class FirebaseManager:
    _db = None

    @classmethod
    def initialize_firebase(cls):
        if cls._db is None:
            try:
                if Config.FIREBASE_SERVICE_ACCOUNT_KEY:
                    cred_dict = json.loads(Config.FIREBASE_SERVICE_ACCOUNT_KEY)
                    cred = credentials.Certificate(cred_dict)
                    firebase_admin.initialize_app(cred)
                    cls._db = firestore.client()
                    print("Firebase initialized successfully.")
                else:
                    print("Firebase credentials not found in environment.")
            except Exception as e:
                print(f"Firebase initialization error: {e}")

    @staticmethod
    def record_trade_outcome(trade_data: Dict):
        db = FirebaseManager._db
        if db:
            try:
                trade_data["timestamp"] = firestore.SERVER_TIMESTAMP
                db.collection("trade_outcomes").add(trade_data)
            except Exception as e:
                print(f"Error recording trade outcome: {e}")

    @staticmethod
    def get_trade_outcomes(limit=100):
        db = FirebaseManager._db
        if not db:
            return []
        try:
            docs = db.collection("trade_outcomes").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            print(f"Error fetching trade outcomes: {e}")
            return []

class NotificationManager:
    def send_telegram(self, message: str):
        if not Config.TELEGRAM_TOKEN or not Config.TELEGRAM_CHAT_ID:
            print(f"Telegram Notification: {message}")
            return
        try:
            url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
            payload = {"chat_id": Config.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"Telegram error: {e}")

class TA:
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(period).mean()

    @staticmethod
    def get_fractal_swings(df: pd.DataFrame, window: int = 2) -> List[Dict]:
        swings = []
        for i in range(window, len(df) - window):
            if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, window+1)) and \
               all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, window+1)):
                swings.append({"type": "high", "price": df['high'].iloc[i], "index": i, "time": df.index[i]})
            if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, window+1)) and \
               all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, window+1)):
                swings.append({"type": "low", "price": df['low'].iloc[i], "index": i, "time": df.index[i]})
        return swings

    @staticmethod
    def get_manifold_swings(df: pd.DataFrame) -> List[Dict]:
        return TA.get_fractal_swings(df, window=3)

    @staticmethod
    def _get_session_times(df: pd.DataFrame) -> Dict:
        return {
            "ASIA": (datetime.time(0, 0), datetime.time(8, 0)),
            "LONDON": (datetime.time(7, 0), datetime.time(16, 0)),
            "NY": (datetime.time(12, 0), datetime.time(21, 0))
        }

    @staticmethod
    def detect_session_liquidity(df: pd.DataFrame) -> List[Dict]:
        sessions = TA._get_session_times(df)
        liquidity = []
        for session_name, (start, end) in sessions.items():
            session_df = df.between_time(start, end)
            if not session_df.empty:
                liquidity.append({"type": f"{session_name}_HIGH", "price": session_df['high'].max(), "timeframe": "SESSION"})
                liquidity.append({"type": f"{session_name}_LOW", "price": session_df['low'].min(), "timeframe": "SESSION"})
        return liquidity

    @staticmethod
    def detect_equal_highs_lows(df: pd.DataFrame, threshold_pips: float = 2.0) -> List[Dict]:
        swings = TA.get_fractal_swings(df)
        levels = []
        for i in range(len(swings)):
            for j in range(i + 1, len(swings)):
                if swings[i]["type"] == swings[j]["type"]:
                    diff = abs(swings[i]["price"] - swings[j]["price"])
                    if diff <= threshold_pips * 0.0001:
                        levels.append({"type": f"EQUAL_{swings[i]['type'].upper()}S", "price": (swings[i]["price"] + swings[j]["price"]) / 2, "timeframe": "CURRENT"})
        return levels

    @staticmethod
    def detect_swing_liquidity(df: pd.DataFrame) -> List[Dict]:
        swings = TA.get_fractal_swings(df, window=5)
        liquidity_levels = []
        for swing in swings:
            if swing["type"] == "high":
                liquidity_levels.append({"type": "SWING_HIGH", "price": swing["price"], "timeframe": "CURRENT"})
            elif swing["type"] == "low":
                liquidity_levels.append({"type": "SWING_LOW", "price": swing["price"], "timeframe": "CURRENT"})
        return liquidity_levels

    @staticmethod
    def confirm_liquidity_sweep(df: pd.DataFrame, liquidity_level: float, direction: str, higher_timeframe_bias: str, atr_multiple: float = 0.5) -> bool:
        if len(df) < 2: return False
        last_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        if direction == "BUY":
            if prev_candle["low"] < liquidity_level and last_candle["close"] > liquidity_level:
                return True
        elif direction == "SELL":
            if prev_candle["high"] > liquidity_level and last_candle["close"] < liquidity_level:
                return True
        return False

    @staticmethod
    def detect_aoi(df: pd.DataFrame, swings: List[Dict], timeframe: str, memory: List[Dict]) -> List[Dict]:
        atr = TA.calculate_atr(df).iloc[-1]
        aois = []
        prices = [s["price"] for s in swings]
        for p in set(prices):
            touches = sum(1 for sp in prices if abs(sp - p) < 0.2 * atr)
            if touches >= 2:
                rejection_strength = 0
                for i in range(len(df) - 10, len(df)):
                    high, low, close, open_p = df['high'].iloc[i], df['low'].iloc[i], df['close'].iloc[i], df['open'].iloc[i]
                    if abs(high - p) < 0.3 * atr or abs(low - p) < 0.3 * atr:
                        body_size = abs(close - open_p) + 0.00001
                        if close < open_p:
                            rejection_strength += (high - open_p) / body_size
                        else:
                            rejection_strength += (p - low) / body_size
                durability_score = min(touches * 10, 100) + min(rejection_strength * 5, 50)
                aois.append({"price": p, "min": p - 0.2 * atr, "max": p + 0.2 * atr, "durability_score": durability_score, "timeframe": timeframe})
        return aois

    @staticmethod
    def calculate_zone_entropy(df: pd.DataFrame, aois: List[Dict]) -> float:
        if not aois: return 0.0
        last_close = df["close"].iloc[-1]
        atr = TA.calculate_atr(df).iloc[-1]
        if atr == 0: return 0.0
        entropy = 0
        for a in aois:
            dist = abs(last_close - a["price"]) / atr
            if dist < 1.5:
                entropy += (1.5 - dist) * (a["durability_score"] / 100.0)
        return min(entropy * 20, 100.0)

    @staticmethod
    def calculate_volatility(df: pd.DataFrame, period: int = 14) -> pd.Series:
        returns = np.log(df['close'] / df['close'].shift(1))
        return returns.rolling(period).std().fillna(0.0)

    @staticmethod
    def detect_volume_spike(df: pd.DataFrame, period: int = 20, threshold: float = 1.5) -> bool:
        if 'volume' not in df.columns or len(df) < period:
            return False
        current_volume = df['volume'].iloc[-1]
        average_volume = df['volume'].iloc[-period:-1].mean()
        std_volume = df['volume'].iloc[-period:-1].std()
        if std_volume == 0:
            return current_volume > average_volume * threshold
        return current_volume > (average_volume + threshold * std_volume)

    @staticmethod
    def detect_order_block(df: pd.DataFrame, lookback: int = 10) -> List[Dict]:
        order_blocks = []
        for i in range(lookback, len(df)):
            current = df.iloc[i]
            prev = df.iloc[i-1]
            if prev["close"] < prev["open"] and current["close"] > current["open"] and current["low"] > prev["high"]:
                if (current["close"] - prev["close"]) > TA.calculate_atr(df.iloc[i-lookback:i+1]).iloc[-1] * 0.5:
                    order_blocks.append({"type": "BULLISH_OB", "open": prev["open"], "close": prev["close"], "high": prev["high"], "low": prev["low"], "time": prev.name})
            elif prev["close"] > prev["open"] and current["close"] < current["open"] and current["high"] < prev["low"]:
                if (prev["close"] - current["close"]) > TA.calculate_atr(df.iloc[i-lookback:i+1]).iloc[-1] * 0.5:
                    order_blocks.append({"type": "BEARISH_OB", "open": prev["open"], "close": prev["close"], "high": prev["high"], "low": prev["low"], "time": prev.name})
        return order_blocks

    @staticmethod
    def detect_micro_bos(df: pd.DataFrame, direction: str, lookback: int = 10) -> bool:
        swings = TA.get_fractal_swings(df.tail(lookback))
        if len(swings) < 2: return False
        last_swing = swings[-1]
        second_last_swing = swings[-2]
        if direction == "BUY":
            if last_swing["type"] == "high" and df["close"].iloc[-1] > last_swing["price"] and second_last_swing["type"] == "low":
                return True
        elif direction == "SELL":
            if last_swing["type"] == "low" and df["close"].iloc[-1] < last_swing["price"] and second_last_swing["type"] == "high":
                return True
        return False

    @staticmethod
    def detect_fair_value_gap(df: pd.DataFrame, lookback: int = 20) -> List[Dict]:
        fvgs = []
        for i in range(2, len(df)):
            candle1, candle2, candle3 = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
            if candle3["low"] > candle1["high"]:
                fvgs.append({"type": "BULLISH_FVG", "min": candle1["high"], "max": candle3["low"], "time": candle2.name})
            elif candle3["high"] < candle1["low"]:
                fvgs.append({"type": "BEARISH_FVG", "min": candle3["high"], "max": candle1["low"], "time": candle2.name})
        return fvgs

    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> Dict:
        last_5 = df.tail(5)
        score, strength = 0, 0.5
        body = abs(last_5["close"].iloc[-1] - last_5["open"].iloc[-1])
        wick_top = last_5["high"].iloc[-1] - max(last_5["close"].iloc[-1], last_5["open"].iloc[-1])
        wick_bottom = min(last_5["close"].iloc[-1], last_5["open"].iloc[-1]) - last_5["low"].iloc[-1]
        if wick_bottom > body * 2:
            score = 70
            strength = min(wick_bottom / (body + 0.0001) * 0.2, 1.0)
        elif wick_top > body * 2:
            score = 70
            strength = min(wick_top / (body + 0.0001) * 0.2, 1.0)
        return {"score": score, "pattern_strength": strength}

class TradingBot:
    def __init__(self):
        self.key_manager = APIKeyManager()
        self.handler = RequestHandler(self.key_manager)
        self.toolbox_prefs = ToolboxPreference()
        self.notifier = NotificationManager()
        FirebaseManager.initialize_firebase()
        self.aoi_memory = {}
        self.sr_probability_gate = 0.5 
        self.trade_count_since_last_recalc = 0

    def _get_current_session_bias(self) -> float:
        now_utc = datetime.datetime.utcnow().time()
        sessions = TA._get_session_times(pd.DataFrame())
        london_start, london_end = sessions["LONDON"]
        ny_start, ny_end = sessions["NY"]
        if (london_start <= now_utc < london_end) or (ny_start <= now_utc < ny_end):
            return 1.0
        return 0.5

    def _detect_all_liquidity(self, df: pd.DataFrame) -> List[Dict]:
        all_liquidity = []
        all_liquidity.extend(TA.detect_session_liquidity(df))
        all_liquidity.extend(TA.detect_equal_highs_lows(df))
        all_liquidity.extend(TA.detect_swing_liquidity(df))
        return all_liquidity

    def get_macro_layer(self, df: pd.DataFrame) -> Dict:
        ema_fast = df["close"].ewm(span=12).mean()
        ema_slow = df["close"].ewm(span=26).mean()
        last_close = df["close"].iloc[-1]
        trend = "BULLISH" if ema_fast.iloc[-1] > ema_slow.iloc[-1] and last_close > ema_fast.iloc[-1] else "BEARISH" if ema_fast.iloc[-1] < ema_slow.iloc[-1] and last_close < ema_fast.iloc[-1] else "NEUTRAL"
        pref_trend = self.toolbox_prefs.get_preference("macro_trend_alignment")
        pref_vol = self.toolbox_prefs.get_preference("macro_volatility_expansion")
        score = (60 if trend != "NEUTRAL" else 0) * pref_trend
        atr = TA.calculate_atr(df)
        if atr.iloc[-1] > atr.iloc[-5:].mean(): score += 40 * pref_vol
        return {"score": score, "trend": trend}

    def get_structural_layer(self, df: pd.DataFrame) -> Dict:
        swings = TA.get_manifold_swings(df)
        score, current_trend = 0, "NEUTRAL"
        if len(swings) >= 3:
            s3, s2, s1 = swings[-3], swings[-2], swings[-1]
            if s2["type"] == "low" and s1["type"] == "high" and s1["price"] > s2["price"]:
                current_trend = "BULLISH"
            elif s2["type"] == "high" and s1["type"] == "low" and s1["price"] < s2["price"]:
                current_trend = "BEARISH"
        pref_bos = self.toolbox_prefs.get_preference("structural_bos_choch")
        pref_vol_cont = self.toolbox_prefs.get_preference("structural_volatility_contraction")
        if current_trend != "NEUTRAL": score += 60 * pref_bos
        vol = TA.calculate_volatility(df)
        if len(vol) > 5 and vol.iloc[-1] < vol.iloc[-5:-1].mean(): score += 40 * pref_vol_cont
        return {"score": score, "trend": current_trend}

    def get_execution_layer(self, df: pd.DataFrame) -> Dict:
        last_candle = df.iloc[-1]
        body = abs(last_candle["close"] - last_candle["open"])
        atr = TA.calculate_atr(df).iloc[-1]
        direction = "NEUTRAL"
        score = 0
        if body > 0.5 * atr:
            direction = "BUY" if last_candle["close"] > last_candle["open"] else "SELL"
            score = 70 * self.toolbox_prefs.get_preference("execution_impulse_reaction")
        return {"score": score, "direction": direction}

    def get_aoi_confluence_score(self, symbol: str, df_15m: pd.DataFrame, df_30m: pd.DataFrame, df_2h: pd.DataFrame) -> float:
        sw_2h = TA.get_fractal_swings(df_2h)
        sw_30m = TA.get_fractal_swings(df_30m)
        sw_15m = TA.get_fractal_swings(df_15m)
        aoi_2h = TA.detect_aoi(df_2h, sw_2h, "2H", self.aoi_memory.get(symbol, []))
        aoi_30m = TA.detect_aoi(df_30m, sw_30m, "30M", self.aoi_memory.get(symbol, []))
        aoi_15m = TA.detect_aoi(df_15m, sw_15m, "15M", self.aoi_memory.get(symbol, []))
        entropy_30m = TA.calculate_zone_entropy(df_30m, aoi_30m)
        last_p = df_15m["close"].iloc[-1]
        aoi_pts = 0
        pref_durability = self.toolbox_prefs.get_preference("structural_aoi_durability")
        pref_true_sr = self.toolbox_prefs.get_preference("structural_true_sr_weight")
        for a in aoi_30m:
            if a["min"] <= last_p <= a["max"]:
                score_component = a["durability_score"] * 0.3 * pref_durability
                score_component *= (1 - (entropy_30m / 100))
                confluent_15m = any(aoi_item["min"] <= a["price"] <= aoi_item["max"] for aoi_item in aoi_15m)
                if confluent_15m: score_component *= 1.2
                aoi_pts += score_component
        return min(aoi_pts * pref_true_sr, 100.0)

    def run_scan(self):
        # 1️⃣ VOLATILITY WINDOW RESTRICTION
        now_utc = datetime.datetime.utcnow().time()
        in_window_1 = datetime.time(7, 0) <= now_utc <= datetime.time(10, 0)
        in_window_2 = datetime.time(13, 0) <= now_utc <= datetime.time(16, 0)
        
        if not (in_window_1 or in_window_2):
            print(f"Outside volatility windows ({now_utc} UTC). Exiting.")
            return

        print(f"Starting Institutional Scan: {datetime.datetime.utcnow()} UTC")
        
        # 2️⃣ SMART API DELAY CONTROLLER
        # Total 30 assets, 3 timeframes = 90 calls.
        # Spread 90 calls across ~4 minutes (240 seconds).
        # Delay between assets = 240 / 30 = 8 seconds.
        delay_between_assets = 8 

        for symbol in Config.SYMBOLS:
            start_time = time.time()
            try:
                df_15m = self.handler.get_data(symbol, "15min")
                df_30m = self.handler.get_data(symbol, "30min")
                df_2h = self.handler.get_data(symbol, "2h")
                
                if df_15m is None or df_30m is None or df_2h is None:
                    time.sleep(delay_between_assets)
                    continue
                if len(df_2h) < 20 or len(df_30m) < 20:
                    time.sleep(delay_between_assets)
                    continue
                
                macro = self.get_macro_layer(df_2h)
                struct_30m = self.get_structural_layer(df_30m)
                exec_a = self.get_execution_layer(df_15m)

                # HTF Alignment (MANDATORY)
                htf_aligned = False
                if macro["trend"] == "BULLISH" and struct_30m["trend"] in ["BULLISH", "NEUTRAL"] and exec_a["direction"] == "BUY":
                    htf_aligned = True
                elif macro["trend"] == "BEARISH" and struct_30m["trend"] in ["BEARISH", "NEUTRAL"] and exec_a["direction"] == "SELL":
                    htf_aligned = True
                
                if not htf_aligned:
                    time.sleep(delay_between_assets)
                    continue

                # Structural AOI Requirement (MANDATORY)
                current_price = df_15m["close"].iloc[-1]
                atr_15m = TA.calculate_atr(df_15m).iloc[-1]
                aoi_2h = TA.detect_aoi(df_2h, TA.get_fractal_swings(df_2h), "2H", self.aoi_memory.get(symbol, []))
                aoi_30m = TA.detect_aoi(df_30m, TA.get_fractal_swings(df_30m), "30M", self.aoi_memory.get(symbol, []))
                order_blocks_30m = TA.detect_order_block(df_30m)
                structural_zone_present = False
                for aoi in aoi_2h + aoi_30m:
                    if aoi["min"] <= current_price <= aoi["max"]:
                        structural_zone_present = True
                        break
                if not structural_zone_present:
                    for ob in order_blocks_30m:
                        ob_min = min(ob["open"], ob["close"]) - atr_15m * 0.5
                        ob_max = max(ob["open"], ob["close"]) + atr_15m * 0.5
                        if ob_min <= current_price <= ob_max:
                            structural_zone_present = True
                            break
                if not structural_zone_present:
                    time.sleep(delay_between_assets)
                    continue

                self.aoi_memory[symbol] = aoi_2h + aoi_30m + TA.detect_aoi(df_15m, TA.get_fractal_swings(df_15m), "15M", self.aoi_memory.get(symbol, []))
                
                # 3️⃣ REDUCE STRICTNESS (Liquidity Sweep no longer mandatory)
                liquidity_levels = self._detect_all_liquidity(df_15m)
                liquidity_sweep_confirmed = False
                for level in liquidity_levels:
                    if (exec_a["direction"] == "BUY" and level["type"] in ["EQUAL_LOW", "SWING_LOW", "ASIA_LOW", "LONDON_LOW", "NY_LOW"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "BUY", macro["trend"])) or \
                       (exec_a["direction"] == "SELL" and level["type"] in ["EQUAL_HIGH", "SWING_HIGH", "ASIA_HIGH", "LONDON_HIGH", "NY_HIGH"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "SELL", macro["trend"])):
                        liquidity_sweep_confirmed = True
                        break
                
                liquidity_score = 0
                if liquidity_sweep_confirmed:
                    liquidity_score = 20 * self.toolbox_prefs.get_preference("liquidity_sweep_confirmation")
                    if TA.detect_volume_spike(df_15m): liquidity_score += 10
                
                volatility = TA.calculate_volatility(df_15m)
                if len(volatility) > 5 and volatility.iloc[-1] > volatility.iloc[-5:-1].mean():
                    liquidity_score += 5

                aoi_pts = self.get_aoi_confluence_score(symbol, df_15m, df_30m, df_2h)
                current_aois = self.aoi_memory.get(symbol, [])
                entropy_pts = TA.calculate_zone_entropy(df_15m, current_aois) * self.toolbox_prefs.get_preference("structural_aoi_entropy")
                pattern_data = TA.detect_patterns(df_15m)
                pattern_pts = (pattern_data["score"] * pattern_data["pattern_strength"]) * self.toolbox_prefs.get_preference("pattern_intelligence_toolbox")

                # 3️⃣ REDUCE STRICTNESS (OB/FVG no longer mandatory)
                order_blocks = TA.detect_order_block(df_15m)
                fvgs = TA.detect_fair_value_gap(df_15m)
                ob_fvg_confluence_score = 0
                last_price = df_15m["close"].iloc[-1]
                ob_found = any((ob["type"] == "BULLISH_OB" if exec_a["direction"] == "BUY" else ob["type"] == "BEARISH_OB") and ob["low"] <= last_price <= ob["high"] for ob in order_blocks)
                fvg_found = any((fvg["type"] == "BULLISH_FVG" if exec_a["direction"] == "BUY" else fvg["type"] == "BEARISH_FVG") and fvg["min"] <= last_price <= fvg["max"] for fvg in fvgs)
                if ob_found and fvg_found:
                    ob_fvg_confluence_score = 40 * self.toolbox_prefs.get_preference("order_block_confluence") * self.toolbox_prefs.get_preference("fvg_confluence")

                t_macro = min(macro["score"], 100)
                t_struct = min(struct_30m["score"] + aoi_pts + entropy_pts + pattern_pts + ob_fvg_confluence_score, 100)
                t_exec = min(exec_a["score"] + liquidity_score, 100)
                confidence = (t_macro * Config.WEIGHT_MACRO) + (t_struct * Config.WEIGHT_STRUCTURAL) + (t_exec * Config.WEIGHT_EXECUTION)
                direction = exec_a["direction"]
                confidence *= self._get_current_session_bias()
                if aoi_pts < 30: confidence *= self.sr_probability_gate

                # Continuation Logic (MANDATORY)
                pullback_continuation_confirmed = False
                if liquidity_sweep_confirmed and exec_a["score"] > 0:
                    pullback_continuation_confirmed = True
                elif TA.detect_micro_bos(df_15m, exec_a["direction"]):
                    pullback_continuation_confirmed = True

                if not pullback_continuation_confirmed:
                    time.sleep(delay_between_assets)
                    continue

                # Final signal confirmation (Removed ob_fvg_confluence_score > 0 requirement)
                if confidence >= Config.CONFIDENCE_THRESHOLD and direction != "NEUTRAL":
                    order_blocks_2h = TA.detect_order_block(df_2h)
                    self.execute_signal(symbol, direction, confidence, df_15m, df_30m, df_2h, liquidity_levels, order_blocks_2h)
                
                time.sleep(delay_between_assets)
            except Exception:
                traceback.print_exc()
                time.sleep(delay_between_assets)
        print("Scan complete.")

    def execute_signal(self, symbol, direction, confidence, df15, df30, df2h, all_liquidity_levels, order_blocks_2h):
        entry = df15["close"].iloc[-1]
        atr = TA.calculate_atr(df15).iloc[-1]
        if atr == 0: return
        sw15 = TA.get_fractal_swings(df15)
        if direction == "BUY":
            recent_lows = [s["price"] for s in sw15 if s["type"] == "low"]
            sl = min(recent_lows) - 0.5 * atr if recent_lows else entry - 2 * atr
            tp = entry + abs(entry - sl) * 2.0
        else:
            recent_highs = [s["price"] for s in sw15 if s["type"] == "high"]
            sl = max(recent_highs) + 0.5 * atr if recent_highs else entry + 2 * atr
            tp = entry - abs(entry - sl) * 2.0

        # Risk Logic (MANDATORY)
        rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
        if rr < 1.5: return

        # Avoid Trading Into Opposing HTF Order Block
        if direction == "BUY":
            for ob in order_blocks_2h:
                if ob["type"] == "BEARISH_OB" and (ob["low"] - entry) < 0.5 * atr and ob["low"] > entry: return
        elif direction == "SELL":
            for ob in order_blocks_2h:
                if ob["type"] == "BULLISH_OB" and (entry - ob["high"]) < 0.5 * atr and ob["high"] < entry: return

        msg = f"🚀 *SIGNAL: {symbol}*\nDir: {direction}\nConf: {confidence:.2f}%\nEntry: {entry:.5f}\nSL: {sl:.5f}\nTP: {tp:.5f}\nRR: 1:{rr:.2f}"
        self.notifier.send_telegram(msg)

        FirebaseManager.record_trade_outcome({
            "symbol": symbol, "direction": direction, "confidence": confidence,
            "entry": entry, "sl": sl, "tp": tp, "outcome": "PENDING",
            "toolbox_weights": self.toolbox_prefs.preferences.copy()
        })
        
        # 4️⃣ ADAPTIVE WEIGHT RECALCULATION (AFTER 20 TRADES)
        self.trade_count_since_last_recalc += 1
        if self.trade_count_since_last_recalc >= 20:
            self.daily_learning_audit_cycle()
            self.trade_count_since_last_recalc = 0

    def daily_learning_audit_cycle(self):
        outcomes = FirebaseManager.get_trade_outcomes(limit=100)
        if not outcomes: return
        comp_adjustments = {comp: 0.0 for comp in self.toolbox_prefs.preferences.keys()}
        comp_stats = {comp: {"wins": 0, "total": 0} for comp in self.toolbox_prefs.preferences.keys()}
        for t in outcomes:
            if t.get("outcome") in ["WIN", "LOSS"]:
                is_win = t["outcome"] == "WIN"
                weights_at_trade = t.get("toolbox_weights", self.toolbox_prefs.preferences)
                for comp, weight in weights_at_trade.items():
                    comp_adjustments[comp] += weight if is_win else -weight
                    comp_stats[comp]["total"] += 1
                    if is_win: comp_stats[comp]["wins"] += 1
        for comp, adj in comp_adjustments.items():
            if comp_stats[comp]["total"] > 0:
                self.toolbox_prefs.adjust_preference(comp, adj / comp_stats[comp]["total"])
                self.toolbox_prefs.update_state(comp, comp_stats[comp]["wins"] / comp_stats[comp]["total"])
        total_wins = sum(s["wins"] for s in comp_stats.values())
        total_trades = sum(s["total"] for s in comp_stats.values())
        total_wr = total_wins / total_trades if total_trades > 0 else 0.5
        if total_wr > 0.65: self.sr_probability_gate = 1.0
        elif 0.60 <= total_wr <= 0.65: self.sr_probability_gate = 0.8
        elif 0.45 <= total_wr < 0.60: self.sr_probability_gate = 0.5
        else: self.sr_probability_gate = 0.2

if __name__ == "__main__":
    bot = TradingBot()
    # Bot runs every 15 minutes via external cron or loop
    bot.run_scan()
