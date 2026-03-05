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
    TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
    TWELVEDATA_API_KEY_1 = os.getenv("TWELVEDATA_API_KEY_1")
    TWELVEDATA_API_KEY_2 = os.getenv("TWELVEDATA_API_KEY_2")
    TWELVEDATA_API_KEY_3 = os.getenv("TWELVEDATA_API_KEY_3")
    
    FIREBASE_SERVICE_ACCOUNT_KEY = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY")
    
    # TwelveData Limits per Key
    MINUTE_CALL_LIMIT = 7
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
            Config.TWELVEDATA_API_KEY,
            Config.TWELVEDATA_API_KEY_1,
            Config.TWELVEDATA_API_KEY_2,
            Config.TWELVEDATA_API_KEY_3
        ]
        # Filter out None keys
        self.keys = [k for k in self.keys if k]
        
        # Fix 2: Hard Fail if No TwelveData API Keys Exist
        if not self.keys:
            raise ValueError("No TwelveData API keys configured.")
            
        self.current_index = 0
        
        # Tracking usage per key
        self.usage = {
            key: {
                "minute_calls": 0,
                "daily_calls": 0,
                "minute_start": time.time(),
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
            
            # Reset windows if needed
            if now - stats["minute_start"] >= 60:
                stats["minute_calls"] = 0
                stats["minute_start"] = now
            if now - stats["daily_start"] >= 86400:
                stats["daily_calls"] = 0
                stats["daily_start"] = now
                
            # Check limits
            if stats["minute_calls"] < Config.MINUTE_CALL_LIMIT and stats["daily_calls"] < Config.DAILY_CALL_LIMIT:
                return key
            
            # Rotate to next key
            self.current_index = (self.current_index + 1) % len(self.keys)
            
            # If we've checked all keys and none are available
            if self.current_index == start_index:
                # Wait for the oldest minute window to reset
                wait_times = []
                for k in self.keys:
                    s = self.usage[k]
                    if s["daily_calls"] < Config.DAILY_CALL_LIMIT:
                        wait_times.append(60 - (now - s["minute_start"]))
                
                if wait_times:
                    sleep_time = max(0, min(wait_times)) + 1
                    print(f"All keys rate-limited. Sleeping for {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    continue # Re-check after sleep
                else:
                    print("All keys reached daily limit.")
                    return None

    def register_call(self, key: str):
        if key in self.usage:
            self.usage[key]["minute_calls"] += 1
            self.usage[key]["daily_calls"] += 1

class RequestHandler:
    """
    Structured request handling system for distributed and controlled API usage.
    Implements User-Agent rotation and a structured proxy-ready routing layer 
    to reduce detection risk and ensure professional API behavior.
    """
    def __init__(self, key_manager: APIKeyManager):
        self.key_manager = key_manager
        self.session = requests.Session()
        
        # IP/Request Separation: List of professional User-Agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/118.0"
        ]
        
        # IP/Request Separation: Proxy routing layer structure
        # If proxies are available in environment, they will be used to separate requests by IP
        self.proxies = os.getenv("PROXY_LIST", "").split(",")
        self.proxies = [p.strip() for p in self.proxies if p.strip()]

    def _get_request_config(self) -> Dict:
        """Generates a randomized request configuration for IP/Request separation."""
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

    def get_data(self, symbol: str, interval: str, outputsize: int = 500) -> Optional[pd.DataFrame]:
        max_retries = len(self.key_manager.keys)
        for attempt in range(max_retries):
            api_key = self.key_manager.get_active_key()
            if not api_key:
                return None
                
            try:
                url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={api_key}"
                
                # Apply IP/Request separation configuration
                config = self._get_request_config()
                
                response = self.session.get(url, **config)
                self.key_manager.register_call(api_key)
                
                data = response.json()
                
                # Check for rate limit error (HTTP 429 or specific message)
                if response.status_code == 429 or (data.get("status") == "error" and "rate limit" in data.get("message", "").lower()):
                    print(f"Rate limit reached for key {api_key}. Rotating key (Attempt {attempt + 1}/{max_retries})...")
                    # Rotate to next key for the next iteration
                    self.key_manager.current_index = (self.key_manager.current_index + 1) % len(self.key_manager.keys)
                    continue
                    
                if "values" not in data:
                    # Do NOT retry if symbol is invalid or other non-rate-limit errors
                    print(f"Error fetching {interval} data for {symbol}: {data.get('message', 'Unknown error')}")
                    return None
                    
                df = pd.DataFrame(data["values"])
                
                # Fix Volume Column Error: Add volume if missing
                if 'volume' not in df.columns:
                    df['volume'] = 0
                    
                # Ensure all required columns are present before conversion
                cols_to_convert = ["open", "high", "low", "close", "volume"]
                df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric)
                
                df["datetime"] = pd.to_datetime(df["datetime"])
                return df.set_index("datetime").sort_index()
            except Exception as e:
                print(f"Request error for {symbol} ({interval}): {e}")
                return None
        return None

class ToolboxPreference:
    def __init__(self):
        self.preferences = Config.TOOLBOX_PREFERENCES.copy()
        # component_state ∈ {ACTIVE, SUSPENDED}
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
        # Fix 4: Encoding Corruption Fix
        print(f"Adjusted preference for {component_name}: {current_pref:.4f} -> {self.preferences[component_name]:.4f} (Delta: {delta:.4f})")

    def update_state(self, component_name: str, win_ratio: float):
        current_state = self.component_states.get(component_name, "ACTIVE")
        if current_state == "ACTIVE":
            if win_ratio < 0.38:
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
                # Fix 1: Firebase Re-Initialization Protection
                cred = credentials.Certificate("firebase-service-account.json")
                if not firebase_admin._apps:
                    firebase_admin.initialize_app(cred)
                cls._db = firestore.client()
                print("Firebase initialized successfully.")
            except Exception as e:
                print(f"Firebase initialization error: {e}")

    @classmethod
    def log_trade(cls, trade_data: Dict):
        if cls._db:
            try:
                cls._db.collection("trades").add(trade_data)
            except Exception as e:
                print(f"Error logging trade to Firebase: {e}")

    @classmethod
    def get_component_performance(cls, component_name: str) -> List[Dict]:
        if not cls._db: return []
        try:
            results = cls._db.collection("component_performance").where("name", "==", component_name).order_by("timestamp", direction=firestore.Query.DESCENDING).limit(50).stream()
            return [doc.to_dict() for doc in results]
        except Exception as e:
            print(f"Error fetching performance: {e}")
            return []

    @classmethod
    def record_trade_outcome(cls, outcome_data: Dict):
        if cls._db:
            try:
                outcome_data["timestamp"] = datetime.datetime.utcnow()
                cls._db.collection("trade_outcomes").add(outcome_data)
            except Exception as e:
                print(f"Error recording trade outcome: {e}")

    @staticmethod
    def get_trade_outcomes(days_back=14):
        db = FirebaseManager._db
        if not db:
            return []
        try:
            cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days_back)
            docs = db.collection("trade_outcomes").where(
                "timestamp", ">=", cutoff
            ).stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            print(f"Error fetching trade outcomes: {e}")
            return []

class NotificationManager:
    @staticmethod
    def send_telegram(message: str):
        if not Config.TELEGRAM_TOKEN or not Config.TELEGRAM_CHAT_ID:
            print(f"Telegram not configured. Message: {message}")
            return
        try:
            url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, data={"chat_id": Config.TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=10)
        # Fix 3: Telegram Silent Failure Removal
        except Exception as e:
            print(f"Telegram error: {e}")

# ============================================================
# TECHNICAL ANALYSIS ENGINE
# ============================================================

class TA:
    @staticmethod
    def _get_session_times(df: pd.DataFrame) -> Dict[str, Tuple[datetime.time, datetime.time]]:
        # Define major forex session times in UTC
        # Asia Session: Tokyo open to London open (approx)
        # London Session: London open to New York open (approx)
        # New York Session: New York open to close (approx)
        return {
            "ASIA": (datetime.time(0, 0), datetime.time(8, 0)),
            "LONDON": (datetime.time(8, 0), datetime.time(17, 0)),
            "NY": (datetime.time(13, 0), datetime.time(22, 0))
        }

    @staticmethod
    def detect_session_liquidity(df: pd.DataFrame) -> List[Dict]:
        liquidity_levels = []
        sessions = TA._get_session_times(df)
        
        for session_name, (start_time, end_time) in sessions.items():
            # Filter DataFrame for the current session
            session_df = df.between_time(start_time, end_time)
            
            if not session_df.empty:
                session_high = session_df["high"].max()
                session_low = session_df["low"].min()
                
                liquidity_levels.append({"type": f"{session_name}_HIGH", "price": session_high, "timeframe": "SESSION"})
                liquidity_levels.append({"type": f"{session_name}_LOW", "price": session_low, "timeframe": "SESSION"})
                
        return liquidity_levels

    @staticmethod
    def detect_equal_highs_lows(df: pd.DataFrame, threshold_atr_multiple: float = 0.1) -> List[Dict]:
        liquidity_levels = []
        atr = TA.calculate_atr(df).iloc[-1]
        if atr == 0: return liquidity_levels

        # Look for equal highs
        for i in range(1, len(df) - 1):
            for j in range(i + 1, len(df)):
                if abs(df["high"].iloc[i] - df["high"].iloc[j]) <= atr * threshold_atr_multiple:
                    # Ensure there's a dip between the two highs to confirm it's not just a flat top
                    if df["low"].iloc[i+1:j].min() < min(df["high"].iloc[i], df["high"].iloc[j]):
                        liquidity_levels.append({"type": "EQUAL_HIGH", "price": df["high"].iloc[i], "timeframe": "CURRENT"})
                        break # Found a cluster, move to next potential high
        
        # Look for equal lows
        for i in range(1, len(df) - 1):
            for j in range(i + 1, len(df)):
                if abs(df["low"].iloc[i] - df["low"].iloc[j]) <= atr * threshold_atr_multiple:
                    # Ensure there's a peak between the two lows to confirm it's not just a flat bottom
                    if df["high"].iloc[i+1:j].max() > max(df["low"].iloc[i], df["low"].iloc[j]):
                        liquidity_levels.append({"type": "EQUAL_LOW", "price": df["low"].iloc[i], "timeframe": "CURRENT"})
                        break # Found a cluster, move to next potential low
        return liquidity_levels

    @staticmethod
    def detect_swing_liquidity(df: pd.DataFrame, window: int = 5) -> List[Dict]:
        liquidity_levels = []
        swings = TA.get_fractal_swings(df, window=window)
        for swing in swings:
            if swing["type"] == "high":
                liquidity_levels.append({"type": "SWING_HIGH", "price": swing["price"], "timeframe": "CURRENT"})
            elif swing["type"] == "low":
                liquidity_levels.append({"type": "SWING_LOW", "price": swing["price"], "timeframe": "CURRENT"})
        return liquidity_levels

    @staticmethod
    def confirm_liquidity_sweep(df: pd.DataFrame, liquidity_level: float, direction: str, higher_timeframe_bias: str, atr_multiple: float = 0.5) -> bool:
        """
        Confirms a liquidity sweep: price moves beyond level, then closes back inside.
        Direction: 'BUY' for bullish sweep (sweeping a low), 'SELL' for bearish sweep (sweeping a high).
        """
        if len(df) < 2: return False
        
        last_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]
        atr = TA.calculate_atr(df).iloc[-1]
        if atr == 0: return False

        if direction == "BUY": # Looking for sweep of a low
            # Price must have gone below the liquidity level
            if last_candle["low"] < liquidity_level:
                # And closed back above it, with a significant wick below
                if last_candle["close"] > liquidity_level and (liquidity_level - last_candle["low"]) > atr * atr_multiple:
                    # Increase confidence when sweep aligns with higher timeframe bias
                    if higher_timeframe_bias == "BULLISH":
                        return True
        elif direction == "SELL": # Looking for sweep of a high
            # Price must have gone above the liquidity level
            if last_candle["high"] > liquidity_level:
                # And closed back below it, with a significant wick above
                if last_candle["close"] < liquidity_level and (last_candle["high"] - liquidity_level) > atr * atr_multiple:
                    # Increase confidence when sweep aligns with higher timeframe bias
                    if higher_timeframe_bias == "BEARISH":
                        return True

        return False

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        if len(df) < period: return pd.Series([0.0] * len(df), index=df.index)
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(period).mean().fillna(0.0)

    @staticmethod
    def get_fractal_swings(df: pd.DataFrame, window: int = 2) -> List[Dict]:
        swings = []
        if len(df) < window * 2 + 1: return swings
        highs, lows, times = df["high"].values, df["low"].values, df.index
        for i in range(window, len(df) - window):
            if all(highs[i] > highs[i-j] for j in range(1, window+1)) and all(highs[i] > highs[i+j] for j in range(1, window+1)):
                swings.append({"type": "high", "price": highs[i], "index": i, "time": times[i]})
            if all(lows[i] < lows[i-j] for j in range(1, window+1)) and all(lows[i] < lows[i+j] for j in range(1, window+1)):
                swings.append({"type": "low", "price": lows[i], "index": i, "time": times[i]})
        return swings

    @staticmethod
    def get_manifold_swings(df: pd.DataFrame, window: int = 3) -> List[Dict]:
        """
        Manifold swing detection for structural topology consistency.
        """
        return TA.get_fractal_swings(df, window=window)

    @staticmethod
    def detect_aoi(df: pd.DataFrame, swings: List[Dict], timeframe: str, existing_aois: List[Dict] = None) -> List[Dict]:
        aois = []
        if not swings: return aois
        
        decayed_existing_aois = []
        if existing_aois:
            for aoi in existing_aois:
                decayed_aoi = aoi.copy()
                decayed_aoi["continuity_score"] = aoi.get("continuity_score", 1.0) * 0.9
                decayed_existing_aois.append(decayed_aoi)

        prices = [s["price"] for s in swings]
        atr = TA.calculate_atr(df).iloc[-1]
        if atr == 0: return aois
        
        for p in prices:
            touches = 0
            rejections = 0
            for i in range(len(df)):
                low, high, close, open_p = df["low"].iloc[i], df["high"].iloc[i], df["close"].iloc[i], df["open"].iloc[i]
                if low <= p <= high:
                    touches += 1
                    if not (min(open_p, close) <= p <= max(open_p, close)):
                        rejections += 1
            
            if touches >= 3 and rejections >= 2:
                rejection_strength = 0.0
                for i in range(len(df)):
                    low, high, open_p, close_p = df["low"].iloc[i], df["high"].iloc[i], df["open"].iloc[i], df["close"].iloc[i]
                    if low <= p <= high:
                        body_size = abs(close_p - open_p)
                        if body_size > 0:
                            if close_p > open_p: # Bullish candle
                                if p > close_p: # Rejection from above
                                    rejection_strength += (high - p) / body_size # Upper wick relative to body
                            else: # Bearish candle
                                if p < close_p: # Rejection from below
                                    rejection_strength += (p - low) / body_size # Lower wick relative to body
                
                # Higher weight for multi-touch levels with strong rejection bodies
                durability_score = min(touches * 10, 100) + min(rejection_strength * 5, 50) # Max 150 score
                
                aois.append({
                    "price": p, "min": p - 0.2 * atr, "max": p + 0.2 * atr,
                    "durability_score": durability_score, "timeframe": timeframe,
                })
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
        # Using standard deviation of log returns as a measure of volatility
        returns = np.log(df['close'] / df['close'].shift(1))
        return returns.rolling(period).std().fillna(0.0)

    @staticmethod
    def detect_volume_spike(df: pd.DataFrame, period: int = 20, threshold: float = 1.5) -> bool:
        if 'volume' not in df.columns or len(df) < period:
            return False
        
        current_volume = df['volume'].iloc[-1]
        average_volume = df['volume'].iloc[-period:-1].mean()
        std_volume = df['volume'].iloc[-period:-1].std()
        
        # A volume spike is when current volume is significantly above the average
        # and also above a certain standard deviation from the mean.
        if std_volume == 0: # Avoid division by zero
            return current_volume > average_volume * threshold
        
        return current_volume > (average_volume + threshold * std_volume)

    @staticmethod
    def detect_order_block(df: pd.DataFrame, lookback: int = 10) -> List[Dict]:
        order_blocks = []
        for i in range(lookback, len(df)):
            current = df.iloc[i]
            prev = df.iloc[i-1]
            
            # Bullish Order Block: Last down candle before an impulse up move
            if prev["close"] < prev["open"] and current["close"] > current["open"] and current["low"] > prev["high"]:
                # Check for impulse move (e.g., strong move up after the down candle)
                if (current["close"] - prev["close"]) > TA.calculate_atr(df.iloc[i-lookback:i+1]).iloc[-1] * 0.5:
                    order_blocks.append({"type": "BULLISH_OB", "open": prev["open"], "close": prev["close"], "high": prev["high"], "low": prev["low"], "time": prev.name})
            
            # Bearish Order Block: Last up candle before an impulse down move
            elif prev["close"] > prev["open"] and current["close"] < current["open"] and current["high"] < prev["low"]:
                # Check for impulse move (e.g., strong move down after the up candle)
                if (prev["close"] - current["close"]) > TA.calculate_atr(df.iloc[i-lookback:i+1]).iloc[-1] * 0.5:
                    order_blocks.append({"type": "BEARISH_OB", "open": prev["open"], "close": prev["close"], "high": prev["high"], "low": prev["low"], "time": prev.name})
        return order_blocks

    @staticmethod
    def detect_micro_bos(df: pd.DataFrame, direction: str, lookback: int = 10) -> bool:
        # Look for a recent swing high/low and then a break of that swing
        swings = TA.get_fractal_swings(df.tail(lookback))
        if len(swings) < 2: return False

        last_swing = swings[-1]
        second_last_swing = swings[-2]

        if direction == "BUY": # Looking for bullish BOS
            # Price broke above a recent swing high after a pullback
            if last_swing["type"] == "high" and df["close"].iloc[-1] > last_swing["price"] and second_last_swing["type"] == "low":
                return True
        elif direction == "SELL": # Looking for bearish BOS
            # Price broke below a recent swing low after a pullback
            if last_swing["type"] == "low" and df["close"].iloc[-1] < last_swing["price"] and second_last_swing["type"] == "high":
                return True
        return False

    @staticmethod
    def detect_fair_value_gap(df: pd.DataFrame, lookback: int = 20) -> List[Dict]:
        fvgs = []
        for i in range(2, len(df)):
            candle1 = df.iloc[i-2]
            candle2 = df.iloc[i-1]
            candle3 = df.iloc[i]
            
            # Bullish FVG: Low of candle3 > High of candle1
            if candle3["low"] > candle1["high"]:
                fvg_min = candle1["high"]
                fvg_max = candle3["low"]
                fvgs.append({"type": "BULLISH_FVG", "min": fvg_min, "max": fvg_max, "time": candle2.name})
            
            # Bearish FVG: High of candle3 < Low of candle1
            elif candle3["high"] < candle1["low"]:
                fvg_min = candle3["high"]
                fvg_max = candle1["low"]
                fvgs.append({"type": "BEARISH_FVG", "min": fvg_min, "max": fvg_max, "time": candle2.name})
        return fvgs

    @staticmethod
    def find_nearest_liquidity(df: pd.DataFrame, current_price: float, direction: str, all_liquidity_levels: List[Dict], atr: float) -> Optional[Dict]:
        potential_targets = []
        for level in all_liquidity_levels:
            if direction == "BUY" and level["type"].endswith("HIGH") and level["price"] > current_price:
                potential_targets.append(level)
            elif direction == "SELL" and level["type"].endswith("LOW") and level["price"] < current_price:
                potential_targets.append(level)
        
        if not potential_targets: return None

        # Sort by distance from current price
        potential_targets.sort(key=lambda x: abs(x["price"] - current_price))

        # Prefer equal highs/lows or swing liquidity pools
        for level in potential_targets:
            if level["type"].startswith("EQUAL") or level["type"].startswith("SWING"):
                return level
        
        # Otherwise, return the nearest one
        return potential_targets[0]

    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> Dict:
        last_5 = df.tail(5)
        score = 0
        strength = 0.5
        
        # Simple Pin Bar detection
        body = abs(last_5["close"].iloc[-1] - last_5["open"].iloc[-1])
        wick_top = last_5["high"].iloc[-1] - max(last_5["close"].iloc[-1], last_5["open"].iloc[-1])
        wick_bottom = min(last_5["close"].iloc[-1], last_5["open"].iloc[-1]) - last_5["low"].iloc[-1]
        
        if wick_bottom > body * 2:
            score = 70 # Bullish Pin
            strength = min(wick_bottom / (body + 0.0001) * 0.2, 1.0)
        elif wick_top > body * 2:
            score = 70 # Bearish Pin
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

    def _get_current_session_bias(self) -> float:
        now_utc = datetime.datetime.utcnow().time()
        sessions = TA._get_session_times(pd.DataFrame()) # Pass empty df, only need times
        
        # Prioritize London and New York sessions
        london_start, london_end = sessions["LONDON"]
        ny_start, ny_end = sessions["NY"]
        
        if (london_start <= now_utc < london_end) or (ny_start <= now_utc < ny_end):
            return 1.0 # High priority session
        
        # Reduce scoring during low-liquidity session unless strong structural breakout exists
        # For now, a simple binary filter. Can be enhanced with structural breakout detection later.
        return 0.5 # Lower priority session

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
        score = 0
        current_trend = "NEUTRAL"
        if len(swings) >= 3:
            s3, s2, s1 = swings[-3], swings[-2], swings[-1]
            if s2["type"] == "low" and s1["type"] == "high" and s1["price"] > s2["price"]:
                current_trend = "BULLISH"
            elif s2["type"] == "high" and s1["type"] == "low" and s1["price"] < s2["price"]:
                current_trend = "BEARISH"
            
            if current_trend == "BULLISH":
                if s1["type"] == "high" and s3["type"] == "high" and s1["price"] > s3["price"]:
                    score += 50
                if s1["type"] == "low" and s1["price"] < s2["price"]:
                    score += 70
            elif current_trend == "BEARISH":
                if s1["type"] == "low" and s3["type"] == "low" and s1["price"] < s3["price"]:
                    score += 50
                if s1["type"] == "high" and s1["price"] > s2["price"]:
                    score += 70

        pref_bos = self.toolbox_prefs.get_preference("structural_bos_choch")
        return {"score": score * pref_bos, "trend": current_trend}

    def get_execution_layer(self, df: pd.DataFrame) -> Dict:
        last_3 = df.tail(3)
        score = 0
        direction = "NEUTRAL"
        body_size = abs(last_3["close"] - last_3["open"])
        if body_size.iloc[-1] > body_size.iloc[-2] * 1.5:
            score += 60
            direction = "BUY" if last_3["close"].iloc[-1] > last_3["open"].iloc[-1] else "SELL"
        pref_exec = self.toolbox_prefs.get_preference("execution_impulse_reaction")
        return {"score": score * pref_exec, "direction": direction}

    def get_aoi_confluence_score(self, symbol, df_15m, df_30m, df_2h) -> float:
        existing_aois = self.aoi_memory.get(symbol, [])
        aoi_2h = TA.detect_aoi(df_2h, TA.get_fractal_swings(df_2h), "2H", existing_aois)
        aoi_30m = TA.detect_aoi(df_30m, TA.get_fractal_swings(df_30m), "30M", existing_aois)
        aoi_15m = TA.detect_aoi(df_15m, TA.get_fractal_swings(df_15m), "15M", existing_aois)
        self.aoi_memory[symbol] = aoi_2h + aoi_30m + aoi_15m
        
        aoi_pts, last_p = 0, df_15m["close"].iloc[-1]
        pref_durability = self.toolbox_prefs.get_preference("structural_aoi_durability")
        pref_true_sr = self.toolbox_prefs.get_preference("structural_true_sr_weight")

        # Calculate entropy for each timeframe's AOIs
        entropy_2h = TA.calculate_zone_entropy(df_2h, aoi_2h)
        entropy_30m = TA.calculate_zone_entropy(df_30m, aoi_30m)
        entropy_15m = TA.calculate_zone_entropy(df_15m, aoi_15m)

        for a in aoi_2h:
            if a["min"] <= last_p <= a["max"]:
                score_component = a["durability_score"] * 0.5 * pref_durability
                # Penalize based on entropy: higher entropy means weaker zone
                score_component *= (1 - (entropy_2h / 100)) # Scale entropy to 0-1
                
                # Check for confluence with lower timeframes
                confluent_15m = any(aoi_item["min"] <= a["price"] <= aoi_item["max"] for aoi_item in aoi_15m)
                confluent_30m = any(aoi_item["min"] <= a["price"] <= aoi_item["max"] for aoi_item in aoi_30m)
                
                if confluent_15m and confluent_30m:
                    score_component *= 1.5 # Significant boost for multi-timeframe confluence
                elif confluent_15m or confluent_30m:
                    score_component *= 1.2 # Moderate boost

                aoi_pts += score_component
                break
        for a in aoi_30m:
            if a["min"] <= last_p <= a["max"]:
                score_component = a["durability_score"] * 0.3 * pref_durability
                score_component *= (1 - (entropy_30m / 100))
                
                # Check for confluence with lower timeframes (only 15m remaining)
                confluent_15m = any(aoi_item["min"] <= a["price"] <= aoi_item["max"] for aoi_item in aoi_15m)
                if confluent_15m:
                    score_component *= 1.2 # Moderate boost

                aoi_pts += score_component
                break
        for a in aoi_15m:
            if a["min"] <= last_p <= a["max"]:
                score_component = a["durability_score"] * 0.2 * pref_durability
                score_component *= (1 - (entropy_15m / 100))
                aoi_pts += score_component
                break
        
        # Apply overall true SR weight
        return min(aoi_pts * pref_true_sr, 100.0)

    def run_scan(self):
        print(f"Starting Institutional Scan: {datetime.datetime.utcnow()} UTC")
        for symbol in Config.SYMBOLS:
            start_time = time.time()
            try:
                # Direct fetching for each timeframe as requested
                df_15m = self.handler.get_data(symbol, "15min")
                df_30m = self.handler.get_data(symbol, "30min")
                df_2h = self.handler.get_data(symbol, "2h")
                
                if df_15m is None or df_30m is None or df_2h is None:
                    # Even if we skip, we still respect pacing if we made API calls
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 60 - elapsed)
                    if sleep_time > 0:
                        print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                        time.sleep(sleep_time)
                    continue
                if len(df_2h) < 20 or len(df_30m) < 20:
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 60 - elapsed)
                    if sleep_time > 0:
                        print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                        time.sleep(sleep_time)
                    continue
                
                macro = self.get_macro_layer(df_2h)
                struct_30m = self.get_structural_layer(df_30m)
                exec_a = self.get_execution_layer(df_15m)

                # 1. Higher Timeframe Bias Enforcement (MANDATORY)
                # 2H defines primary bias (macro["trend"])
                # 30M structure must not oppose 2H direction (struct_30m["trend"])
                # 15M execution must confirm same direction (exec_a["direction"])
                htf_aligned = False
                if macro["trend"] == "BULLISH" and struct_30m["trend"] in ["BULLISH", "NEUTRAL"] and exec_a["direction"] == "BUY":
                    htf_aligned = True
                elif macro["trend"] == "BEARISH" and struct_30m["trend"] in ["BEARISH", "NEUTRAL"] and exec_a["direction"] == "SELL":
                    htf_aligned = True
                
                if not htf_aligned:
                    # print(f"Skipping {symbol}: HTF bias not aligned. Macro: {macro["trend"]}, 30M Struct: {struct_30m["trend"]}, 15M Exec: {exec_a["direction"]}")
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 60 - elapsed)
                    if sleep_time > 0:
                        print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                        time.sleep(sleep_time)
                    continue

                # 2. Structural AOI Requirement (MANDATORY)
                current_price = df_15m["close"].iloc[-1]
                atr_15m = TA.calculate_atr(df_15m).iloc[-1]
                
                aoi_2h = TA.detect_aoi(df_2h, TA.get_fractal_swings(df_2h), "2H", self.aoi_memory.get(symbol, []))
                aoi_30m = TA.detect_aoi(df_30m, TA.get_fractal_swings(df_30m), "30M", self.aoi_memory.get(symbol, []))
                order_blocks_30m = TA.detect_order_block(df_30m)

                structural_zone_present = False
                # Check 30M or 2H AOI
                for aoi in aoi_2h + aoi_30m:
                    if aoi["min"] <= current_price <= aoi["max"]:
                        structural_zone_present = True
                        break
                
                # Check 30M Order Block
                if not structural_zone_present:
                    for ob in order_blocks_30m:
                        # Define ATR-adaptive zone boundaries for OB
                        ob_min = min(ob["open"], ob["close"]) - atr_15m * 0.5
                        ob_max = max(ob["open"], ob["close"]) + atr_15m * 0.5
                        if ob_min <= current_price <= ob_max:
                            structural_zone_present = True
                            break
                
                if not structural_zone_present:
                    # print(f"Skipping {symbol}: Price not interacting with valid structural zone.")
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 60 - elapsed)
                    if sleep_time > 0:
                        print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                        time.sleep(sleep_time)
                    continue

                # Update AOI memory after checks
                self.aoi_memory[symbol] = aoi_2h + aoi_30m + TA.detect_aoi(df_15m, TA.get_fractal_swings(df_15m), "15M", self.aoi_memory.get(symbol, []))
                
                # Liquidity Mapping and Sweep Detection
                liquidity_levels = self._detect_all_liquidity(df_15m)
                liquidity_sweep_confirmed = False
                for level in liquidity_levels:
                    if (exec_a["direction"] == "BUY" and level["type"] in ["EQUAL_LOW", "SWING_LOW", "ASIA_LOW", "LONDON_LOW", "NY_LOW"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "BUY", macro["trend"])) or \
                       (exec_a["direction"] == "SELL" and level["type"] in ["EQUAL_HIGH", "SWING_HIGH", "ASIA_HIGH", "LONDON_HIGH", "NY_HIGH"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "SELL", macro["trend"])):
                        liquidity_sweep_confirmed = True
                        break
                
                liquidity_score = 0
                volume_spike_confirmed = False
                volatility_expansion_confirmed = False

                if liquidity_sweep_confirmed:
                    liquidity_score = 20 * self.toolbox_prefs.get_preference("liquidity_sweep_confirmation")
                    
                    # Volume spike confirmation during liquidity sweep
                    if TA.detect_volume_spike(df_15m):
                        volume_spike_confirmed = True
                        liquidity_score += 10 # Boost for volume confirmation

                # Volatility expansion after contraction before execution entries
                volatility = TA.calculate_volatility(df_15m)
                if len(volatility) > 5 and volatility.iloc[-1] > volatility.iloc[-5:-1].mean():
                    volatility_expansion_confirmed = True
                    liquidity_score += 5 # Boost for volatility expansion

                aoi_pts = self.get_aoi_confluence_score(symbol, df_15m, df_30m, df_2h)
                
                current_aois = self.aoi_memory.get(symbol, [])
                entropy_score = TA.calculate_zone_entropy(df_15m, current_aois)
                entropy_pts = entropy_score * self.toolbox_prefs.get_preference("structural_aoi_entropy")
                
                pattern_data = TA.detect_patterns(df_15m)
                pattern_weight = self.toolbox_prefs.get_preference("pattern_intelligence_toolbox")
                pattern_pts = (pattern_data["score"] * pattern_data["pattern_strength"]) * pattern_weight

                # Order Block and FVG Detection
                order_blocks = TA.detect_order_block(df_15m)
                fvgs = TA.detect_fair_value_gap(df_15m)

                ob_fvg_confluence_score = 0
                if order_blocks and fvgs and current_aois:
                    # Require confluence between AOI + Order Block + FVG for stronger scoring
                    last_price = df_15m["close"].iloc[-1]
                    
                    ob_found = False
                    for ob in order_blocks:
                        if (ob["type"] == "BULLISH_OB" and ob["low"] <= last_price <= ob["high"]) or \
                           (ob["type"] == "BEARISH_OB" and ob["low"] <= last_price <= ob["high"]):
                            ob_found = True
                            break
                    
                    fvg_found = False
                    for fvg in fvgs:
                        if (fvg["type"] == "BULLISH_FVG" and fvg["min"] <= last_price <= fvg["max"]) or \
                           (fvg["type"] == "BEARISH_FVG" and fvg["min"] <= last_price <= fvg["max"]):
                            fvg_found = True
                            break
                    
                    aoi_found = False
                    for aoi in current_aois:
                        if aoi["min"] <= last_price <= aoi["max"]:
                            aoi_found = True
                            break

                    if ob_found and fvg_found and aoi_found:
                        ob_fvg_confluence_score = 40 * self.toolbox_prefs.get_preference("order_block_confluence") * self.toolbox_prefs.get_preference("fvg_confluence")

                t_macro = min(macro["score"], 100)
                t_struct = min(struct_30m["score"] + aoi_pts + entropy_pts + pattern_pts + ob_fvg_confluence_score, 100)
                t_exec = min(exec_a["score"] + liquidity_score, 100) # Add liquidity score to execution layer
                
                confidence = (t_macro * Config.WEIGHT_MACRO) + (t_struct * Config.WEIGHT_STRUCTURAL) + (t_exec * Config.WEIGHT_EXECUTION)
                direction = exec_a["direction"]
                
                # Apply session bias filter
                session_bias = self._get_current_session_bias()
                confidence *= session_bias

                if aoi_pts < 30:
                    confidence *= self.sr_probability_gate

                # 3. Pullback -> Continuation Logic (MANDATORY)
                pullback_continuation_confirmed = False
                if liquidity_sweep_confirmed and exec_a["score"] > 0: # Impulse candle is part of execution layer score
                    pullback_continuation_confirmed = True
                elif TA.detect_micro_bos(df_15m, exec_a["direction"]):
                    pullback_continuation_confirmed = True

                if not pullback_continuation_confirmed:
                    # print(f"Skipping {symbol}: No pullback continuation confirmed.")
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 60 - elapsed)
                    if sleep_time > 0:
                        print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                        time.sleep(sleep_time)
                    continue

                # Final signal confirmation
                if confidence >= Config.CONFIDENCE_THRESHOLD and direction != "NEUTRAL" and ob_fvg_confluence_score > 0:
                    # Get 2H order blocks for the 'Avoid Trading Into Opposing HTF Order Block' filter
                    order_blocks_2h = TA.detect_order_block(df_2h)
                    self.execute_signal(symbol, direction, confidence, df_15m, df_30m, df_2h, liquidity_levels, order_blocks_2h)
                
                # Pacing control: ensure at least 60s between assets
                elapsed = time.time() - start_time
                sleep_time = max(0, 60 - elapsed)
                if sleep_time > 0:
                    print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                    time.sleep(sleep_time)
            except Exception:
                traceback.print_exc()
                # Ensure pacing even on error
                elapsed = time.time() - start_time
                sleep_time = max(0, 60 - elapsed)
                if sleep_time > 0:
                    print(f"Pacing control: sleeping {sleep_time:.2f}s before next asset...")
                    time.sleep(sleep_time)
        print("Scan complete.")

    def execute_signal(self, symbol, direction, confidence, df15, df30, df2h, all_liquidity_levels, order_blocks_2h):
        entry = df15["close"].iloc[-1]
        atr = TA.calculate_atr(df15).iloc[-1]
        if atr == 0: return
        
        sw15 = TA.get_fractal_swings(df15)
        sw30 = TA.get_fractal_swings(df30)
        
        # Calculate SL using existing structural logic
        if direction == "BUY":
            v_lows = [s["price"] for s in reversed(sw15) if s["type"] == "low" and s["price"] < entry]
            sl_raw_dist = (entry - v_lows[0]) / atr if v_lows else 1.2
            sl_atr_dist = max(0.8, min(1.5, sl_raw_dist))
            sl = entry - sl_atr_dist * atr
        else:
            v_highs = [s["price"] for s in reversed(sw15) if s["type"] == "high" and s["price"] > entry]
            sl_raw_dist = (v_highs[0] - entry) / atr if v_highs else 1.2
            sl_atr_dist = max(0.8, min(1.5, sl_raw_dist))
            sl = entry + sl_atr_dist * atr

        # Calculate TP using structural swing logic (existing) and enhance with liquidity pools
        tp = None
        nearest_liquidity = TA.find_nearest_liquidity(df15, entry, direction, all_liquidity_levels, atr)

        if direction == "BUY":
            o_highs = [s["price"] for s in sw30 if s["type"] == "high" and s["price"] > entry]
            if nearest_liquidity and nearest_liquidity["price"] > entry:
                # If nearest liquidity is < 1 ATR from entry -> skip trade.
                if (nearest_liquidity["price"] - entry) < atr:
                    return # Skip trade
                tp = nearest_liquidity["price"]
            elif o_highs:
                tp = min(o_highs)
            else:
                tp = entry + 2.5 * atr # Default TP
        else:
            o_lows = [s["price"] for s in sw30 if s["type"] == "low" and s["price"] < entry]
            if nearest_liquidity and nearest_liquidity["price"] < entry:
                # If nearest liquidity is < 1 ATR from entry -> skip trade.
                if (entry - nearest_liquidity["price"]) < atr:
                    return # Skip trade
                tp = nearest_liquidity["price"]
            elif o_lows:
                tp = max(o_lows)
            else:
                tp = entry - 2.5 * atr # Default TP

        if tp is None: return # Should not happen with default TP, but for safety

        # 4. Minimum Risk-Reward Filter (MANDATORY)
        rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
        if rr < 1.5:
            # print(f"Skipping {symbol}: RR ({rr:.2f}) < 1.5")
            return

        # 6. Avoid Trading Into Opposing HTF Order Block
        # Before BUY: If price is within 0.5 ATR below a 2H bearish OB -> skip.
        # Before SELL: If price is within 0.5 ATR above a 2H bullish OB -> skip.
        if direction == "BUY":
            for ob in order_blocks_2h:
                if ob["type"] == "BEARISH_OB" and (ob["low"] - entry) < 0.5 * atr and ob["low"] > entry:
                    # print(f"Skipping {symbol}: BUY signal too close to 2H Bearish OB.")
                    return
        elif direction == "SELL":
            for ob in order_blocks_2h:
                if ob["type"] == "BULLISH_OB" and (entry - ob["high"]) < 0.5 * atr and ob["high"] < entry:
                    # print(f"Skipping {symbol}: SELL signal too close to 2H Bullish OB.")
                    return

        rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
        msg = f"🚀 *SIGNAL: {symbol}*\nDir: {direction}\nConf: {confidence:.2f}%\nEntry: {entry:.5f}\nSL: {sl:.5f}\nTP: {tp:.5f}\nRR: 1:{rr:.2f}"
        self.notifier.send_telegram(msg)

        FirebaseManager.record_trade_outcome({
            "symbol": symbol, "direction": direction, "confidence": confidence,
            "entry": entry, "sl": sl, "tp": tp, "outcome": "PENDING",
            "toolbox_weights": self.toolbox_prefs.preferences.copy()
        })

    def daily_learning_audit_cycle(self):
        outcomes = FirebaseManager.get_trade_outcomes(days_back=14)
        if not outcomes: return
        comp_adjustments = {comp: 0.0 for comp in self.toolbox_prefs.preferences.keys()}
        comp_stats = {comp: {"wins": 0, "total": 0} for comp in self.toolbox_prefs.preferences.keys()}
        
        for t in outcomes:
            if t.get("outcome") in ["WIN", "LOSS"]:
                is_win = t["outcome"] == "WIN"
                weights_at_trade = t.get("toolbox_weights", self.toolbox_prefs.preferences)
                for comp, weight in weights_at_trade.items():
                    impact = weight if is_win else -weight
                    comp_adjustments[comp] += impact
                    comp_stats[comp]["total"] += 1
                    if is_win: comp_stats[comp]["wins"] += 1

        for comp, adj in comp_adjustments.items():
            if comp_stats[comp]["total"] > 0:
                self.toolbox_prefs.adjust_preference(comp, adj / comp_stats[comp]["total"])
                wr = comp_stats[comp]["wins"] / comp_stats[comp]["total"]
                self.toolbox_prefs.update_state(comp, wr)
                
        total_wr = sum(s["wins"] for s in comp_stats.values()) / sum(s["total"] for s in comp_stats.values()) if sum(s["total"] for s in comp_stats.values()) > 0 else 0.5
        if total_wr > 0.65: self.sr_probability_gate = 1.0
        elif 0.60 <= total_wr <= 0.65: self.sr_probability_gate = 0.8
        elif 0.45 <= total_wr < 0.60: self.sr_probability_gate = 0.5
        else: self.sr_probability_gate = 0.2

if __name__ == "__main__":
    bot = TradingBot()
    bot.run_scan()
