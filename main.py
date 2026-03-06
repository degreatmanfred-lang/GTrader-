'''import os
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
# EXECUTION CONTROL LAYER (RUNTIME LOCK & SCAN CONTROLLER)
# ============================================================

class ExecutionController:
    LOCK_FILE = "bot.lock"
    STATE_FILE = "bot_state.json"
    SCAN_INTERVAL_SECONDS = 15 * 60  # 15 minutes

    @staticmethod
    def is_in_volatility_window() -> bool:
        """Checks if current UTC time is within the predefined volatility windows."""
        now_utc = datetime.datetime.utcnow().time()
        # Window 1: 07:00 - 10:00 UTC
        in_window_1 = datetime.time(7, 0) <= now_utc <= datetime.time(10, 0)
        # Window 2: 13:00 - 16:00 UTC
        in_window_2 = datetime.time(13, 0) <= now_utc <= datetime.time(16, 0)
        return in_window_1 or in_window_2

    @classmethod
    def check_and_lock(cls) -> bool:
        """
        Implements the runtime lock system and 15-minute scan controller.
        Returns True if the bot is allowed to run, False otherwise.
        """
        # 1. Volatility Window Check (Must exit before any API request)
        if not cls.is_in_volatility_window():
            print(f"[{datetime.datetime.utcnow()}] Execution blocked: Outside volatility windows.")
            return False

        # 2. Runtime Lock System
        if os.path.exists(cls.LOCK_FILE):
            print(f"[{datetime.datetime.utcnow()}] Execution blocked: Lock file '{cls.LOCK_FILE}' exists.")
            return False

        # 3. 15-Minute Scan Controller
        if os.path.exists(cls.STATE_FILE):
            try:
                with open(cls.STATE_FILE, "r") as f:
                    state = json.load(f)
                    last_scan_timestamp = state.get("last_scan_timestamp", 0)
                    
                if (time.time() - last_scan_timestamp) < cls.SCAN_INTERVAL_SECONDS:
                    remaining = int(cls.SCAN_INTERVAL_SECONDS - (time.time() - last_scan_timestamp))
                    print(f"[{datetime.datetime.utcnow()}] Execution blocked: 15-minute interval not met. {remaining}s remaining.")
                    return False
            except Exception as e:
                print(f"Error reading state file: {e}")

        # If all checks pass, create the lock file
        try:
            with open(cls.LOCK_FILE, "w") as f:
                f.write(str(os.getpid()))
            return True
        except Exception as e:
            print(f"Error creating lock file: {e}")
            return False

    @classmethod
    def release_lock_and_update_state(cls):
        """Safely deletes the lock file and updates the last scan timestamp."""
        # Update last scan timestamp
        try:
            state = {"last_scan_timestamp": time.time()}
            with open(cls.STATE_FILE, "w") as f:
                json.dump(state, f)
        except Exception as e:
            print(f"Error updating state file: {e}")

        # Remove lock file
        if os.path.exists(cls.LOCK_FILE):
            try:
                os.remove(cls.LOCK_FILE)
            except Exception as e:
                print(f"Error removing lock file: {e}")

# ============================================================
# GITHUB WORKFLOW SELF-RESTART CONTROLLER
# ============================================================

class RestartController:
    def __init__(self, owner: str, repository: str, token: str):
        self.start_time = time.time()
        self.restart_triggered = False
        self.owner = owner
        self.repository = repository
        self.token = token
        self.last_check_time = time.time()
        print(f"RestartController initialized. Start time: {datetime.datetime.fromtimestamp(self.start_time)}")

    def _trigger_workflow_restart(self):
        if not self.owner or not self.repository or not self.token:
            print("GitHub OWNER, REPOSITORY, or BOT_TRIGGER_TOKEN not configured. Cannot trigger restart.")
            return

        url = f"https://api.github.com/repos/{self.owner}/{self.repository}/actions/workflows/tradingbot.yml/dispatches"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        payload = {"ref": "main"}

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            print(f"Successfully triggered GitHub workflow restart. Status: {response.status_code}")
            self.restart_triggered = True
        except requests.exceptions.RequestException as e:
            print(f"Error triggering GitHub workflow restart: {e}")

    def check_and_restart(self):
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        # Check every 60 seconds
        if (current_time - self.last_check_time) < 60:
            return
        self.last_check_time = current_time

        # Trigger restart if runtime reaches approximately 5 hours (18000 seconds) and not already triggered
        # Line 135
if elapsed_time >= 18000 and not self.restart_triggered:
    print(f"Runtime reached {elapsed_time:.2f} seconds (approx 5 hours). Triggering workflow restart...")

    self._trigger_workflow_restart()

    self.restart_triggered = True

    # Release lock so the next bot can start
    if os.path.exists("bot.lock"):
        os.remove("bot.lock")
        print("Lock released for new bot instance.")

    print("Exiting current bot for clean Bot A → Bot B handover.")

    import sys
    sys.exit(0)

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
    
    # GitHub Workflow Restart Controller Configuration
    GITHUB_OWNER = os.getenv("GITHUB_OWNER")
    GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")
    BOT_TRIGGER_TOKEN = os.getenv("BOT_TRIGGER_TOKEN")

    # TwelveData Limits per Key
    MINUTE_CALL_LIMIT = 7 # STRICT HARD LIMIT (Buffer safety)
    DAILY_CALL_LIMIT = 800
    
    CONFIDENCE_THRESHOLD = 75.0
    
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
        
        # 1️⃣ Initialization Per Key
        current_minute = int(time.time() // 60)
        self.usage = {
            key: {
                "calls_used": 0,
                "daily_calls": 0,
                "minute_bucket": current_minute,
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
            current_minute = int(now // 60)
            
            # 2️⃣ Reset Logic (Before Every Request)
            if stats["minute_bucket"] != current_minute:
                stats["calls_used"] = 0
                stats["minute_bucket"] = current_minute
            
            if now - stats["daily_start"] >= 86400:
                stats["daily_calls"] = 0
                stats["daily_start"] = now
                
            # 3️⃣ Enforce Ceiling
            if stats["calls_used"] < 7 and stats["daily_calls"] < Config.DAILY_CALL_LIMIT:
                stats["calls_used"] += 1
                stats["daily_calls"] += 1
                return key
            
            # Rotate to next key
            self.current_index = (self.current_index + 1) % len(self.keys)
            
            # 4️⃣ Sleep Logic (If All Keys Exhausted)
            if self.current_index == start_index:
                now = time.time()
                current_minute = int(now // 60)
                
                # Check if any key is available for daily limit
                any_daily_available = any(s["daily_calls"] < Config.DAILY_CALL_LIMIT for s in self.usage.values())
                
                if any_daily_available:
                    sleep_time = ((current_minute + 1) * 60) - now
                    if sleep_time > 0:
                        print(f"All keys exhausted for current minute. Sleeping for {sleep_time:.2f}s until next bucket...")
                        time.sleep(sleep_time)
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
        if db:
            try:
                docs = db.collection("trade_outcomes").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
                return [doc.to_dict() for doc in docs]
            except Exception as e:
                print(f"Error getting trade outcomes: {e}")
        return []

class TelegramNotifier:
    def __init__(self):
        self.token = Config.TELEGRAM_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID

    def send_telegram(self, message: str):
        if not self.token or not self.chat_id:
            print(f"Telegram Notification (Simulation): {message}")
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown"}
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"Telegram error: {e}")

class TA:
    @staticmethod
    def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
        high_low = df["high"] - df["low"]
        high_close = (df["high"] - df["close"].shift()).abs()
        low_close = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(window=window).mean()

    @staticmethod
    def get_fractal_swings(df: pd.DataFrame, window: int = 5) -> List[Dict]:
        swings = []
        for i in range(window, len(df) - window):
            is_high = all(df["high"].iloc[i] > df["high"].iloc[i-j] for j in range(1, window+1)) and \
                      all(df["high"].iloc[i] > df["high"].iloc[i+j] for j in range(1, window+1))
            is_low = all(df["low"].iloc[i] < df["low"].iloc[i-j] for j in range(1, window+1)) and \
                     all(df["low"].iloc[i] < df["low"].iloc[i+j] for j in range(1, window+1))
            if is_high: swings.append({"type": "high", "price": df["high"].iloc[i], "index": i})
            if is_low: swings.append({"type": "low", "price": df["low"].iloc[i], "index": i})
        return swings

    @staticmethod
    def detect_aoi(df: pd.DataFrame, swings: List[Dict], timeframe: str, memory: List[Dict]) -> List[Dict]:
        aois = []
        atr = TA.calculate_atr(df).iloc[-1]
        for s in swings:
            price = s["price"]
            found = False
            for a in aois:
                if abs(a["price"] - price) < 0.5 * atr:
                    a["hits"] += 1
                    found = True
                    break
            if not found:
                aois.append({"price": price, "min": price - 0.2 * atr, "max": price + 0.2 * atr, "hits": 1, "timeframe": timeframe, "durability_score": 1.0})
        
        for a in aois:
            for m in memory:
                if abs(a["price"] - m["price"]) < 0.5 * atr:
                    a["hits"] += m["hits"]
                    a["durability_score"] = min(5.0, 1.0 + (a["hits"] * 0.2))
        return aois

    @staticmethod
    def calculate_zone_entropy(df: pd.DataFrame, aois: List[Dict]) -> float:
        if not aois: return 50.0
        total_touches = sum(a["hits"] for a in aois)
        if total_touches == 0: return 50.0
        probabilities = [a["hits"] / total_touches for a in aois]
        entropy = -sum(p * np.log2(p) for p in probabilities if p > 0)
        max_entropy = np.log2(len(aois)) if len(aois) > 1 else 1.0
        return (entropy / max_entropy) * 100

    @staticmethod
    def detect_order_block(df: pd.DataFrame) -> List[Dict]:
        obs = []
        for i in range(2, len(df)):
            if df["close"].iloc[i] > df["high"].iloc[i-1] and df["close"].iloc[i-1] < df["open"].iloc[i-1]:
                obs.append({"type": "BULLISH_OB", "low": df["low"].iloc[i-1], "high": df["high"].iloc[i-1], "open": df["open"].iloc[i-1], "close": df["close"].iloc[i-1]})
            elif df["close"].iloc[i] < df["low"].iloc[i-1] and df["close"].iloc[i-1] > df["open"].iloc[i-1]:
                obs.append({"type": "BEARISH_OB", "low": df["low"].iloc[i-1], "high": df["high"].iloc[i-1], "open": df["open"].iloc[i-1], "close": df["close"].iloc[i-1]})
        return obs[-5:]

    @staticmethod
    def detect_fair_value_gap(df: pd.DataFrame) -> List[Dict]:
        fvgs = []
        for i in range(2, len(df)):
            if df["low"].iloc[i] > df["high"].iloc[i-2]:
                fvgs.append({"type": "BULLISH_FVG", "min": df["high"].iloc[i-2], "max": df["low"].iloc[i], "size": df["low"].iloc[i] - df["high"].iloc[i-2]})
            elif df["high"].iloc[i] < df["low"].iloc[i-2]:
                fvgs.append({"type": "BEARISH_FVG", "min": df["high"].iloc[i], "max": df["low"].iloc[i-2], "size": df["low"].iloc[i-2] - df["high"].iloc[i]})
        return fvgs[-5:]

    @staticmethod
    def detect_patterns(df: pd.DataFrame) -> Dict:
        return {"score": 50, "pattern_strength": 0.5}

    @staticmethod
    def detect_candlestick_confirmation(df: pd.DataFrame, direction: str) -> bool:
        last = df.iloc[-1]
        if direction == "BUY": return last["close"] > last["open"]
        return last["close"] < last["open"]

    @staticmethod
    def confirm_liquidity_sweep(df: pd.DataFrame, level_price: float, direction: str, trend: str) -> bool:
        last = df.iloc[-1]
        if direction == "BUY": return last["low"] < level_price and last["close"] > level_price
        return last["high"] > level_price and last["close"] < level_price

    @staticmethod
    def detect_volume_spike(df: pd.DataFrame) -> bool:
        return df["volume"].iloc[-1] > df["volume"].rolling(20).mean().iloc[-1] * 1.5

    @staticmethod
    def detect_micro_bos(df: pd.DataFrame, direction: str) -> bool:
        if direction == "BUY": return df["close"].iloc[-1] > df["high"].iloc[-2]
        return df["close"].iloc[-1] < df["low"].iloc[-2]

class TradingBot:
    def __init__(self):
        self.key_manager = APIKeyManager()
        self.handler = RequestHandler(self.key_manager)
        self.notifier = TelegramNotifier()
        self.toolbox_prefs = ToolboxPreference()
        self.aoi_memory = {}
        self.sr_probability_gate = 1.0
        self.trade_count_since_last_recalc = 0
        FirebaseManager.initialize_firebase()

    def _get_current_session_bias(self) -> float:
        now_utc = datetime.datetime.utcnow().hour
        if 8 <= now_utc <= 12: return 1.1 # London
        if 13 <= now_utc <= 17: return 1.2 # NY
        if 0 <= now_utc <= 7: return 0.9 # Asia
        return 1.0

    def _detect_all_liquidity(self, df: pd.DataFrame) -> List[Dict]:
        swings = TA.get_fractal_swings(df)
        levels = []
        for s in swings[-10:]:
            t = "SWING_HIGH" if s["type"] == "high" else "SWING_LOW"
            levels.append({"type": t, "price": s["price"]})
        return levels

    def get_macro_layer(self, df: pd.DataFrame) -> Dict:
        ema_fast = df["close"].ewm(span=50).mean()
        ema_slow = df["close"].ewm(span=200).mean()
        trend = "BULLISH" if ema_fast.iloc[-1] > ema_slow.iloc[-1] else "BEARISH"
        return {"score": 80, "trend": trend}

    def get_structural_layer(self, df: pd.DataFrame) -> Dict:
        return {"score": 70, "trend": "NEUTRAL"}

    def get_execution_layer(self, df: pd.DataFrame) -> Dict:
        last_candle = df.iloc[-1]
        body = abs(last_candle["close"] - last_candle["open"])
        atr = TA.calculate_atr(df).iloc[-1]
        
        impulse_score = min((body / (0.5 * atr)) * 25, 50) if atr > 0 else 0
        direction = "BUY" if last_candle["close"] > last_candle["open"] else "SELL"
        
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        timing_score = 0
        if direction == "BUY":
            timing_score = min(max(rsi.iloc[-1] - 30, 0) * 0.5, 20)
        else:
            timing_score = min(max(70 - rsi.iloc[-1], 0) * 0.5, 20)
            
        pref_impulse = self.toolbox_prefs.get_preference("execution_impulse_reaction")
        adaptive_bonus = (pref_impulse - 1.0) * 10
        adaptive_bonus = max(-10, min(10, adaptive_bonus))
        
        return {"score": impulse_score + timing_score + adaptive_bonus, "direction": direction}

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
                score_component = a["durability_score"] * 2.0 * pref_durability
                score_component *= (1 - (entropy_30m / 100))
                confluent_15m = any(aoi_item["min"] <= a["price"] <= aoi_item["max"] for aoi_item in aoi_15m)
                if confluent_15m: score_component *= 1.2
                aoi_pts += score_component
        return min(aoi_pts * pref_true_sr, 20.0)

    def run_scan(self, restart_controller=None):
        now_utc = datetime.datetime.utcnow().time()
        in_window_1 = datetime.time(7, 0) <= now_utc <= datetime.time(10, 0)
        in_window_2 = datetime.time(13, 0) <= now_utc <= datetime.time(16, 0)
        
        if not (in_window_1 or in_window_2):
            print(f"Outside volatility windows ({now_utc} UTC). Exiting.")
            return

        print(f"Starting Institutional Scan: {datetime.datetime.utcnow()} UTC")
        
        delay_between_assets = 8 

        valid_signals_count = 0
        rejected_setups = []

        for symbol in Config.SYMBOLS:
            start_time = time.time()
            failed_confirmations = []
            try:
                df_15m = self.handler.get_data(symbol, "15min")
                df_30m = self.handler.get_data(symbol, "30min")
                df_2h = self.handler.get_data(symbol, "2h")
                
                if df_15m is None or df_30m is None or df_2h is None:
                    failed_confirmations.append("API Data Missing")
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue
                if len(df_2h) < 20 or len(df_30m) < 20:
                    failed_confirmations.append("Insufficient Historical Data")
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue
                
                macro = self.get_macro_layer(df_2h)
                struct_30m = self.get_structural_layer(df_30m)
                exec_a = self.get_execution_layer(df_15m)

                htf_aligned = False
                if macro["trend"] == "BULLISH" and struct_30m["trend"] in ["BULLISH", "NEUTRAL"] and exec_a["direction"] == "BUY":
                    htf_aligned = True
                elif macro["trend"] == "BEARISH" and struct_30m["trend"] in ["BEARISH", "NEUTRAL"] and exec_a["direction"] == "SELL":
                    htf_aligned = True
                
                if not htf_aligned:
                    failed_confirmations.append(f"HTF Alignment Failed (Macro: {macro['trend']}, Struct: {struct_30m['trend']}, Exec: {exec_a['direction']})")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, exec_a["direction"])
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": exec_a["direction"], "final_score_percentage": 0,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue

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
                    failed_confirmations.append("No Structural AOI/OB Present")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, exec_a["direction"])
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": exec_a["direction"], "final_score_percentage": 0,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue

                if not TA.detect_candlestick_confirmation(df_15m, exec_a["direction"]):
                    failed_confirmations.append("No Candlestick Confirmation at AOI")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, exec_a["direction"])
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": exec_a["direction"], "final_score_percentage": 0,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue

                self.aoi_memory[symbol] = aoi_2h + aoi_30m + TA.detect_aoi(df_15m, TA.get_fractal_swings(df_15m), "15M", self.aoi_memory.get(symbol, []))
                
                liquidity_levels = self._detect_all_liquidity(df_15m)
                liquidity_sweep_confirmed = False
                for level in liquidity_levels:
                    if (exec_a["direction"] == "BUY" and level["type"] in ["EQUAL_LOW", "SWING_LOW", "ASIA_LOW", "LONDON_LOW", "NY_LOW"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "BUY", macro["trend"])) or \
                       (exec_a["direction"] == "SELL" and level["type"] in ["EQUAL_HIGH", "SWING_HIGH", "ASIA_HIGH", "LONDON_HIGH", "NY_HIGH"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "SELL", macro["trend"])):
                        liquidity_sweep_confirmed = True
                        break
                
                liquidity_score = 0
                if liquidity_sweep_confirmed:
                    pref_liq = self.toolbox_prefs.get_preference("liquidity_sweep_confirmation")
                    liquidity_score = 20 * pref_liq
                    if TA.detect_volume_spike(df_15m): liquidity_score += 10
                liquidity_score = min(liquidity_score, 15)
                
                aoi_pts = self.get_aoi_confluence_score(symbol, df_15m, df_30m, df_2h)
                current_aois = self.aoi_memory.get(symbol, [])
                
                entropy_pts = (10 - TA.calculate_zone_entropy(df_15m, current_aois) / 10) * self.toolbox_prefs.get_preference("structural_aoi_entropy")
                entropy_pts = max(0, min(10, entropy_pts))
                
                pattern_data = TA.detect_patterns(df_15m)
                pattern_pts = min((pattern_data["score"] * pattern_data["pattern_strength"] / 100) * 15 * self.toolbox_prefs.get_preference("pattern_intelligence_toolbox"), 15)

                order_blocks = TA.detect_order_block(df_15m)
                fvgs = TA.detect_fair_value_gap(df_15m)
                ob_fvg_confluence_score = 0
                last_price = df_15m["close"].iloc[-1]
                ob_found = any((ob["type"] == "BULLISH_OB" if exec_a["direction"] == "BUY" else ob["type"] == "BEARISH_OB") and ob["low"] <= last_price <= ob["high"] for ob in order_blocks)
                fvg_found = any((fvg["type"] == "BULLISH_FVG" if exec_a["direction"] == "BUY" else fvg["type"] == "BEARISH_FVG") and fvg["min"] <= last_price <= fvg["max"] for fvg in fvgs)
                if ob_found and fvg_found:
                    ob_fvg_confluence_score = 15 * self.toolbox_prefs.get_preference("order_block_confluence") * self.toolbox_prefs.get_preference("fvg_confluence")
                elif ob_found or fvg_found:
                    ob_fvg_confluence_score = 7.5

                confluence_total = pattern_pts + aoi_pts + ob_fvg_confluence_score + liquidity_score
                confluence_total = min(confluence_total, 40)

                t_macro = max(0, min(macro["score"], 100))
                t_struct = max(0, min(struct_30m["score"] + entropy_pts + confluence_total, 100))
                t_exec = max(0, min(exec_a["score"], 100))
                
                confidence_raw = (t_macro * Config.WEIGHT_MACRO) + (t_struct * Config.WEIGHT_STRUCTURAL) + (t_exec * Config.WEIGHT_EXECUTION)
                direction = exec_a["direction"]
                confidence_raw *= self._get_current_session_bias()
                confidence = min(confidence_raw, 100)
                
                if aoi_pts < 5: confidence *= self.sr_probability_gate

                bos_occurred = TA.detect_micro_bos(df_15m, exec_a["direction"])
                pullback_continuation_confirmed = False
                if bos_occurred:
                    prev_high = df_15m["high"].iloc[-2]
                    prev_low = df_15m["low"].iloc[-2]
                    if direction == "BUY":
                        pullback_continuation_confirmed = df_15m["low"].iloc[-1] <= prev_high
                    else:
                        pullback_continuation_confirmed = df_15m["high"].iloc[-1] >= prev_low
                else:
                    pullback_continuation_confirmed = True

                if not pullback_continuation_confirmed:
                    failed_confirmations.append("BOS Retest Mandatory but Not Confirmed")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, exec_a["direction"])
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": exec_a["direction"], "final_score_percentage": confidence,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                    if restart_controller:
                        restart_controller.check_and_restart()
                    time.sleep(delay_between_assets)
                    continue

                if confidence >= Config.CONFIDENCE_THRESHOLD and direction != "NEUTRAL":
                    order_blocks_2h = TA.detect_order_block(df_2h)
                    if self.execute_signal(symbol, direction, confidence, df_15m, df_30m, df_2h, liquidity_levels, order_blocks_2h):
                        valid_signals_count += 1
                    else:
                        failed_confirmations.append("Risk/Reward, TP Missing, or HTF OB Conflict")
                        entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, direction)
                        rejected_setups.append({
                            "asset_name": symbol, "direction_bias": direction, "final_score_percentage": confidence,
                            "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                            "list_of_failed_confirmations": failed_confirmations
                        })
                else:
                    if direction == "NEUTRAL": failed_confirmations.append("Direction Neutral")
                    if confidence < Config.CONFIDENCE_THRESHOLD: failed_confirmations.append(f"Confidence Below Threshold ({confidence:.2f}%)")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, direction)
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": direction, "final_score_percentage": confidence,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                
                if restart_controller:
                    restart_controller.check_and_restart()
                time.sleep(delay_between_assets)
            except Exception:
                traceback.print_exc()
                if restart_controller:
                    restart_controller.check_and_restart()
                time.sleep(delay_between_assets)
        
        print("Scan complete.")
        if valid_signals_count == 0:
            self._send_diagnostic_report(rejected_setups)

    def _calculate_trade_params(self, df15, df30, df2h, direction) -> Tuple[float, float, float]:
        entry = df15["close"].iloc[-1]
        atr15 = TA.calculate_atr(df15).iloc[-1]
        if atr15 == 0: return entry, 0, 0
        
        sw15 = TA.get_fractal_swings(df15)
        recent_swings = [s for s in sw15 if s["index"] >= len(df15) - 15]
        
        if direction == "BUY":
            valid_lows = [s["price"] for s in recent_swings if s["type"] == "low" and s["price"] < entry]
            if valid_lows:
                sl = max(valid_lows) - 0.2 * atr15
            else:
                return entry, 0, 0
        else:
            valid_highs = [s["price"] for s in recent_swings if s["type"] == "high" and s["price"] > entry]
            if valid_highs:
                sl = min(valid_highs) + 0.2 * atr15
            else:
                return entry, 0, 0
            
        sw30 = TA.get_fractal_swings(df30)
        sw2h = TA.get_fractal_swings(df2h)
        
        if direction == "BUY":
            targets = [s["price"] for s in sw30 if s["type"] == "high" and s["price"] > entry]
            if not targets: return entry, 0, 0
            tp = min(targets)
            resistances = [s["price"] for s in sw2h if s["type"] == "high" and s["price"] > entry]
            if resistances:
                tp = min(tp, min(resistances))
        else:
            targets = [s["price"] for s in sw30 if s["type"] == "low" and s["price"] < entry]
            if not targets: return entry, 0, 0
            tp = max(targets)
            supports = [s["price"] for s in sw2h if s["type"] == "low" and s["price"] < entry]
            if supports:
                tp = max(tp, max(supports))
                
        return entry, sl, tp

    def _send_diagnostic_report(self, rejected_setups: List[Dict]):
        if not rejected_setups: return
        sorted_setups = sorted(rejected_setups, key=lambda x: x["final_score_percentage"], reverse=True)
        top_5 = sorted_setups[:5]
        
        report = "📊 *Institutional Scan Diagnostic Report*\n\nNo Valid Signals Produced.\n\nTop Rejected Setups:\n\n"
        for i, setup in enumerate(top_5, 1):
            reasons = "\n".join([f"- {r}" for r in setup["list_of_failed_confirmations"]])
            report += f"{i}️⃣ *{setup['asset_name']}*\n"
            report += f"Bias: {setup['direction_bias']}\n"
            report += f"Score: {setup['final_score_percentage']:.2f}%\n"
            report += f"Entry: {setup['calculated_entry']:.5f}\n"
            report += f"Stop Loss: {setup['calculated_stop_loss']:.5f}\n"
            report += f"Take Profit: {setup['calculated_take_profit']:.5f}\n"
            report += f"Failed Confirmations:\n{reasons}\n\n"
        
        self.notifier.send_telegram(report)

    def execute_signal(self, symbol, direction, confidence, df15, df30, df2h, all_liquidity_levels, order_blocks_2h) -> bool:
        entry, sl, tp = self._calculate_trade_params(df15, df30, df2h, direction)
        if sl == 0: return False

        rr = abs(tp - entry) / abs(entry - sl) if abs(entry - sl) > 0 else 0
        if rr < 1.5: return False

        atr = TA.calculate_atr(df15).iloc[-1]
        if direction == "BUY":
            for ob in order_blocks_2h:
                if ob["type"] == "BEARISH_OB" and (ob["low"] - entry) < 0.5 * atr and ob["low"] > entry: return False
        elif direction == "SELL":
            for ob in order_blocks_2h:
                if ob["type"] == "BULLISH_OB" and (entry - ob["high"]) < 0.5 * atr and ob["high"] < entry: return False

        msg = f"🚀 *SIGNAL: {symbol}*\nDir: {direction}\nConf: {confidence:.2f}%\nEntry: {entry:.5f}\nSL: {sl:.5f}\nTP: {tp:.5f}\nRR: 1:{rr:.2f}"
        self.notifier.send_telegram(msg)

        FirebaseManager.record_trade_outcome({
            "symbol": symbol, "direction": direction, "confidence": confidence,
            "entry": entry, "sl": sl, "tp": tp, "outcome": "PENDING",
            "toolbox_weights": self.toolbox_prefs.preferences.copy()
        })
        
        self.trade_count_since_last_recalc += 1
        if self.trade_count_since_last_recalc >= 20:
            self.daily_learning_audit_cycle()
            self.trade_count_since_last_recalc = 0
        return True

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
restart_controller = RestartController(
        owner=Config.GITHUB_OWNER,
        repository=Config.GITHUB_REPOSITORY,
        token=Config.BOT_TRIGGER_TOKEN
    )

    while True:

        if ExecutionController.check_and_lock():

            try:
                bot = TradingBot()
                bot.run_scan(restart_controller=restart_controller)

            finally:
                ExecutionController.release_lock_and_update_state()

        time.sleep(30)

            
    else:
        # Program exits here if conditions are not met, before reaching the scanning stage.
        pass
'''
