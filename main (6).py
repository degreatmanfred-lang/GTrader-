import os
import requests
import time
import pandas as pd
import numpy as np
import traceback
import datetime
import json
import random
import threading
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
        self.owner = owner
        self.repository = repository
        self.token = token
        self.runtime_limit = 5 * 3600  # 5 hours in seconds
        print(f"[RestartController] Timer started. Start time: {datetime.datetime.fromtimestamp(self.start_time)}")
        
        # Start background thread timer
        self.timer_thread = threading.Thread(target=self._run_timer, daemon=True)
        self.timer_thread.start()

    def _run_timer(self):
        """Background thread that monitors runtime and triggers restart."""
        while True:
            elapsed = time.time() - self.start_time
            if elapsed >= self.runtime_limit:
                print(f"[RestartController] 5 hour runtime reached ({elapsed:.2f}s)")
                self._trigger_handover()
                break
            time.sleep(60) # Check every minute

    def _trigger_handover(self):
        """Handles the handover process: remove lock, dispatch workflow, exit."""
        # 1. Remove bot.lock
        if os.path.exists("bot.lock"):
            try:
                os.remove("bot.lock")
                print("[System] Lock file removed")
            except Exception as e:
                print(f"[System] Error removing lock file: {e}")

        # 2. Trigger GitHub workflow dispatch
        if not self.owner or not self.repository or not self.token:
            print("[System] GitHub config missing. Cannot trigger dispatch. Exiting.")
            import sys
            sys.exit(1)

        url = f"https://api.github.com/repos/{self.owner}/{self.repository}/actions/workflows/tradingbot.yml/dispatches"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        payload = {"ref": "main"}

        # BotA must never exit before dispatch confirmation.
        # BotA may wait up to 15 minutes maximum for confirmation.
        start_dispatch = time.time()
        success = False
        
        print("[System] Workflow dispatch triggered")
        while time.time() - start_dispatch < 900: # 15 minutes max
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                if response.status_code in [201, 204]:
                    print(f"[System] Workflow dispatch request succeeded (Status: {response.status_code})")
                    success = True
                    break
                else:
                    print(f"[System] Dispatch failed with status {response.status_code}. Retrying in 30s...")
            except Exception as e:
                print(f"[System] Dispatch error: {e}. Retrying in 30s...")
            
            time.sleep(30)

        if not success:
            print("[System] Failed to confirm dispatch after 15 minutes. Exiting anyway to prevent infinite run.")
        
        print("[RestartController] Exiting BotA for handover.")
        import sys
        sys.exit(0)

    def check_and_restart(self):
        """Legacy method kept for compatibility, but logic moved to background thread."""
        pass

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
    TWELVEDATA_API_KEY_4 = os.getenv("TWELVEDATA_API_KEY_4")
    TWELVEDATA_API_KEY_5 = os.getenv("TWELVEDATA_API_KEY_5")
    
    FIREBASE_SERVICE_ACCOUNT_KEY = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY")
    
    # GitHub Workflow Restart Controller Configuration
    GITHUB_OWNER = os.getenv("GITHUB_OWNER")
    GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY")
    BOT_TRIGGER_TOKEN = os.getenv("BOT_TRIGGER_TOKEN")

    # TwelveData Limits per Key
    MINUTE_CALL_LIMIT = 7
    DAILY_CALL_LIMIT = 800
    
    CONFIDENCE_THRESHOLD = 68.0
    
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
            Config.TWELVEDATA_API_KEY_3,
            Config.TWELVEDATA_API_KEY_4,
            Config.TWELVEDATA_API_KEY_5
        ]
        # Filter out None keys
        self.keys = [k for k in self.keys if k]
        
        if not self.keys:
            raise ValueError("No TwelveData API keys configured.")
            
        self.current_index = 0
        self.last_utc_date = datetime.datetime.utcnow().date()
        
        # Initialization Per Key
        self.usage = {
            key: {
                "calls_this_minute": 0,
                "calls_today": 0,
                "last_call_time": 0,
                "exhausted_today": False
            } for key in self.keys
        }

    def _check_daily_reset(self):
        """Checks if UTC date has changed and resets daily counters."""
        now_utc = datetime.datetime.utcnow()
        current_date = now_utc.date()
        if current_date != self.last_utc_date:
            print(f"[API RESET] UTC midnight detected ({current_date})")
            for key in self.keys:
                self.usage[key]["calls_today"] = 0
                self.usage[key]["exhausted_today"] = False
            self.last_utc_date = current_date
            print("[API RESET] Daily counters reset")

    def mark_key_exhausted(self, key: str):
        """Marks a key as exhausted for the day if a limit error is received."""
        if key in self.usage:
            self.usage[key]["exhausted_today"] = True
            print(f"[API ROTATION] Key #{self.keys.index(key)+1} marked as exhausted today due to API error.")

    def get_active_key(self) -> Optional[str]:
        self._check_daily_reset()
        
        start_index = self.current_index
        while True:
            key = self.keys[self.current_index]
            stats = self.usage[key]
            now = time.time()
            
            # Reset minute limit if a minute has passed
            if int(now // 60) > int(stats["last_call_time"] // 60):
                stats["calls_this_minute"] = 0
            
            # Check limits
            if not stats["exhausted_today"] and \
               stats["calls_this_minute"] < Config.MINUTE_CALL_LIMIT and \
               stats["calls_today"] < Config.DAILY_CALL_LIMIT:
                
                # Add 3-5 second delay between API calls
                time.sleep(random.uniform(3, 5))
                
                stats["calls_this_minute"] += 1
                stats["calls_today"] += 1
                stats["last_call_time"] = time.time()
                
                print(f"[API ROTATION] Using key #{self.current_index + 1}")
                return key
            
            # If minute limit reached, log it
            if stats["calls_this_minute"] >= Config.MINUTE_CALL_LIMIT:
                print(f"[API ROTATION] Key #{self.current_index + 1} minute limit reached")
            
            # Rotate to next key
            self.current_index = (self.current_index + 1) % len(self.keys)
            print(f"[API ROTATION] Switching to key #{self.current_index + 1}")
            
            # If we've checked all keys and none are available
            if self.current_index == start_index:
                # Check if all are exhausted for the day
                if all(self.usage[k]["exhausted_today"] or self.usage[k]["calls_today"] >= Config.DAILY_CALL_LIMIT for k in self.keys):
                    print("[API ROTATION] ALL KEYS EXHAUSTED FOR THE DAY. Waiting for UTC reset...")
                    # Wait until next UTC day
                    while datetime.datetime.utcnow().date() == self.last_utc_date:
                        time.sleep(60)
                    self._check_daily_reset()
                    continue
                
                # Otherwise, all are just at minute limit, wait for next minute
                print("[API ROTATION] All keys at minute limit. Waiting 30s...")
                time.sleep(30)
                continue

# ============================================================
# DATA ACQUISITION & REQUEST HANDLING
# ============================================================

class RequestHandler:
    def __init__(self, key_manager: APIKeyManager):
        self.key_manager = key_manager

    def get_data(self, symbol: str, interval: str, outputsize: int = 100) -> Optional[pd.DataFrame]:
        """Fetches historical data from TwelveData with key rotation and error handling."""
        # BotA must never terminate early due to API errors or limits.
        while True:
            api_key = self.key_manager.get_active_key()
            if not api_key:
                time.sleep(60)
                continue

            url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={outputsize}&apikey={api_key}"
            
            try:
                response = requests.get(url, timeout=30)
                data = response.json()

                if response.status_code == 200:
                    if "values" in data:
                        df = pd.DataFrame(data["values"])
                        df["datetime"] = pd.to_datetime(df["datetime"])
                        df = df.sort_values("datetime")
                        for col in ["open", "high", "low", "close"]:
                            df[col] = df[col].astype(float)
                        return df
                    elif "code" in data and data["code"] == 429:
                        # Check if it's a daily limit error
                        if "daily" in data.get("message", "").lower():
                            self.key_manager.mark_key_exhausted(api_key)
                        continue # Key manager will handle rotation
                    else:
                        print(f"API Error for {symbol}: {data.get('message', 'Unknown error')}")
                        return None
                else:
                    print(f"HTTP Error {response.status_code} for {symbol}")
                    return None

            except Exception as e:
                print(f"Request Exception for {symbol}: {e}")
                time.sleep(10)
                continue

# ============================================================
# NOTIFICATION SYSTEM
# ============================================================

class TelegramNotifier:
    def __init__(self):
        self.token = Config.TELEGRAM_TOKEN
        self.chat_id = Config.TELEGRAM_CHAT_ID

    def send_telegram(self, message: str):
        if not self.token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"Telegram Error: {e}")

# ============================================================
# FIREBASE PERSISTENCE LAYER
# ============================================================

class FirebaseManager:
    _db = None

    @classmethod
    def initialize_firebase(cls):
        if not cls._db:
            try:
                service_account_info = json.loads(Config.FIREBASE_SERVICE_ACCOUNT_KEY)
                cred = credentials.Certificate(service_account_info)
                firebase_admin.initialize_app(cred)
                cls._db = firestore.client()
            except Exception as e:
                print(f"Firebase Init Error: {e}")

    @classmethod
    def record_trade_outcome(cls, trade_data: Dict):
        if not cls._db: return
        try:
            trade_data["timestamp"] = firestore.SERVER_TIMESTAMP
            cls._db.collection("trades").add(trade_data)
        except Exception as e:
            print(f"Firebase Record Error: {e}")

    @classmethod
    def get_trade_outcomes(cls, limit: int = 100) -> List[Dict]:
        if not cls._db: return []
        try:
            docs = cls._db.collection("trades").order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()
            return [doc.to_dict() for doc in docs]
        except Exception as e:
            print(f"Firebase Fetch Error: {e}")
            return []

# ============================================================
# TECHNICAL ANALYSIS TOOLBOX (TA)
# ============================================================

class TA:
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high_low = df["high"] - df["low"]
        high_close = np.abs(df["high"] - df["close"].shift())
        low_close = np.abs(df["low"] - df["close"].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(window=period).mean()

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
        for s in swings:
            aois.append({
                "price": s["price"], "min": s["price"] * 0.9998, "max": s["price"] * 1.0002,
                "type": s["type"], "timeframe": timeframe, "durability_score": 10
            })
        return aois

    @staticmethod
    def calculate_zone_entropy(df: pd.DataFrame, aois: List[Dict]) -> float:
        return 20.0

    @staticmethod
    def detect_order_block(df: pd.DataFrame) -> List[Dict]:
        return []

    @staticmethod
    def detect_candlestick_confirmation(df: pd.DataFrame, direction: str) -> bool:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        if direction == "BUY": return last["close"] > prev["high"]
        return last["close"] < prev["low"]

    @staticmethod
    def confirm_liquidity_sweep(df: pd.DataFrame, level_price: float, direction: str, trend: str) -> bool:
        last = df.iloc[-1]
        if direction == "BUY": return last["low"] < level_price and last["close"] > level_price
        return last["high"] > level_price and last["close"] < level_price

    @staticmethod
    def detect_volume_spike(df: pd.DataFrame) -> bool:
        if "volume" not in df.columns:
            return False
        return df["volume"].iloc[-1] > df["volume"].rolling(20).mean().iloc[-1] * 1.5

    @staticmethod
    def detect_micro_bos(df: pd.DataFrame, direction: str) -> bool:
        if direction == "BUY": return df["close"].iloc[-1] > df["high"].iloc[-2]
        return df["close"].iloc[-1] < df["low"].iloc[-2]

# ============================================================
# ADAPTIVE PREFERENCE ENGINE
# ============================================================

class ToolboxPreference:
    def __init__(self):
        self.preferences = Config.TOOLBOX_PREFERENCES.copy()
        self.performance_history = {k: [] for k in self.preferences.keys()}

    def get_preference(self, component: str) -> float:
        return self.preferences.get(component, 1.0)

    def adjust_preference(self, component: str, adjustment: float):
        if component in self.preferences:
            self.preferences[component] = max(0.5, min(2.0, self.preferences[component] + adjustment))

    def update_state(self, component: str, win_rate: float):
        pass

# ============================================================
# CORE TRADING BOT ENGINE
# ============================================================

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
                    time.sleep(delay_between_assets)
                    continue
                if len(df_2h) < 20 or len(df_30m) < 20:
                    failed_confirmations.append("Insufficient Historical Data")
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
                    time.sleep(delay_between_assets)
                    continue

                self.aoi_memory[symbol] = aoi_2h + aoi_30m + TA.detect_aoi(df_15m, TA.get_fractal_swings(df_15m), "15M", self.aoi_memory.get(symbol, []))
                
                liquidity_levels = self._detect_all_liquidity(df_15m)
                liquidity_sweep_confirmed = False
                for level in liquidity_levels:
                    if (exec_a["direction"] == "BUY" and level["type"] in ["EQUAL_LOW", "SWING_LOW", "ASIA_LOW", "LONDON_LOW", "NY_LOW"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "BUY", macro["trend"])) or \
                       (exec_a["direction"] == "SELL" and level["type"] in ["EQUAL_HIGH", "SWING_HIGH", "ASIA_HIGH", "LONDON_HIGH", "NY_HIGH"] and TA.confirm_liquidity_sweep(df_15m, level["price"], "SELL", macro["trend"])):
                        liquidity_sweep_confirmed = True
                
                liquidity_score = 20 if liquidity_sweep_confirmed else 0
                aoi_score = self.get_aoi_confluence_score(symbol, df_15m, df_30m, df_2h)
                
                final_score = (macro["score"] * Config.WEIGHT_MACRO) + \
                              ((struct_30m["score"] + aoi_score) * Config.WEIGHT_STRUCTURAL) + \
                              ((exec_a["score"] + liquidity_score) * Config.WEIGHT_EXECUTION)
                
                if final_score >= Config.CONFIDENCE_THRESHOLD:
                    if self.execute_signal(symbol, exec_a["direction"], final_score, df_15m, df_30m, df_2h, liquidity_levels, []):
                        valid_signals_count += 1
                else:
                    failed_confirmations.append(f"Confidence Threshold Not Met ({final_score:.2f}%)")
                    entry, sl, tp = self._calculate_trade_params(df_15m, df_30m, df_2h, exec_a["direction"])
                    rejected_setups.append({
                        "asset_name": symbol, "direction_bias": exec_a["direction"], "final_score_percentage": final_score,
                        "calculated_entry": entry, "calculated_stop_loss": sl, "calculated_take_profit": tp,
                        "list_of_failed_confirmations": failed_confirmations
                    })
                
                time.sleep(delay_between_assets)

            except Exception as e:
                print(f"Error scanning {symbol}: {e}")
                traceback.print_exc()
                time.sleep(delay_between_assets)

        if valid_signals_count == 0:
            pass # Diagnostic reports are never sent to Telegram

    def _calculate_trade_params(self, df15, df30, df2h, direction) -> Tuple[float, float, float]:
        entry = df15["close"].iloc[-1]
        atr = TA.calculate_atr(df15).iloc[-1]
        if direction == "BUY":
            sl = entry - (2 * atr)
            tp = entry + (3 * atr)
        else:
            sl = entry + (2 * atr)
            tp = entry - (3 * atr)
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
