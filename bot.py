# -*- coding: utf-8 -*-
"""
AI-нутрициолог для Telegram (aiogram 3.x + google-genai + aiosqlite).

ВАЖНО про безопасность:
  Токен бота и ключи Gemini НЕ хранятся в коде. Задай их в переменных окружения:

    export BOT_TOKEN="123456:ABC..."
    export GEMINI_API_KEYS="key1,key2,key3"     # через запятую
    export ADMIN_ID="1183393935"                # необязательно

Запуск:
    pip install aiogram aiosqlite google-genai
    python nutrition_bot.py
"""

import asyncio
import logging
import os
import re
import json
import html
import sqlite3
import io
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
from dataclasses import dataclass

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton,
    Message, CallbackQuery, PreCheckoutQuery, LabeledPrice, BufferedInputFile
)
from aiogram.exceptions import TelegramBadRequest

import aiosqlite

from google import genai
from google.genai import types as genai_types
from google.genai.errors import APIError

# Загружаем переменные из файла .env, если установлен python-dotenv (pip install python-dotenv).
# Без пакета бот тоже работает — тогда переменные берутся из системного окружения.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Конфигурация (из окружения) ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "1183393935"))
GEMINI_API_KEYS = [k.strip() for k in os.getenv("GEMINI_API_KEYS", "").split(",") if k.strip()]

GEMINI_MODELS_SMART = ["models/gemini-2.5-flash", "models/gemini-2.0-flash"]
GEMINI_MODELS_LITE = ["models/gemini-2.5-flash-lite", "models/gemini-2.0-flash-lite", "models/gemini-2.0-flash"]

# Параллельность запросов к ИИ: не 1 (иначе все пользователи стоят в очереди), но и не безлимит.
AI_SEMAPHORE = asyncio.Semaphore(int(os.getenv("AI_CONCURRENCY", "4")))

DB_NAME = os.getenv("DB_NAME", "nutrition_bot.db")

# Сколько дней подписки дарим за приглашённого друга (и ему, и пригласившему)
REFERRAL_BONUS_DAYS = int(os.getenv("REFERRAL_BONUS_DAYS", "7"))
BOT_USERNAME = ""  # заполняется при старте в main()

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# Кэш клиентов Gemini по ключу (не пересоздаём на каждый запрос)
_clients: Dict[str, "genai.Client"] = {}


def get_client(key: str) -> "genai.Client":
    if key not in _clients:
        _clients[key] = genai.Client(api_key=key)
    return _clients[key]


# --- Модель пользователя ---
@dataclass
class UserProfile:
    user_id: int
    language: str = "ru"
    weight: float = 70.0
    height: float = 170.0
    age: int = 30
    gender: str = "male"
    activity: str = "sedentary"
    goal: str = "maintain"
    allergies: str = ""
    favorite_foods: str = ""
    disliked_foods: str = ""
    sport_types: str = ""
    habits: str = ""
    utc_offset: int = 3


# --- База данных ---
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                language TEXT DEFAULT 'ru',
                weight REAL DEFAULT 70.0,
                height REAL DEFAULT 170.0,
                age INTEGER DEFAULT 30,
                gender TEXT DEFAULT 'male',
                activity TEXT DEFAULT 'sedentary',
                goal TEXT DEFAULT 'maintain',
                allergies TEXT DEFAULT '',
                favorite_foods TEXT DEFAULT '',
                disliked_foods TEXT DEFAULT '',
                sport_types TEXT DEFAULT '',
                habits TEXT DEFAULT '',
                utc_offset INTEGER DEFAULT 3,
                subscribed_until TEXT,
                trial_used INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS food_diary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                date TEXT,
                meal_time TEXT,
                description TEXT,
                calories REAL,
                protein REAL,
                fat REAL,
                carbs REAL,
                photo_id TEXT
            );
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                time TEXT,
                text TEXT,
                active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS referrals (
                referred_id INTEGER PRIMARY KEY,
                referrer_id INTEGER,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS water_log (
                user_id INTEGER,
                date TEXT,
                amount_ml INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, date)
            );
            CREATE TABLE IF NOT EXISTS weight_log (
                user_id INTEGER,
                date TEXT,
                weight REAL,
                PRIMARY KEY (user_id, date)
            );
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                description TEXT,
                calories REAL,
                protein REAL,
                fat REAL,
                carbs REAL,
                UNIQUE(user_id, description)
            );
            CREATE TABLE IF NOT EXISTS shopping_list (
                user_id INTEGER PRIMARY KEY,
                content TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_diary_user_date ON food_diary(user_id, date);
            CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id);
            CREATE INDEX IF NOT EXISTS idx_fav_user ON favorites(user_id);
        """)
        await db.commit()


async def add_referral(referrer_id: int, referred_id: int) -> bool:
    """Возвращает True, если реферал засчитан впервые (без самоприглашения и дублей)."""
    if referrer_id == referred_id:
        return False
    async with aiosqlite.connect(DB_NAME) as db:
        # реферер должен существовать
        async with db.execute("SELECT 1 FROM users WHERE user_id=?", (referrer_id,)) as cur:
            if not await cur.fetchone():
                return False
        try:
            await db.execute(
                "INSERT INTO referrals (referred_id, referrer_id, created_at) VALUES (?,?,?)",
                (referred_id, referrer_id, datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # этого пользователя уже приглашали


async def count_referrals(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


# --- Вода ---
async def add_water(user_id: int, date: str, delta_ml: int) -> int:
    """Прибавляет (или убавляет) воду за день, возвращает новый итог (не меньше 0)."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO water_log (user_id, date, amount_ml) VALUES (?,?,?)
            ON CONFLICT(user_id, date) DO UPDATE SET amount_ml = MAX(0, amount_ml + ?)
        """, (user_id, date, max(0, delta_ml), delta_ml))
        await db.commit()
        async with db.execute("SELECT amount_ml FROM water_log WHERE user_id=? AND date=?",
                              (user_id, date)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def get_water(user_id: int, date: str) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT amount_ml FROM water_log WHERE user_id=? AND date=?",
                              (user_id, date)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


def water_goal_ml(profile: UserProfile) -> int:
    """Норма воды ~30 мл на кг, округлённая до 100 мл, в разумных пределах."""
    goal = round(profile.weight * 30 / 100) * 100
    return int(max(1500, min(goal, 4000)))


# --- Вес ---
async def add_weight(user_id: int, date: str, weight: float):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO weight_log (user_id, date, weight) VALUES (?,?,?)
            ON CONFLICT(user_id, date) DO UPDATE SET weight=excluded.weight
        """, (user_id, date, weight))
        await db.commit()


async def get_weight_history(user_id: int, limit: int = 60) -> List[tuple]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute(
            "SELECT date, weight FROM weight_log WHERE user_id=? ORDER BY date DESC LIMIT ?",
            (user_id, limit)
        ) as cur:
            rows = await cur.fetchall()
    return [(r[0], r[1]) for r in reversed(rows)]  # по возрастанию даты


# --- Частые блюда и избранное ---
async def get_frequent_meals(user_id: int, limit: int = 6) -> List[dict]:
    """Самые частые блюда пользователя с самыми свежими КБЖУ для каждого."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT fd.id, fd.description, fd.calories, fd.protein, fd.fat, fd.carbs
            FROM food_diary fd
            JOIN (
                SELECT description, MAX(id) AS mid, COUNT(*) AS cnt
                FROM food_diary WHERE user_id=?
                GROUP BY description
            ) g ON fd.id = g.mid
            ORDER BY g.cnt DESC, g.mid DESC
            LIMIT ?
        """, (user_id, limit)) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_food_row(food_id: int, user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM food_diary WHERE id=? AND user_id=?",
                              (food_id, user_id)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def add_favorite(user_id: int, description: str, cal, p, f, c) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        try:
            await db.execute("""INSERT INTO favorites
                (user_id, description, calories, protein, fat, carbs) VALUES (?,?,?,?,?,?)""",
                (user_id, description, cal, p, f, c))
            await db.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # уже в избранном


async def get_favorites(user_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM favorites WHERE user_id=? ORDER BY id DESC", (user_id,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_favorite(fav_id: int, user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM favorites WHERE id=? AND user_id=?",
                              (fav_id, user_id)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def remove_favorite(fav_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("DELETE FROM favorites WHERE id=? AND user_id=?", (fav_id, user_id))
        await db.commit()
        return cur.rowcount > 0


# --- Список покупок ---
async def save_shopping_list(user_id: int, content: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""INSERT INTO shopping_list (user_id, content) VALUES (?,?)
            ON CONFLICT(user_id) DO UPDATE SET content=excluded.content""", (user_id, content))
        await db.commit()


async def get_shopping_list(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT content FROM shopping_list WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] else None


async def get_user(user_id: int) -> Optional[UserProfile]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            if row:
                return UserProfile(
                    user_id=row['user_id'], language=row['language'], weight=row['weight'],
                    height=row['height'], age=row['age'], gender=row['gender'],
                    activity=row['activity'], goal=row['goal'], allergies=row['allergies'],
                    favorite_foods=row['favorite_foods'], disliked_foods=row['disliked_foods'],
                    sport_types=row['sport_types'], habits=row['habits'], utc_offset=row['utc_offset'],
                )
    return None


async def save_user(p: UserProfile):
    """UPSERT, который НЕ затирает subscribed_until и trial_used."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO users
                (user_id, language, weight, height, age, gender, activity, goal,
                 allergies, favorite_foods, disliked_foods, sport_types, habits, utc_offset)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                language=excluded.language, weight=excluded.weight, height=excluded.height,
                age=excluded.age, gender=excluded.gender, activity=excluded.activity,
                goal=excluded.goal, allergies=excluded.allergies,
                favorite_foods=excluded.favorite_foods, disliked_foods=excluded.disliked_foods,
                sport_types=excluded.sport_types, habits=excluded.habits, utc_offset=excluded.utc_offset
        """, (p.user_id, p.language, p.weight, p.height, p.age, p.gender, p.activity, p.goal,
              p.allergies, p.favorite_foods, p.disliked_foods, p.sport_types, p.habits, p.utc_offset))
        await db.commit()


async def update_subscription(user_id: int, days: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT subscribed_until FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            current = row[0] if row and row[0] else None
        base = datetime.now(timezone.utc)
        if current:
            try:
                cur_dt = datetime.fromisoformat(current)
                if cur_dt > base:
                    base = cur_dt
            except ValueError:
                pass
        new_until = base + timedelta(days=days)
        await db.execute("UPDATE users SET subscribed_until=? WHERE user_id=?",
                         (new_until.isoformat(), user_id))
        await db.commit()


async def subscription_state(user_id: int):
    """Возвращает (status, days_left). status: 'admin' | 'active' | 'trial_available' | 'none'."""
    if user_id == ADMIN_ID:
        return "admin", None
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT subscribed_until, trial_used FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        return "none", None
    sub_until, trial_used = row
    if sub_until:
        try:
            dt = datetime.fromisoformat(sub_until)
            if dt > datetime.now(timezone.utc):
                days_left = max(1, (dt - datetime.now(timezone.utc)).days + 1)
                return "active", days_left
        except ValueError:
            pass
    if not trial_used:
        return "trial_available", None
    return "none", None


async def is_subscribed(user_id: int) -> bool:
    status, _ = await subscription_state(user_id)
    return status in ("admin", "active", "trial_available")


async def activate_trial(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        trial_end = datetime.now(timezone.utc) + timedelta(days=3)
        await db.execute("UPDATE users SET subscribed_until=?, trial_used=1 WHERE user_id=?",
                         (trial_end.isoformat(), user_id))
        await db.commit()


async def add_food_entry(user_id, date, meal_time, description, calories, protein, fat, carbs, photo_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""INSERT INTO food_diary
            (user_id, date, meal_time, description, calories, protein, fat, carbs, photo_id)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (user_id, date, meal_time, description, calories, protein, fat, carbs, photo_id))
        await db.commit()


async def delete_food_entry(entry_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("DELETE FROM food_diary WHERE id=? AND user_id=?", (entry_id, user_id))
        await db.commit()
        return cur.rowcount > 0


async def get_daily_food(user_id: int, date: str) -> List[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM food_diary WHERE user_id=? AND date=? ORDER BY meal_time", (user_id, date)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_recent_food(user_id: int, since_date: str) -> List[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT date, calories FROM food_diary WHERE user_id=? AND date>=?", (user_id, since_date)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_user_reminders(user_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM reminders WHERE user_id=? AND active=1 ORDER BY time", (user_id,)
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def delete_reminder(reminder_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("DELETE FROM reminders WHERE id=? AND user_id=?", (reminder_id, user_id))
        await db.commit()
        return cur.rowcount > 0


# --- Расчёты ---
def calculate_daily_calories(profile: UserProfile) -> float:
    if profile.gender == "male":
        bmr = 10 * profile.weight + 6.25 * profile.height - 5 * profile.age + 5
    else:
        bmr = 10 * profile.weight + 6.25 * profile.height - 5 * profile.age - 161
    mult = {"sedentary": 1.2, "light": 1.375, "moderate": 1.55, "active": 1.725, "very_active": 1.9}
    tdee = bmr * mult.get(profile.activity, 1.2)
    if profile.goal == "lose":
        return max(tdee * 0.8, bmr * 1.2)
    if profile.goal == "gain":
        return min(tdee * 1.2, tdee * 1.5)
    return tdee


def get_macronutrient_targets(profile: UserProfile) -> Dict[str, float]:
    w = profile.weight
    if profile.goal == "lose":
        protein, fat = 2.0 * w, 0.8 * w
    elif profile.goal == "gain":
        protein, fat = 2.2 * w, 1.0 * w
    else:
        protein, fat = 1.6 * w, 0.8 * w
    cal_target = calculate_daily_calories(profile)
    carbs = max(0, cal_target - protein * 4 - fat * 9) / 4
    return {"protein": round(protein, 1), "fat": round(fat, 1), "carbs": round(carbs, 1)}


def user_now(profile: Optional[UserProfile]) -> datetime:
    off = profile.utc_offset if profile else 3
    return datetime.now(timezone.utc) + timedelta(hours=off)


def num(x, default=0.0) -> float:
    """Безопасно достаём число из ответа ИИ ('30g' -> 30)."""
    if isinstance(x, (int, float)):
        return float(x)
    m = re.search(r'-?\d+(?:[.,]\d+)?', str(x))
    return float(m.group().replace(",", ".")) if m else default


def progress_bar(consumed: float, norm: float, width: int = 12) -> str:
    if norm <= 0:
        return ""
    ratio = max(0.0, min(consumed / norm, 1.0))
    filled = round(ratio * width)
    return "▓" * filled + "░" * (width - filled)


def sparkline(values: List[float]) -> str:
    blocks = "▁▂▃▄▅▆▇█"
    if not values:
        return ""
    lo, hi = min(values), max(values)
    if hi == lo:
        return blocks[3] * len(values)
    return "".join(blocks[int((v - lo) / (hi - lo) * (len(blocks) - 1))] for v in values)


def render_weight_png(history: List[tuple], title: str) -> Optional[bytes]:
    """График веса картинкой через matplotlib. None, если matplotlib недоступен."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from datetime import datetime as _dt
        dates = [_dt.strptime(d, "%Y-%m-%d") for d, _ in history]
        weights = [w for _, w in history]
        fig, ax = plt.subplots(figsize=(7, 4), dpi=110)
        ax.plot(dates, weights, marker="o", linewidth=2.2, color="#43a047",
                markerfacecolor="#2e7d32")
        ax.fill_between(dates, weights, min(weights), alpha=0.12, color="#43a047")
        ax.set_title(title, fontsize=13)
        ax.grid(True, alpha=0.3)
        ax.set_ylabel("кг / kg")
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        fig.autofmt_xdate()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        logging.warning("render_weight_png недоступен (%s) — будет текстовый график", e)
        return None


# --- Локализация (тёплый, дружелюбный тон) ---
LOCALE = {
    "ru": {
        "welcome": "Привет! 👋 Я твой личный AI-нутрициолог. Будем вместе следить за питанием — это правда несложно.\nДля начала выбери язык / Choose language:",
        "onboarding": {
            "weight": "Отлично, давай знакомиться! ⚖️ Напиши свой текущий вес в кг (например, 72):",
            "height": "Принято! 📏 Какой у тебя рост в см?",
            "age": "Супер. 🎂 Сколько тебе полных лет?",
            "gender": "Укажи свой пол:",
            "activity": "Насколько ты активен в течение дня?",
            "goal": "И главное — какая у тебя цель?",
            "allergies": "Почти готово! ⚠️ Есть пищевые аллергии? Перечисли через запятую или напиши «нет»:",
        },
        "gender": {"male": "Мужской", "female": "Женский"},
        "activity": {"sedentary": "Сидячий", "light": "Лёгкая", "moderate": "Умеренная", "active": "Высокая"},
        "goal": {"lose": "Похудеть", "gain": "Набрать массу", "maintain": "Поддерживать форму"},
        "profile_created": "🎉 Готово, профиль создан!\nЦель: {goal}\nТвоя дневная норма: {calories:.0f} ккал\nБелки: {protein} г · Жиры: {fat} г · Углеводы: {carbs} г\n\n🎁 Дарю тебе 3 дня бесплатного доступа. Просто пришли мне фото тарелки или описание блюда — и я всё посчитаю!",
        "profile_created_admin": "🎉 Профиль создан!\nЦель: {goal}\nДневная норма: {calories:.0f} ккал\nБелки: {protein} г · Жиры: {fat} г · Углеводы: {carbs} г\n\n♾️ Безлимитный доступ активен.",
        "returning": "С возвращением! 🙌 Рад тебя снова видеть.\nЦель: {goal} · норма: {calories:.0f} ккал\nБ: {protein} г · Ж: {fat} г · У: {carbs} г",
        "subscription_expired": "🌟 Твой доступ закончился. Чтобы я и дальше помогал считать калории и составлять меню, оформи подписку:",
        "premium_only": "🌟 <b>Это функция по подписке</b>\n\nБесплатно навсегда: 📒 дневник еды (фото/голос/текст), 💧 вода, ⚖️ вес и напоминания.\n\nПо подписке открываются: 📅 планы питания, 🧊 рецепты из холодильника, 🛒 списки покупок и 💬 вопросы нутрициологу.\n\n👥 Кстати, можно получить дни бесплатно — пригласи друга!",
        "buy_subscription_btn": "💳 Оформить подписку (30 дней · 10⭐)",
        "food_diary_menu": "📒 Дневник питания",
        "add_food_btn": "➕ Добавить блюдо",
        "show_today_btn": "📋 Что я ел сегодня",
        "week_stats_btn": "📈 Статистика за неделю",
        "quick_add_btn": "🔁 Быстро добавить",
        "quick_menu_title": "🔁 <b>Быстрое добавление</b>\nВыбери блюдо — добавлю его в сегодняшний дневник одним тапом. ⭐ — избранное.",
        "quick_empty": "Здесь появятся твои частые и избранные блюда, как только наберётся история. Добавь пару блюд — и сюда можно будет возвращаться в один тап! 🍽",
        "repeat_yesterday_btn": "📋 Повторить вчерашний день",
        "fav_add_btn": "⭐ Добавить + в избранное",
        "fav_added": "⭐ Добавлено и сохранено в избранное!",
        "fav_exists": "⭐ Уже в избранном — просто добавил в дневник.",
        "fav_removed": "Убрано из избранного.",
        "yesterday_copied": "✅ Перенёс вчерашние блюда в сегодня ({n} шт.)!",
        "yesterday_empty": "За вчера записей нет — нечего повторять 🙂",
        "add_reminder_btn": "➕ Новое напоминание",
        "no_entries": "Сегодня ты ещё ничего не добавлял. Пришли фото или описание блюда — начнём! 🍽",
        "food_added": "✅ Добавил: {food}",
        "food_cancelled": "Хорошо, не добавляю. 👌",
        "today_status": "📊 <b>Сегодня съедено: {cal:.0f} из {norm:.0f} ккал</b>\n{bar}\nБ: {p:.0f}/{tp:.0f} г · Ж: {f:.0f}/{tf:.0f} г · У: {c:.0f}/{tc:.0f} г\n{tip}",
        "tip_left": "💪 Осталось ещё ~{left:.0f} ккал. Так держать!",
        "tip_close": "🎯 Ты почти у цели на сегодня — отличная работа!",
        "tip_over": "🙂 Норма на сегодня уже набрана. Завтра новый день — будем балансировать.",
        "profile": "👤 Профиль",
        "meal_plan": "📅 План питания",
        "water_menu_btn": "💧 Вода",
        "weight_menu_btn": "⚖️ Вес",
        "reminders": "⏰ Напоминания",
        "subscription": "💳 Подписка",
        "water_title": "💧 <b>Вода сегодня: {ml} / {goal} мл</b>\n{bar}\n{tip}",
        "water_tip_done": "🎉 Дневная норма воды выполнена. Молодец!",
        "water_tip_left": "Осталось ещё {left} мл — не забывай пить 💧",
        "water_add_glass": "➕ Стакан (250)",
        "water_add_bottle": "➕ 0.5 л",
        "water_remove": "➖ Убрать стакан",
        "weight_title": "⚖️ <b>Текущий вес: {weight} кг</b>\n{history}",
        "weight_history_line": "{spark}\n{first:.1f} → {last:.1f} кг ({change:+.1f} кг за {days} дн.)",
        "weight_no_history": "Записей пока нет. Нажми «✍️ Записать вес» и я начну строить твой график 📈",
        "weight_log_btn": "✍️ Записать вес",
        "weight_chart_btn": "📈 График",
        "weight_enter": "Введи свой вес в кг (например, 71.5):",
        "weight_saved": "✅ Вес {weight} кг записан! 🔥 Новая норма: {calories:.0f} ккал",
        "weight_chart_caption": "📈 Динамика веса",
        "weight_chart_need_more": "Нужно хотя бы 2 записи для графика. Записывай вес регулярно — и тренд появится! 📊",
        "help": "🍎 <b>Что я умею:</b>\n\n• Пришли <b>фото тарелки</b>, <b>голосовое</b> («съел тарелку борща») или текст («200 г куриной грудки») — посчитаю калории и БЖУ.\n• Задай вопрос («что приготовить на ужин?») — отвечу как нутрициолог.\n• 📒 Дневник — смотри, что съел за день и сколько осталось.\n• 💧 Вода — отмечай выпитое в один тап.\n• ⚖️ Вес — записывай вес и следи за динамикой на графике.\n• 📅 План — составлю меню на 1–2 дня под твою цель, соберу 🛒 список покупок, а из содержимого холодильника подскажу рецепт.\n• ⏰ Напоминания — напишу «попить воды» или «принять витамины». Формат: <code>09:00 текст</code>.\n• 👥 Пригласи друга через /invite — оба получите бонусные дни доступа!\n\nПросто пиши или говори мне в любой момент — я рядом! 🤝",
        "send_food_prompt": "📷 Пришли фото блюда или опиши его текстом.\nЕсли укажешь количество (например, «200 г риса» или «2 яйца») — посчитаю именно на это. Иначе — на 100 г.",
        "food_analyzing": "🔍 Минутку, изучаю твоё блюдо...",
        "voice_analyzing": "🎤 Слушаю и считаю...",
        "voice_heard": "🎤 Услышал: «{text}»",
        "invite_btn": "👥 Пригласить друга",
        "invite_title": "👥 <b>Приглашай друзей — получай дни доступа!</b>\n\nЗа каждого друга, который запустит бота по твоей ссылке и создаст профиль, <b>вы оба получаете +{ref} дней</b> подписки. 🎁\n\nТвоя ссылка:\n{link}\n\n📊 Уже пригласил: <b>{count}</b>",
        "invite_no_username": "Реферальные ссылки временно недоступны, попробуй позже 🙏",
        "ref_inviter_reward": "🎉 По твоей ссылке зашёл новый друг! Тебе начислено +{ref} дней доступа. Спасибо! 💛",
        "ref_new_reward": "🎁 Ты пришёл по приглашению — держи бонус +{ref} дней доступа сверху!",
        "create_profile_first": "Сначала давай создадим профиль — нажми /start 🙂",
        "profile_not_found": "Профиль не найден. Нажми /start, чтобы начать.",
        "error_try_again": "Что-то пошло не так, попробуй ещё раз.",
        "entry_deleted": "🗑 Удалил запись.",
        "edit_field": {
            "weight": "Вес", "height": "Рост", "age": "Возраст", "gender": "Пол",
            "activity": "Активность", "goal": "Цель", "allergies": "Аллергии",
            "favorite_foods": "Любимая еда", "disliked_foods": "Нелюбимая еда",
            "sport_types": "Спорт", "habits": "Привычки", "language": "Язык", "clear": "🗑 Очистить"
        },
        "field_updated": "✅ Готово, «{field}» обновлено!",
        "field_updated_with_norm": "✅ «{field}» обновлено! Новая норма: {calories:.0f} ккал",
        "field_cleared": "✅ Поле «{field}» очищено.",
        "current_value": "Сейчас: {value}",
        "plan_choose_days": "На сколько дней составить меню?",
        "plan_1_day": "🍽 План на 1 день",
        "plan_2_days": "🍽 План на 2 дня",
        "fridge_btn": "🧊 Рецепт из холодильника",
        "fridge_prompt": "🧊 Напиши, что есть в холодильнике (через запятую). Например: яйца, помидоры, сыр, куриная грудка, рис.",
        "fridge_generating": "🧑‍🍳 Придумываю, что из этого приготовить...",
        "shopping_btn": "🛒 Список покупок",
        "shopping_title": "🛒 <b>Список покупок</b>\n{items}",
        "shopping_empty": "Сначала составь план питания — и я соберу из него список покупок 🛒",
        "plan_generating": "⏳ Готовлю для тебя вкусное меню...",
        "nutritionist_generating": "⏳ Думаю над ответом...",
        "reminders_empty": "У тебя пока нет напоминаний.\nЧтобы добавить, просто напиши время и текст, например:\n<code>09:00 Выпить воды</code> 💧",
        "reminders_list": "⏰ <b>Твои напоминания:</b>\n{items}\n\nЧтобы удалить — нажми на кнопку ниже.",
        "reminder_add_info": "Напиши напоминание в формате: <code>ЧЧ:ММ текст</code>\nНапример: 09:00 Выпить витамины · 13:30 Обед · 7.30 Завтрак",
        "reminder_added": "⏰ Запомнил! В {time} напомню: «{text}»",
        "reminder_deleted": "🗑 Напоминание удалено.",
        "week_stats_title": "📈 <b>Твоя неделя:</b>\n{lines}\n\n📊 В среднем: <b>{avg:.0f} ккал/день</b> (норма {norm:.0f})\n{note}",
        "week_note_good": "👏 Ты хорошо держишь баланс!",
        "week_note_low": "🍳 В среднем недобираешь — добавь полноценных приёмов пищи.",
        "week_note_high": "🙂 Немного выше нормы. Чуть больше движения или поменьше порции — и идеально.",
        "no_week_data": "За последнюю неделю записей пока нет. Начни добавлять блюда — и тут появится статистика! 📊",
        "payment_title": "Подписка на AI-нутрициолога",
        "payment_desc": "30 дней доступа к личному диетологу за 10 Telegram Stars",
        "payment_success": "✅ Оплата прошла, спасибо за доверие! 💛 Подписка продлена на 30 дней.",
        "gift_success": "Пользователю {id} подарено {days} дн. подписки.",
        "gift_error": "Ошибка. Используй: /gift ID ДНИ",
        "not_food_danger": "⚠️ Это несъедобно или опасно! Если ты или кто-то рядом это проглотил — срочно обратись к врачу или вызови скорую (103).",
        "not_food": "Хм, я не распознал тут еду. 🤔 Попробуй другое описание или фото получше.",
        "analysis_failed": "❌ Не получилось распознать блюдо. Попробуй ещё раз или опиши поточнее.",
        "error_generic": "Упс, произошла ошибка. Попробуй чуть позже 🙏",
        "no_keys": "⚠️ ИИ временно недоступен (не заданы ключи). Сообщи администратору.",
        "daily_summary": "🌙 <b>Итоги дня ({date})</b>\nСъедено: {consumed:.0f} из {norm:.0f} ккал\n{bar}\nБ: {p:.0f} г · Ж: {f:.0f} г · У: {c:.0f} г\n\n{advice}",
        "daily_summary_ok": "✅ Ты отлично уложился в норму сегодня! Спокойной ночи 🌙",
        "daily_summary_over": "🙂 Сегодня немного перебор. Ничего страшного — завтра сбалансируем!",
        "daily_summary_under": "🍽 Сегодня калорий маловато. Завтра постарайся поесть посытнее.",
        "subscription_menu_active": "✅ Подписка активна ещё {days} дн. Спасибо, что ты с нами! 💛",
        "subscription_menu": "Выбери действие:",
        "confirm_add": "✅ Добавить",
        "cancel_food": "❌ Отмена",
        "kcal_unit": "ккал",
        "meal_plan_labels": {
            "recommended": "Рекомендуемая норма", "day": "День", "breakfast": "Завтрак",
            "lunch": "Обед", "dinner": "Ужин", "snacks": "Перекус",
            "daily_total": "Итого за день", "macros": "БЖУ", "preparation": "Приготовление"
        }
    },
    "en": {
        "welcome": "Hi there! 👋 I'm your personal AI nutritionist. Let's take care of your nutrition together — it's easier than it sounds.\nFirst, choose your language / Выбери язык:",
        "onboarding": {
            "weight": "Great, let's get to know each other! ⚖️ What's your current weight in kg (e.g. 72)?",
            "height": "Got it! 📏 What's your height in cm?",
            "age": "Awesome. 🎂 How old are you (full years)?",
            "gender": "Please pick your gender:",
            "activity": "How active are you during the day?",
            "goal": "And most importantly — what's your goal?",
            "allergies": "Almost done! ⚠️ Any food allergies? List them with commas or type 'none':",
        },
        "gender": {"male": "Male", "female": "Female"},
        "activity": {"sedentary": "Sedentary", "light": "Light", "moderate": "Moderate", "active": "Active"},
        "goal": {"lose": "Lose weight", "gain": "Gain mass", "maintain": "Stay in shape"},
        "profile_created": "🎉 All set, your profile is ready!\nGoal: {goal}\nYour daily norm: {calories:.0f} kcal\nProtein: {protein} g · Fat: {fat} g · Carbs: {carbs} g\n\n🎁 Here's a 3-day free trial. Just send me a photo of your plate or a description — I'll do the math!",
        "profile_created_admin": "🎉 Profile created!\nGoal: {goal}\nDaily norm: {calories:.0f} kcal\nProtein: {protein} g · Fat: {fat} g · Carbs: {carbs} g\n\n♾️ Unlimited access enabled.",
        "returning": "Welcome back! 🙌 Great to see you again.\nGoal: {goal} · norm: {calories:.0f} kcal\nP: {protein} g · F: {fat} g · C: {carbs} g",
        "subscription_expired": "🌟 Your access has ended. To keep counting calories and building menus together, grab a subscription:",
        "premium_only": "🌟 <b>This is a subscription feature</b>\n\nAlways free: 📒 food diary (photo/voice/text), 💧 water, ⚖️ weight and reminders.\n\nSubscription unlocks: 📅 meal plans, 🧊 fridge recipes, 🛒 shopping lists and 💬 nutritionist questions.\n\n👥 By the way, you can earn free days — invite a friend!",
        "buy_subscription_btn": "💳 Get subscription (30 days · 10⭐)",
        "food_diary_menu": "📒 Food Diary",
        "add_food_btn": "➕ Add meal",
        "show_today_btn": "📋 What I ate today",
        "week_stats_btn": "📈 Weekly stats",
        "quick_add_btn": "🔁 Quick add",
        "quick_menu_title": "🔁 <b>Quick add</b>\nPick a meal — I'll log it to today in one tap. ⭐ means favorite.",
        "quick_empty": "Your frequent and favorite meals will appear here once you build some history. Log a couple of meals and you'll be able to re-add them in one tap! 🍽",
        "repeat_yesterday_btn": "📋 Repeat yesterday",
        "fav_add_btn": "⭐ Add + favorite",
        "fav_added": "⭐ Added and saved to favorites!",
        "fav_exists": "⭐ Already in favorites — just logged it.",
        "fav_removed": "Removed from favorites.",
        "yesterday_copied": "✅ Copied yesterday's meals to today ({n})!",
        "yesterday_empty": "No entries for yesterday — nothing to repeat 🙂",
        "add_reminder_btn": "➕ New reminder",
        "no_entries": "You haven't added anything today yet. Send a photo or describe a meal — let's start! 🍽",
        "food_added": "✅ Added: {food}",
        "food_cancelled": "No problem, not adding it. 👌",
        "today_status": "📊 <b>Eaten today: {cal:.0f} of {norm:.0f} kcal</b>\n{bar}\nP: {p:.0f}/{tp:.0f} g · F: {f:.0f}/{tf:.0f} g · C: {c:.0f}/{tc:.0f} g\n{tip}",
        "tip_left": "💪 About {left:.0f} kcal left. Keep it up!",
        "tip_close": "🎯 You're almost at today's goal — nice work!",
        "tip_over": "🙂 You've hit today's norm. Tomorrow's a fresh start — we'll balance it.",
        "profile": "👤 Profile",
        "meal_plan": "📅 Meal Plan",
        "water_menu_btn": "💧 Water",
        "weight_menu_btn": "⚖️ Weight",
        "reminders": "⏰ Reminders",
        "subscription": "💳 Subscription",
        "water_title": "💧 <b>Water today: {ml} / {goal} ml</b>\n{bar}\n{tip}",
        "water_tip_done": "🎉 Daily water goal reached. Well done!",
        "water_tip_left": "{left} ml to go — keep sipping 💧",
        "water_add_glass": "➕ Glass (250)",
        "water_add_bottle": "➕ 0.5 L",
        "water_remove": "➖ Remove a glass",
        "weight_title": "⚖️ <b>Current weight: {weight} kg</b>\n{history}",
        "weight_history_line": "{spark}\n{first:.1f} → {last:.1f} kg ({change:+.1f} kg over {days} d)",
        "weight_no_history": "No entries yet. Tap '✍️ Log weight' and I'll start charting your progress 📈",
        "weight_log_btn": "✍️ Log weight",
        "weight_chart_btn": "📈 Chart",
        "weight_enter": "Enter your weight in kg (e.g. 71.5):",
        "weight_saved": "✅ Weight {weight} kg saved! 🔥 New norm: {calories:.0f} kcal",
        "weight_chart_caption": "📈 Weight trend",
        "weight_chart_need_more": "Need at least 2 entries for a chart. Log your weight regularly and the trend will appear! 📊",
        "help": "🍎 <b>What I can do:</b>\n\n• Send a <b>photo of your plate</b>, a <b>voice message</b> ('I ate a bowl of soup') or text ('200g chicken breast') — I'll count calories and macros.\n• Ask me a question ('what to cook for dinner?') — I'll answer like a nutritionist.\n• 📒 Diary — see what you ate and what's left for the day.\n• 💧 Water — log what you drink in one tap.\n• ⚖️ Weight — log your weight and track the trend on a chart.\n• 📅 Plan — a 1–2 day menu for your goal, a 🛒 shopping list, and recipe ideas from what's in your fridge.\n• ⏰ Reminders — 'drink water' or 'take vitamins'. Format: <code>09:00 text</code>.\n• 👥 Invite a friend via /invite — you both get bonus access days!\n\nJust type or speak to me anytime — I'm here! 🤝",
        "send_food_prompt": "📷 Send a photo of your meal or describe it.\nIf you mention an amount (e.g. '200g rice' or '2 eggs') I'll count for that. Otherwise — per 100 g.",
        "food_analyzing": "🔍 One sec, analysing your meal...",
        "voice_analyzing": "🎤 Listening and counting...",
        "voice_heard": "🎤 Heard: '{text}'",
        "invite_btn": "👥 Invite a friend",
        "invite_title": "👥 <b>Invite friends — earn access days!</b>\n\nFor every friend who starts the bot via your link and creates a profile, <b>you both get +{ref} days</b> of subscription. 🎁\n\nYour link:\n{link}\n\n📊 Invited so far: <b>{count}</b>",
        "invite_no_username": "Referral links are temporarily unavailable, try later 🙏",
        "ref_inviter_reward": "🎉 A new friend joined via your link! You earned +{ref} days of access. Thank you! 💛",
        "ref_new_reward": "🎁 You joined via an invite — here's a bonus +{ref} days of access!",
        "create_profile_first": "Let's create a profile first — tap /start 🙂",
        "profile_not_found": "Profile not found. Tap /start to begin.",
        "error_try_again": "Something went wrong, please try again.",
        "entry_deleted": "🗑 Entry deleted.",
        "edit_field": {
            "weight": "Weight", "height": "Height", "age": "Age", "gender": "Gender",
            "activity": "Activity", "goal": "Goal", "allergies": "Allergies",
            "favorite_foods": "Favorite foods", "disliked_foods": "Disliked foods",
            "sport_types": "Sports", "habits": "Habits", "language": "Language", "clear": "🗑 Clear"
        },
        "field_updated": "✅ Done, '{field}' updated!",
        "field_updated_with_norm": "✅ '{field}' updated! New norm: {calories:.0f} kcal",
        "field_cleared": "✅ '{field}' cleared.",
        "current_value": "Now: {value}",
        "plan_choose_days": "How many days should the menu cover?",
        "plan_1_day": "🍽 1-day plan",
        "plan_2_days": "🍽 2-day plan",
        "fridge_btn": "🧊 Recipe from fridge",
        "fridge_prompt": "🧊 List what's in your fridge (comma-separated). E.g.: eggs, tomatoes, cheese, chicken breast, rice.",
        "fridge_generating": "🧑‍🍳 Figuring out what to cook with that...",
        "shopping_btn": "🛒 Shopping list",
        "shopping_title": "🛒 <b>Shopping list</b>\n{items}",
        "shopping_empty": "Generate a meal plan first — then I'll build a shopping list from it 🛒",
        "plan_generating": "⏳ Cooking up a tasty menu for you...",
        "nutritionist_generating": "⏳ Thinking it over...",
        "reminders_empty": "You have no reminders yet.\nTo add one, just write time and text, e.g.:\n<code>09:00 Drink water</code> 💧",
        "reminders_list": "⏰ <b>Your reminders:</b>\n{items}\n\nTap a button below to delete.",
        "reminder_add_info": "Write a reminder as: <code>HH:MM text</code>\nE.g.: 09:00 Take vitamins · 13:30 Lunch · 7.30 Breakfast",
        "reminder_added": "⏰ Got it! At {time} I'll remind you: '{text}'",
        "reminder_deleted": "🗑 Reminder deleted.",
        "week_stats_title": "📈 <b>Your week:</b>\n{lines}\n\n📊 Average: <b>{avg:.0f} kcal/day</b> (norm {norm:.0f})\n{note}",
        "week_note_good": "👏 You're keeping a great balance!",
        "week_note_low": "🍳 You're under on average — add some fuller meals.",
        "week_note_high": "🙂 A bit above norm. A little more movement or smaller portions and you're set.",
        "no_week_data": "No entries in the past week yet. Start logging meals and stats will show up here! 📊",
        "payment_title": "AI Nutritionist Subscription",
        "payment_desc": "30 days of access to a personal dietitian for 10 Telegram Stars",
        "payment_success": "✅ Payment received, thank you for trusting me! 💛 Subscription extended by 30 days.",
        "gift_success": "User {id} received {days} days of subscription.",
        "gift_error": "Error. Use: /gift ID DAYS",
        "not_food_danger": "⚠️ This is inedible or dangerous! If you or someone nearby swallowed it — seek medical help or call emergency services immediately.",
        "not_food": "Hmm, I don't see food here. 🤔 Try another description or a clearer photo.",
        "analysis_failed": "❌ Couldn't recognise the meal. Try again or describe it more precisely.",
        "error_generic": "Oops, an error occurred. Please try again later 🙏",
        "no_keys": "⚠️ AI is temporarily unavailable (no keys configured). Please tell the admin.",
        "daily_summary": "🌙 <b>Daily summary ({date})</b>\nEaten: {consumed:.0f} of {norm:.0f} kcal\n{bar}\nP: {p:.0f} g · F: {f:.0f} g · C: {c:.0f} g\n\n{advice}",
        "daily_summary_ok": "✅ You nailed your norm today! Good night 🌙",
        "daily_summary_over": "🙂 A bit over today. No worries — we'll balance it tomorrow!",
        "daily_summary_under": "🍽 A little low on calories today. Try to eat a bit more tomorrow.",
        "subscription_menu_active": "✅ Subscription active for {days} more days. Thanks for being here! 💛",
        "subscription_menu": "Choose an action:",
        "confirm_add": "✅ Add",
        "cancel_food": "❌ Cancel",
        "kcal_unit": "kcal",
        "meal_plan_labels": {
            "recommended": "Recommended intake", "day": "Day", "breakfast": "Breakfast",
            "lunch": "Lunch", "dinner": "Dinner", "snacks": "Snacks",
            "daily_total": "Total for the day", "macros": "Macros", "preparation": "Preparation"
        }
    }
}


def t(lang: str, key: str, **kwargs) -> str:
    val = LOCALE.get(lang, LOCALE["en"])
    for part in key.split("."):
        if isinstance(val, dict):
            val = val.get(part)
        else:
            break
        if val is None:
            return key
    if isinstance(val, str):
        return val.format(**kwargs) if kwargs else val
    return val if val is not None else key


# --- Безопасная отправка (HTML с фолбэком на чистый текст) ---
def esc(s) -> str:
    return html.escape(str(s))


async def answer_html(message: Message, text: str, **kw):
    try:
        return await message.answer(text, parse_mode="HTML", **kw)
    except TelegramBadRequest:
        return await message.answer(re.sub(r'<[^>]+>', '', text), **kw)


async def edit_html(message: Message, text: str, **kw):
    try:
        return await message.edit_text(text, parse_mode="HTML", **kw)
    except TelegramBadRequest as e:
        if "not modified" in str(e).lower():
            return
        try:
            return await message.edit_text(re.sub(r'<[^>]+>', '', text), **kw)
        except TelegramBadRequest:
            return


async def send_html(chat_id: int, text: str, **kw):
    try:
        return await bot.send_message(chat_id, text, parse_mode="HTML", **kw)
    except TelegramBadRequest:
        return await bot.send_message(chat_id, re.sub(r'<[^>]+>', '', text), **kw)


# --- AI ---
def extract_json(text: str) -> Optional[dict]:
    text = text.strip()
    text = re.sub(r'^```(?:json)?', '', text).strip()
    text = re.sub(r'```$', '', text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r'[\{\[].*', text, re.DOTALL)
    if m:
        s = m.group()
        s += '}' * max(0, s.count('{') - s.count('}'))
        s += ']' * max(0, s.count('[') - s.count(']'))
        s = re.sub(r',\s*([}\]])', r'\1', s)
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass
    logging.warning("Failed to parse JSON: %s", text[:200])
    return None


async def ai_call(contents, expect_json: bool = False, model: str = None,
                  max_tokens: int = 1000, use_lite: bool = False) -> dict:
    """Внутренние ошибки помечаются ключом '_error'. Полезные данные модели возвращаются как есть."""
    if not GEMINI_API_KEYS:
        return {"_error": "no_keys"}
    base = GEMINI_MODELS_LITE if use_lite else GEMINI_MODELS_SMART
    models_to_try = base if not model else [model] + base
    async with AI_SEMAPHORE:
        for key in GEMINI_API_KEYS:
            client = get_client(key)
            for m in models_to_try:
                for attempt in range(3):  # ограниченные ретраи (без бесконечного while)
                    try:
                        cfg = genai_types.GenerateContentConfig(
                            max_output_tokens=max_tokens, temperature=0.3,
                        )
                        if expect_json:
                            cfg.response_mime_type = "application/json"
                        resp = await client.aio.models.generate_content(
                            model=m, contents=contents, config=cfg,
                        )
                        text = resp.text
                        if not text:
                            raise ValueError("empty response")
                        if expect_json:
                            parsed = extract_json(text)
                            if parsed is None:
                                break  # эта модель не дала валидный JSON — пробуем следующую
                            return parsed
                        return {"text": text}
                    except APIError as e:
                        if getattr(e, "code", None) == 429:
                            await asyncio.sleep(5)
                            continue
                        logging.warning("Model %s key ...%s: %s", m, key[-4:], e)
                        break
                    except Exception as e:
                        logging.warning("Model %s key ...%s: %s", m, key[-4:], e)
                        break
    return {"_error": "all_failed"}


# --- Анализ еды ---
_FOOD_SCHEMA = (
    'Return ONLY a JSON object. If it is edible food or drink: '
    '{"is_food": true, "food_name": "...", "portion": "amount used, e.g. 200g or 1 plate", '
    '"calories": number, "protein": number, "fat": number, "carbs": number}. '
    'If it is NOT food, or is poison/dangerous: '
    '{"is_food": false, "danger": true_or_false, "message": "short note"}. '
    'Use real numbers, not strings. If a quantity is given, compute for that exact amount; '
    'otherwise compute for 100 g of the main item.'
)


async def analyze_food_text(description: str, lang: str) -> dict:
    prompt = (
        f"Analyse this meal description (user language: {lang}). " + _FOOD_SCHEMA +
        f' Description: "{description}".'
    )
    result = await ai_call(prompt, expect_json=True, max_tokens=400, use_lite=True)
    if "_error" in result:
        return {"_error": "analysis_failed"}
    return result


async def analyze_food_photo(photo_bytes: bytes, lang: str) -> dict:
    image_part = genai_types.Part.from_bytes(data=photo_bytes, mime_type="image/jpeg")
    prompt = (
        f"Identify what is in this photo (user language: {lang}). " + _FOOD_SCHEMA +
        " Estimate the portion from what is visible."
    )
    result = await ai_call([prompt, image_part], expect_json=True, max_tokens=400, use_lite=True)
    if "_error" in result:
        return {"_error": "analysis_failed"}
    return result


async def analyze_food_audio(audio_bytes: bytes, mime_type: str, lang: str) -> dict:
    """Распознаёт еду из голосового сообщения. Gemini сам транскрибирует аудио."""
    audio_part = genai_types.Part.from_bytes(data=audio_bytes, mime_type=mime_type)
    prompt = (
        f"The user sent a voice message describing what they ate (user language: {lang}). "
        "First transcribe it, then analyse the meal. " + _FOOD_SCHEMA +
        ' Also add a field "heard" containing the transcription of what the user said.'
    )
    result = await ai_call([prompt, audio_part], expect_json=True, max_tokens=500, use_lite=True)
    if "_error" in result:
        return {"_error": "analysis_failed"}
    return result


# --- Планы питания (1 и 2 дня) ---
def normalize_plan(data) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        plan = data.get("plan", data)
        if isinstance(plan, list):
            return plan
        if isinstance(plan, dict):
            days = []
            for k in sorted(plan.keys()):
                if k.startswith("day"):
                    entry = plan[k]
                    if isinstance(entry, dict):
                        entry.setdefault("day", len(days) + 1)
                        days.append(entry)
            if days:
                return days
            for v in plan.values():
                if isinstance(v, list):
                    return v
    return []


def format_meal_plan(plan_data: list, daily_cal: float, lang: str) -> str:
    labels = t(lang, "meal_plan_labels")
    if not isinstance(labels, dict):
        labels = LOCALE["en"]["meal_plan_labels"]
    unit = t(lang, "kcal_unit")
    lines = [f"🍽 <b>{esc(labels['recommended'])}: {daily_cal:.0f} {esc(unit)}</b>"]
    for day in plan_data:
        day_num = day.get("day", "?")
        lines.append(f"\n📅 <b>{esc(labels['day'])} {esc(day_num)}</b>")
        total = 0.0
        for mk in ["breakfast", "lunch", "dinner", "snacks"]:
            meal = day.get(mk)
            if not isinstance(meal, dict):
                continue
            name = meal.get("meal_name", mk)
            cal = num(meal.get("calories", 0))
            total += cal
            macros = meal.get("macros", {}) if isinstance(meal.get("macros"), dict) else {}
            p = macros.get("proteins", macros.get("protein", "—"))
            f = macros.get("fats", macros.get("fat", "—"))
            c = macros.get("carbohydrates", macros.get("carbs", "—"))
            lines.append(f"  • <b>{esc(labels[mk])}</b>: {esc(name)} ({cal:.0f} {esc(unit)})")
            ingredients = meal.get("ingredients", [])
            if isinstance(ingredients, list) and ingredients:
                lines.append(f"    🛒 <i>{esc(', '.join(str(i) for i in ingredients))}</i>")
            prep = meal.get("preparation", "")
            if prep:
                lines.append(f"    🥣 <b>{esc(labels['preparation'])}</b>: {esc(prep)}")
            lines.append(f"    {esc(labels['macros'])}: {esc(p)} · {esc(f)} · {esc(c)}")
        lines.append(f"  📊 <b>{esc(labels['daily_total'])}: {total:.0f} {esc(unit)}</b>")
    return "\n".join(lines)


async def generate_meal_plan(profile: UserProfile, days: int = 1):
    """Возвращает (text, plan_data). При ошибке plan_data = None."""
    days = days if days in (1, 2) else 1
    daily_cal = calculate_daily_calories(profile)
    macros = get_macronutrient_targets(profile)
    prompt = (
        f"Create a {days}-day meal plan. Goal={profile.goal}, ~{daily_cal:.0f} kcal/day, "
        f"protein ~{macros['protein']}g, fat ~{macros['fat']}g, carbs ~{macros['carbs']}g. "
        f"Allergies={profile.allergies or 'none'}, likes={profile.favorite_foods or 'any'}, "
        f"dislikes={profile.disliked_foods or 'none'}, sports={profile.sport_types or 'none'}. "
        f"Write all text fields in language: {profile.language}. Use affordable, common ingredients. "
        "Each meal: list of ingredients and a 1-2 sentence preparation. "
        "Return ONLY JSON: "
        '{"plan":[{"day":1,'
        '"breakfast":{"meal_name":"...","ingredients":["..."],"calories":0,"preparation":"...","macros":{"proteins":"30g","fats":"10g","carbohydrates":"40g"}},'
        '"lunch":{...},"dinner":{...},"snacks":{...}}]}'
    )
    result = await ai_call(prompt, expect_json=True, max_tokens=3000)
    if "_error" in result:
        return t(profile.language, "no_keys" if result["_error"] == "no_keys" else "error_generic"), None
    plan_data = normalize_plan(result)
    if not plan_data:
        return t(profile.language, "error_generic"), None
    return format_meal_plan(plan_data, daily_cal, profile.language), plan_data


def build_shopping_list(plan_data: list, lang: str) -> str:
    """Собирает все ингредиенты из плана в один список без дублей."""
    items, seen = [], set()
    for day in plan_data:
        for mk in ("breakfast", "lunch", "dinner", "snacks"):
            meal = day.get(mk)
            if not isinstance(meal, dict):
                continue
            for ing in meal.get("ingredients", []) or []:
                key = str(ing).strip().lower()
                if key and key not in seen:
                    seen.add(key)
                    items.append(str(ing).strip())
    lines = "\n".join(f"☑️ {esc(i)}" for i in items)
    return t(lang, "shopping_title", items=lines)


async def fridge_recipe(items: str, profile: UserProfile, lang: str) -> str:
    macros = get_macronutrient_targets(profile)
    prompt = (
        "You are a friendly cook and nutritionist. Suggest 1-2 simple recipes the user can make "
        f"mostly from these ingredients: {items}. Basic staples (salt, oil, water, spices) are assumed available. "
        f"Respect the user's goal={profile.goal}, ~{calculate_daily_calories(profile):.0f} kcal/day, "
        f"allergies={profile.allergies or 'none'}, dislikes={profile.disliked_foods or 'none'}. "
        "For each recipe give: a short name, the ingredients used, brief steps, and approximate calories per serving. "
        f"Answer in {lang}, plain text only (no markdown symbols like * or #). Keep it concise and practical."
    )
    result = await ai_call(prompt, expect_json=False, max_tokens=900)
    if "_error" in result:
        return t(lang, "no_keys" if result["_error"] == "no_keys" else "error_generic")
    return result.get("text", t(lang, "error_generic"))


# --- Вопросы нутрициологу ---
def is_nutrition_question(text: str) -> bool:
    tl = text.lower()
    triggers = [
        "что приготовить", "что поесть", "посоветуй", "как приготовить", "рецепт",
        "что съесть", "подскажи", "какая еда", "чем перекусить", "что лучше",
        "как питаться", "диета", "рацион", "what to cook", "what to eat", "advice",
        "suggest", "recipe", "how to prepare", "what should i eat",
    ]
    return any(x in tl for x in triggers) or tl.strip().endswith("?")


async def ask_nutritionist(question: str, profile: UserProfile, lang: str) -> str:
    macros = get_macronutrient_targets(profile)
    prompt = (
        "You are a warm, friendly, professional nutritionist. Be encouraging and practical. "
        f"User profile: goal={profile.goal}, weight={profile.weight}kg, height={profile.height}cm, "
        f"age={profile.age}, daily target ~{calculate_daily_calories(profile):.0f} kcal, "
        f"protein {macros['protein']}g, fat {macros['fat']}g, carbs {macros['carbs']}g, "
        f"allergies={profile.allergies or 'none'}, likes={profile.favorite_foods or 'any'}, "
        f"dislikes={profile.disliked_foods or 'none'}, sports={profile.sport_types or 'none'}. "
        f"Answer in {lang}, clearly and helpfully. Use plain text only (no markdown). "
        f"Question: {question}"
    )
    result = await ai_call(prompt, expect_json=False, max_tokens=1000)
    if "_error" in result:
        return t(lang, "no_keys" if result["_error"] == "no_keys" else "error_generic")
    return result.get("text", t(lang, "error_generic"))


# --- Состояния FSM ---
class Onboarding(StatesGroup):
    language = State()
    weight = State()
    height = State()
    age = State()
    gender = State()
    activity = State()
    goal = State()
    allergies = State()


class FoodInput(StatesGroup):
    waiting_for_text_or_photo = State()
    confirm_add = State()


class EditProfile(StatesGroup):
    waiting_for_value = State()


class WeightInput(StatesGroup):
    waiting = State()


class FridgeInput(StatesGroup):
    waiting = State()


# --- Клавиатуры ---
async def build_main_menu(user_id: int, lang: str = "ru") -> ReplyKeyboardMarkup:
    sub = await is_subscribed(user_id)
    buttons = [
        [KeyboardButton(text=t(lang, "food_diary_menu"))],
        [KeyboardButton(text=t(lang, "water_menu_btn")), KeyboardButton(text=t(lang, "weight_menu_btn"))],
        [KeyboardButton(text=t(lang, "profile")), KeyboardButton(text=t(lang, "meal_plan"))],
        [KeyboardButton(text=t(lang, "reminders"))],
    ]
    if not sub:
        buttons.append([KeyboardButton(text=t(lang, "subscription"))])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def get_lang(user_id: int, state: FSMContext = None) -> str:
    profile = await get_user(user_id)
    if profile:
        return profile.language
    if state:
        data = await state.get_data()
        if data.get("language"):
            return data["language"]
    return "ru"


def food_diary_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "add_food_btn"), callback_data="add_food")],
        [InlineKeyboardButton(text=t(lang, "quick_add_btn"), callback_data="quick_add")],
        [InlineKeyboardButton(text=t(lang, "show_today_btn"), callback_data="show_today")],
        [InlineKeyboardButton(text=t(lang, "week_stats_btn"), callback_data="week_stats")],
    ])


# --- Сводка дня ---
async def today_totals(profile: UserProfile):
    today = user_now(profile).strftime("%Y-%m-%d")
    entries = await get_daily_food(profile.user_id, today)
    cal = sum(e['calories'] or 0 for e in entries)
    p = sum(e['protein'] or 0 for e in entries)
    f = sum(e['fat'] or 0 for e in entries)
    c = sum(e['carbs'] or 0 for e in entries)
    return entries, cal, p, f, c


def today_status_text(profile, lang, cal, p, f, c) -> str:
    norm = calculate_daily_calories(profile)
    m = get_macronutrient_targets(profile)
    left = norm - cal
    if left > 150:
        tip = t(lang, "tip_left", left=left)
    elif left >= -100:
        tip = t(lang, "tip_close")
    else:
        tip = t(lang, "tip_over")
    return t(lang, "today_status", cal=cal, norm=norm, bar=progress_bar(cal, norm),
             p=p, tp=m['protein'], f=f, tf=m['fat'], c=c, tc=m['carbs'], tip=tip)


async def render_today(profile: UserProfile, lang: str):
    entries, cal, p, f, c = await today_totals(profile)
    if not entries:
        return t(lang, "no_entries"), None
    unit = t(lang, "kcal_unit")
    lines = [f"🕘 {esc(e['meal_time'])} — {esc(e['description'])}: {num(e['calories']):.0f} {esc(unit)}"
             for e in entries]
    text = "\n".join(lines) + "\n\n" + today_status_text(profile, lang, cal, p, f, c)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {e['meal_time']} {e['description'][:20]}",
                              callback_data=f"del_entry_{e['id']}")]
        for e in entries
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text="🔄 " + t(lang, "show_today_btn"),
                                                    callback_data="show_today")])
    return text, kb


# --- Доступ к premium-функциям ---
# Freemium-модель: дневник еды, вода, вес, напоминания, профиль — бесплатно всегда.
# По подписке/триалу: планы питания, рецепты из холодильника, список покупок, вопросы нутрициологу.
def _buy_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "buy_subscription_btn"), callback_data="buy_subscription")],
        [InlineKeyboardButton(text=t(lang, "invite_btn"), callback_data="show_invite")],
    ])


async def ensure_premium_message(message: Message, lang: str) -> bool:
    """Возвращает True, если у пользователя есть доступ. Иначе шлёт предложение подписки."""
    if await is_subscribed(message.from_user.id):
        return True
    await answer_html(message, t(lang, "premium_only"), reply_markup=_buy_kb(lang))
    return False


async def ensure_premium_callback(callback: CallbackQuery, lang: str) -> bool:
    if await is_subscribed(callback.from_user.id):
        return True
    await callback.answer()
    await send_html(callback.from_user.id, t(lang, "premium_only"), reply_markup=_buy_kb(lang))
    return False


# --- /start /help ---
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        # Реферальный код из ссылки вида ?start=ref_12345 (учитываем только новых пользователей)
        ref_id = None
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) == 2 and parts[1].startswith("ref_"):
            try:
                candidate = int(parts[1][4:])
                if candidate != user_id:
                    ref_id = candidate
            except ValueError:
                pass
        await state.update_data(ref_id=ref_id)
        await message.answer(t("ru", "welcome"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data="lang_ru"),
             InlineKeyboardButton(text="English", callback_data="lang_en")]
        ]))
        await state.set_state(Onboarding.language)
        return
    lang = profile.language
    daily_cal = calculate_daily_calories(profile)
    macros = get_macronutrient_targets(profile)
    goal_text = t(lang, f"goal.{profile.goal}")
    msg = t(lang, "returning", goal=goal_text, calories=daily_cal,
            protein=macros['protein'], fat=macros['fat'], carbs=macros['carbs'])
    await message.answer(msg, reply_markup=await build_main_menu(user_id, lang))


@router.message(Command("help"))
async def cmd_help(message: Message, state: FSMContext):
    lang = await get_lang(message.from_user.id, state)
    await answer_html(message, t(lang, "help", ref=REFERRAL_BONUS_DAYS))


async def invite_text(user_id: int, lang: str) -> str:
    if not BOT_USERNAME:
        return t(lang, "invite_no_username")
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
    count = await count_referrals(user_id)
    return t(lang, "invite_title", ref=REFERRAL_BONUS_DAYS, link=link, count=count)


@router.message(Command("invite"))
async def cmd_invite(message: Message, state: FSMContext):
    lang = await get_lang(message.from_user.id, state)
    await answer_html(message, await invite_text(message.from_user.id, lang))


@router.callback_query(F.data == "show_invite")
async def show_invite(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    await callback.answer()
    await send_html(callback.from_user.id, await invite_text(callback.from_user.id, lang))


# --- Онбординг ---
@router.callback_query(StateFilter(Onboarding.language))
async def process_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split("_")[1]
    await state.update_data(language=lang)
    await callback.message.edit_text(t(lang, "onboarding.weight"))
    await state.set_state(Onboarding.weight)
    await callback.answer()


@router.message(StateFilter(Onboarding.weight))
async def process_weight(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    try:
        await state.update_data(weight=float(message.text.replace(",", ".")))
        await message.answer(t(lang, "onboarding.height"))
        await state.set_state(Onboarding.height)
    except ValueError:
        await message.answer(t(lang, "onboarding.weight"))


@router.message(StateFilter(Onboarding.height))
async def process_height(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    try:
        await state.update_data(height=float(message.text.replace(",", ".")))
        await message.answer(t(lang, "onboarding.age"))
        await state.set_state(Onboarding.age)
    except ValueError:
        await message.answer(t(lang, "onboarding.height"))


@router.message(StateFilter(Onboarding.age))
async def process_age(message: Message, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    try:
        await state.update_data(age=int(message.text))
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "gender.male"), callback_data="gender_male"),
             InlineKeyboardButton(text=t(lang, "gender.female"), callback_data="gender_female")]
        ])
        await message.answer(t(lang, "onboarding.gender"), reply_markup=kb)
        await state.set_state(Onboarding.gender)
    except ValueError:
        await message.answer(t(lang, "onboarding.age"))


@router.callback_query(StateFilter(Onboarding.gender))
async def process_gender(callback: CallbackQuery, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(gender=callback.data.split("_")[1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, f"activity.{lvl}"), callback_data=f"activity_{lvl}")]
        for lvl in ["sedentary", "light", "moderate", "active"]
    ])
    await callback.message.edit_text(t(lang, "onboarding.activity"), reply_markup=kb)
    await state.set_state(Onboarding.activity)
    await callback.answer()


@router.callback_query(StateFilter(Onboarding.activity))
async def process_activity(callback: CallbackQuery, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(activity=callback.data.split("_")[1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, f"goal.{g}"), callback_data=f"goal_{g}")]
        for g in ["lose", "gain", "maintain"]
    ])
    await callback.message.edit_text(t(lang, "onboarding.goal"), reply_markup=kb)
    await state.set_state(Onboarding.goal)
    await callback.answer()


@router.callback_query(StateFilter(Onboarding.goal))
async def process_goal(callback: CallbackQuery, state: FSMContext):
    lang = (await state.get_data()).get("language", "ru")
    await state.update_data(goal=callback.data.split("_")[1])
    await callback.message.edit_text(t(lang, "onboarding.allergies"))
    await state.set_state(Onboarding.allergies)
    await callback.answer()


@router.message(StateFilter(Onboarding.allergies))
async def process_allergies(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    allergies = message.text.strip()
    if allergies.lower() in ("нет", "none"):
        allergies = ""
    profile = UserProfile(
        user_id=message.from_user.id, language=lang, weight=data["weight"], height=data["height"],
        age=data["age"], gender=data["gender"], activity=data["activity"], goal=data["goal"],
        allergies=allergies,
    )
    await save_user(profile)
    daily_cal = calculate_daily_calories(profile)
    macros = get_macronutrient_targets(profile)
    goal_text = t(lang, f"goal.{profile.goal}")
    if message.from_user.id == ADMIN_ID:
        key = "profile_created_admin"
    else:
        await activate_trial(message.from_user.id)
        key = "profile_created"
    await message.answer(
        t(lang, key, goal=goal_text, calories=daily_cal,
          protein=macros['protein'], fat=macros['fat'], carbs=macros['carbs']),
        reply_markup=await build_main_menu(message.from_user.id, lang),
    )

    # Реферальный бонус: дарим дни и новичку, и пригласившему
    ref_id = data.get("ref_id")
    if ref_id and await add_referral(ref_id, message.from_user.id):
        await update_subscription(message.from_user.id, REFERRAL_BONUS_DAYS)
        await update_subscription(ref_id, REFERRAL_BONUS_DAYS)
        await message.answer(t(lang, "ref_new_reward", ref=REFERRAL_BONUS_DAYS))
        try:
            inviter = await get_user(ref_id)
            ilang = inviter.language if inviter else "ru"
            await bot.send_message(ref_id, t(ilang, "ref_inviter_reward", ref=REFERRAL_BONUS_DAYS))
        except Exception:
            pass

    await state.clear()


# --- Главное меню ---
@router.message(F.text.in_([t("ru", "food_diary_menu"), t("en", "food_diary_menu")]))
async def food_diary_menu(message: Message, state: FSMContext):
    lang = await get_lang(message.from_user.id, state)
    await message.answer(t(lang, "food_diary_menu"), reply_markup=food_diary_kb(lang))


@router.message(F.text.in_([t("ru", "profile"), t("en", "profile")]))
async def profile_menu(message: Message):
    profile = await get_user(message.from_user.id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        return
    lang = profile.language
    daily_cal = calculate_daily_calories(profile)
    macros = get_macronutrient_targets(profile)
    text = (
        f"<b>{esc(t(lang, 'profile'))}</b>\n"
        f"{esc(t(lang, 'edit_field.weight'))}: {profile.weight} kg\n"
        f"{esc(t(lang, 'edit_field.height'))}: {profile.height} cm\n"
        f"{esc(t(lang, 'edit_field.age'))}: {profile.age}\n"
        f"{esc(t(lang, 'edit_field.gender'))}: {esc(t(lang, f'gender.{profile.gender}'))}\n"
        f"{esc(t(lang, 'edit_field.activity'))}: {esc(t(lang, f'activity.{profile.activity}'))}\n"
        f"{esc(t(lang, 'edit_field.goal'))}: {esc(t(lang, f'goal.{profile.goal}'))}\n"
        f"{esc(t(lang, 'edit_field.allergies'))}: {esc(profile.allergies or '—')}\n"
        f"{esc(t(lang, 'edit_field.favorite_foods'))}: {esc(profile.favorite_foods or '—')}\n"
        f"{esc(t(lang, 'edit_field.disliked_foods'))}: {esc(profile.disliked_foods or '—')}\n"
        f"{esc(t(lang, 'edit_field.sport_types'))}: {esc(profile.sport_types or '—')}\n"
        f"{esc(t(lang, 'edit_field.habits'))}: {esc(profile.habits or '—')}\n"
        f"{esc(t(lang, 'edit_field.language'))}: {profile.language}\n\n"
        f"🔥 {daily_cal:.0f} {esc(t(lang, 'kcal_unit'))} · "
        f"Б {macros['protein']} · Ж {macros['fat']} · У {macros['carbs']}"
    )
    fields1 = [("weight", "⚖️"), ("height", "📏"), ("age", "🎂")]
    fields2 = [("gender", "🚻"), ("activity", "🏃"), ("goal", "🎯")]
    fields3 = [("allergies", "⚠️"), ("favorite_foods", "🍕"), ("disliked_foods", "🚫")]
    fields4 = [("sport_types", "🏅"), ("habits", "🌙"), ("language", "🌐")]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{ico} {t(lang, f'edit_field.{f}')}", callback_data=f"edit_field_{f}")
         for f, ico in row]
        for row in (fields1, fields2, fields3, fields4)
    ])
    await answer_html(message, text, reply_markup=kb)


@router.message(F.text.in_([t("ru", "meal_plan"), t("en", "meal_plan")]))
async def meal_plan_menu(message: Message):
    lang = await get_lang(message.from_user.id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "plan_1_day"), callback_data="plan_full_1")],
        [InlineKeyboardButton(text=t(lang, "plan_2_days"), callback_data="plan_full_2")],
        [InlineKeyboardButton(text=t(lang, "fridge_btn"), callback_data="fridge_recipe")],
    ])
    await message.answer(t(lang, "plan_choose_days"), reply_markup=kb)


@router.message(F.text.in_([t("ru", "reminders"), t("en", "reminders")]))
async def reminders_menu(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    reminders = await get_user_reminders(user_id)
    if not reminders:
        await answer_html(message, t(lang, "reminders_empty"))
        return
    items = "\n".join(f"⏰ {esc(r['time'])} — {esc(r['text'])}" for r in reminders)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {r['time']} {r['text'][:20]}", callback_data=f"del_rem_{r['id']}")]
        for r in reminders
    ])
    kb.inline_keyboard.append([InlineKeyboardButton(text=t(lang, "add_reminder_btn"),
                                                    callback_data="add_reminder_info")])
    await answer_html(message, t(lang, "reminders_list", items=items), reply_markup=kb)


@router.message(F.text.in_([t("ru", "subscription"), t("en", "subscription")]))
async def subscription_menu(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    invite_btn = InlineKeyboardButton(text=t(lang, "invite_btn"), callback_data="show_invite")
    status, days = await subscription_state(user_id)
    if status in ("active", "admin"):
        days = days if days else 9999
        await message.answer(t(lang, "subscription_menu_active", days=days),
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[[invite_btn]]))
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "buy_subscription_btn"), callback_data="buy_subscription")],
        [invite_btn],
    ])
    await message.answer(t(lang, "subscription_menu"), reply_markup=kb)


# --- Вода ---
def water_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "water_add_glass"), callback_data="water_add_250"),
         InlineKeyboardButton(text=t(lang, "water_add_bottle"), callback_data="water_add_500")],
        [InlineKeyboardButton(text=t(lang, "water_remove"), callback_data="water_sub_250")],
    ])


def water_text(profile: UserProfile, lang: str, ml: int) -> str:
    goal = water_goal_ml(profile)
    if ml >= goal:
        tip = t(lang, "water_tip_done")
    else:
        tip = t(lang, "water_tip_left", left=goal - ml)
    return t(lang, "water_title", ml=ml, goal=goal, bar=progress_bar(ml, goal), tip=tip)


@router.message(F.text.in_([t("ru", "water_menu_btn"), t("en", "water_menu_btn")]))
async def water_menu(message: Message):
    profile = await get_user(message.from_user.id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        return
    lang = profile.language
    today = user_now(profile).strftime("%Y-%m-%d")
    ml = await get_water(message.from_user.id, today)
    await answer_html(message, water_text(profile, lang, ml), reply_markup=water_kb(lang))


@router.callback_query(F.data.in_({"water_add_250", "water_add_500", "water_sub_250"}))
async def water_change(callback: CallbackQuery):
    profile = await get_user(callback.from_user.id)
    if not profile:
        await callback.answer()
        return
    lang = profile.language
    today = user_now(profile).strftime("%Y-%m-%d")
    delta = {"water_add_250": 250, "water_add_500": 500, "water_sub_250": -250}[callback.data]
    ml = await add_water(callback.from_user.id, today, delta)
    await edit_html(callback.message, water_text(profile, lang, ml), reply_markup=water_kb(lang))
    await callback.answer()


# --- Вес ---
def weight_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "weight_log_btn"), callback_data="weight_log")],
        [InlineKeyboardButton(text=t(lang, "weight_chart_btn"), callback_data="weight_chart")],
    ])


def weight_history_line(history: List[tuple], lang: str) -> str:
    weights = [w for _, w in history]
    d0 = datetime.strptime(history[0][0], "%Y-%m-%d")
    d1 = datetime.strptime(history[-1][0], "%Y-%m-%d")
    days = max(1, (d1 - d0).days)
    return t(lang, "weight_history_line", spark=sparkline(weights),
             first=weights[0], last=weights[-1], change=weights[-1] - weights[0], days=days)


def weight_menu_text(profile: UserProfile, lang: str, history: List[tuple]) -> str:
    if not history:
        block = t(lang, "weight_no_history")
    elif len(history) < 2:
        block = ""
    else:
        block = weight_history_line(history, lang)
    return t(lang, "weight_title", weight=profile.weight, history=block)


@router.message(F.text.in_([t("ru", "weight_menu_btn"), t("en", "weight_menu_btn")]))
async def weight_menu(message: Message):
    profile = await get_user(message.from_user.id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        return
    lang = profile.language
    history = await get_weight_history(message.from_user.id)
    await answer_html(message, weight_menu_text(profile, lang, history), reply_markup=weight_kb(lang))


@router.callback_query(F.data == "weight_log")
async def weight_log_start(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    await callback.message.answer(t(lang, "weight_enter"))
    await state.set_state(WeightInput.waiting)
    await callback.answer()


@router.message(StateFilter(WeightInput.waiting))
async def weight_input_value(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        await state.clear()
        return
    lang = profile.language
    try:
        w = float((message.text or "").replace(",", "."))
        if not (20 <= w <= 400):
            raise ValueError
    except ValueError:
        await message.answer(t(lang, "weight_enter"))
        return
    today = user_now(profile).strftime("%Y-%m-%d")
    await add_weight(user_id, today, w)
    profile.weight = w
    await save_user(profile)
    await state.clear()
    await message.answer(t(lang, "weight_saved", weight=w, calories=calculate_daily_calories(profile)),
                         reply_markup=await build_main_menu(user_id, lang))
    history = await get_weight_history(user_id)
    await answer_html(message, weight_menu_text(profile, lang, history), reply_markup=weight_kb(lang))


@router.callback_query(F.data == "weight_chart")
async def weight_chart(callback: CallbackQuery):
    profile = await get_user(callback.from_user.id)
    if not profile:
        await callback.answer()
        return
    lang = profile.language
    await callback.answer()
    history = await get_weight_history(callback.from_user.id, 90)
    if len(history) < 2:
        await send_html(callback.from_user.id, t(lang, "weight_chart_need_more"))
        return
    png = render_weight_png(history, t(lang, "weight_chart_caption"))
    if png:
        await bot.send_photo(callback.from_user.id,
                             BufferedInputFile(png, filename="weight.png"),
                             caption=t(lang, "weight_chart_caption"))
    else:
        await send_html(callback.from_user.id, t(lang, "weight_chart_caption") + "\n" +
                        weight_history_line(history, lang))


# --- Дневник питания ---
@router.callback_query(F.data == "add_food")
async def add_food_start(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    await callback.message.answer(t(lang, "send_food_prompt"))
    await state.set_state(FoodInput.waiting_for_text_or_photo)
    await callback.answer()


@router.callback_query(F.data == "show_today")
async def show_today_diary(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t("ru", "profile_not_found"), show_alert=True)
        return
    lang = profile.language
    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass
    text, kb = await render_today(profile, lang)
    await send_html(user_id, text, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "week_stats")
async def week_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t("ru", "profile_not_found"), show_alert=True)
        return
    lang = profile.language
    await callback.answer()
    now_local = user_now(profile)
    since = (now_local - timedelta(days=6)).strftime("%Y-%m-%d")
    rows = await get_recent_food(user_id, since)
    if not rows:
        await send_html(user_id, t(lang, "no_week_data"))
        return
    per_day: Dict[str, float] = {}
    for r in rows:
        per_day[r['date']] = per_day.get(r['date'], 0) + (r['calories'] or 0)
    norm = calculate_daily_calories(profile)
    lines = []
    for i in range(6, -1, -1):
        d = (now_local - timedelta(days=i)).strftime("%Y-%m-%d")
        cal = per_day.get(d, 0)
        lines.append(f"{esc(d[5:])}: {progress_bar(cal, norm, 8)} {cal:.0f}")
    avg = sum(per_day.values()) / len(per_day)
    if avg < norm - 150:
        note = t(lang, "week_note_low")
    elif avg > norm + 150:
        note = t(lang, "week_note_high")
    else:
        note = t(lang, "week_note_good")
    await send_html(user_id, t(lang, "week_stats_title", lines="\n".join(lines),
                               avg=avg, norm=norm, note=note))


@router.callback_query(F.data.startswith("del_entry_"))
async def delete_entry_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    lang = profile.language if profile else "ru"
    try:
        entry_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Invalid ID", show_alert=True)
        return
    if await delete_food_entry(entry_id, user_id):
        try:
            await callback.message.delete()
        except TelegramBadRequest:
            pass
        text, kb = await render_today(profile, lang)
        await send_html(user_id, text, reply_markup=kb)
        await callback.answer(t(lang, "entry_deleted"))
    else:
        await callback.answer("Could not delete", show_alert=True)


@router.message(StateFilter(FoodInput.waiting_for_text_or_photo))
async def handle_food_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(t("ru", "create_profile_first"))
        await state.clear()
        return
    lang = profile.language

    if message.text and is_nutrition_question(message.text):
        await handle_nutrition_question(message, state, profile, lang)
        return

    is_voice = bool(message.voice or message.audio)
    status_msg = await message.answer(t(lang, "voice_analyzing" if is_voice else "food_analyzing"))
    if message.photo:
        file = await bot.get_file(message.photo[-1].file_id)
        photo_io = await bot.download_file(file.file_path)
        result = await analyze_food_photo(photo_io.read(), lang)
    elif is_voice:
        media = message.voice or message.audio
        mime = getattr(media, "mime_type", None) or "audio/ogg"
        file = await bot.get_file(media.file_id)
        audio_io = await bot.download_file(file.file_path)
        result = await analyze_food_audio(audio_io.read(), mime, lang)
    elif message.text:
        result = await analyze_food_text(message.text, lang)
    else:
        result = {"_error": "analysis_failed"}

    try:
        await bot.delete_message(message.chat.id, status_msg.message_id)
    except TelegramBadRequest:
        pass

    if "_error" in result:
        if result["_error"] == "no_keys":
            await answer_html(message, t(lang, "no_keys"))
        else:
            await message.answer(t(lang, "analysis_failed"),
                                 reply_markup=await build_main_menu(user_id, lang))
        await state.clear()
        return

    if not result.get("is_food", False):
        if result.get("danger"):
            await message.answer(t(lang, "not_food_danger"),
                                 reply_markup=await build_main_menu(user_id, lang))
        else:
            await message.answer(t(lang, "not_food"),
                                 reply_markup=await build_main_menu(user_id, lang))
        await state.clear()
        return

    food_name = result.get("food_name", "Meal")
    portion = result.get("portion", "")
    description = f"{food_name} ({portion})" if portion else food_name
    calories = num(result.get("calories", 0))
    protein = num(result.get("protein", 0))
    fat = num(result.get("fat", 0))
    carbs = num(result.get("carbs", 0))
    unit = t(lang, "kcal_unit")

    await state.update_data(pending_food={
        "description": description, "calories": calories,
        "protein": protein, "fat": fat, "carbs": carbs,
    })
    heard = result.get("heard")
    heard_line = (t(lang, "voice_heard", text=esc(heard)) + "\n\n") if heard else ""
    text = (heard_line +
            f"🍽 <b>{esc(description)}</b>\n"
            f"{esc(unit).capitalize()}: {calories:.0f}\n"
            f"Б: {protein:.0f} г · Ж: {fat:.0f} г · У: {carbs:.0f} г")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "confirm_add"), callback_data="confirm_add"),
         InlineKeyboardButton(text=t(lang, "cancel_food"), callback_data="cancel_food")],
        [InlineKeyboardButton(text=t(lang, "fav_add_btn"), callback_data="confirm_add_fav")],
    ])
    await answer_html(message, text, reply_markup=kb)
    await state.set_state(FoodInput.confirm_add)


async def handle_nutrition_question(message: Message, state: FSMContext, profile: UserProfile, lang: str):
    if not await ensure_premium_message(message, lang):
        if state:
            await state.clear()
        return
    status_msg = await message.answer(t(lang, "nutritionist_generating"))
    try:
        answer = await ask_nutritionist(message.text, profile, lang)
    except Exception as e:
        logging.error("Nutrition question error: %s", e)
        answer = t(lang, "error_generic")
    try:
        await bot.delete_message(message.chat.id, status_msg.message_id)
    except TelegramBadRequest:
        pass
    await message.answer(answer, reply_markup=await build_main_menu(message.from_user.id, lang))
    await state.clear()


async def _log_meal_for_today(user_id: int, profile, description, cal, p, f, c):
    """Записывает блюдо в сегодняшний дневник по часовому поясу пользователя."""
    now = user_now(profile)
    await add_food_entry(user_id, now.strftime("%Y-%m-%d"), now.strftime("%H:%M"),
                         description, cal, p, f, c)


@router.callback_query(F.data == "confirm_add", StateFilter(FoodInput.confirm_add))
async def confirm_add_food(callback: CallbackQuery, state: FSMContext):
    pending = (await state.get_data()).get("pending_food")
    if not pending:
        await callback.answer("No data", show_alert=True)
        return
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    lang = profile.language if profile else "ru"
    await _log_meal_for_today(user_id, profile, pending["description"], pending["calories"],
                              pending["protein"], pending["fat"], pending["carbs"])
    await callback.message.edit_text(t(lang, "food_added", food=pending["description"]))
    _, cal, p, f, c = await today_totals(profile)
    await send_html(user_id, today_status_text(profile, lang, cal, p, f, c))
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "confirm_add_fav", StateFilter(FoodInput.confirm_add))
async def confirm_add_food_fav(callback: CallbackQuery, state: FSMContext):
    pending = (await state.get_data()).get("pending_food")
    if not pending:
        await callback.answer("No data", show_alert=True)
        return
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    lang = profile.language if profile else "ru"
    await _log_meal_for_today(user_id, profile, pending["description"], pending["calories"],
                              pending["protein"], pending["fat"], pending["carbs"])
    is_new = await add_favorite(user_id, pending["description"], pending["calories"],
                                pending["protein"], pending["fat"], pending["carbs"])
    await callback.message.edit_text(t(lang, "fav_added" if is_new else "fav_exists"))
    _, cal, p, f, c = await today_totals(profile)
    await send_html(user_id, today_status_text(profile, lang, cal, p, f, c))
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "cancel_food", StateFilter(FoodInput.confirm_add))
async def cancel_food(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    await callback.message.edit_text(t(lang, "food_cancelled"))
    await state.clear()
    await callback.answer()


# --- Быстрое добавление: частые блюда + избранное + повтор вчера ---
def _trim(label: str, limit: int = 30) -> str:
    return label if len(label) <= limit else label[:limit - 1] + "…"


async def build_quick_menu(user_id: int, lang: str) -> tuple:
    favorites = await get_favorites(user_id)
    frequent = await get_frequent_meals(user_id, 6)
    unit = t(lang, "kcal_unit")
    rows = []
    seen = set()
    for fav in favorites:
        seen.add(fav["description"])
        label = f"⭐ {_trim(fav['description'])} · {num(fav['calories']):.0f} {unit}"
        rows.append([
            InlineKeyboardButton(text=label, callback_data=f"qadd_fav_{fav['id']}"),
            InlineKeyboardButton(text="❌", callback_data=f"favdel_{fav['id']}"),
        ])
    for meal in frequent:
        if meal["description"] in seen:
            continue
        label = f"🍽 {_trim(meal['description'])} · {num(meal['calories']):.0f} {unit}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"qadd_food_{meal['id']}")])
    if not rows:
        return t(lang, "quick_empty"), None
    rows.append([InlineKeyboardButton(text=t(lang, "repeat_yesterday_btn"),
                                      callback_data="repeat_yesterday")])
    return t(lang, "quick_menu_title"), InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "quick_add")
async def quick_add_menu(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    text, kb = await build_quick_menu(callback.from_user.id, lang)
    await send_html(callback.from_user.id, text, reply_markup=kb)
    await callback.answer()


async def _quick_log_and_report(callback: CallbackQuery, description, cal, p, f, c):
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    lang = profile.language if profile else "ru"
    await _log_meal_for_today(user_id, profile, description, cal, p, f, c)
    await callback.answer(t(lang, "food_added", food=_trim(description)))
    _, tcal, tp, tf, tc = await today_totals(profile)
    await send_html(user_id, t(lang, "food_added", food=description) + "\n\n" +
                    today_status_text(profile, lang, tcal, tp, tf, tc))


@router.callback_query(F.data.startswith("qadd_food_"))
async def quick_add_food_row(callback: CallbackQuery):
    try:
        food_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer()
        return
    row = await get_food_row(food_id, callback.from_user.id)
    if not row:
        await callback.answer("Not found", show_alert=True)
        return
    await _quick_log_and_report(callback, row["description"], row["calories"],
                                row["protein"], row["fat"], row["carbs"])


@router.callback_query(F.data.startswith("qadd_fav_"))
async def quick_add_favorite(callback: CallbackQuery):
    try:
        fav_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer()
        return
    fav = await get_favorite(fav_id, callback.from_user.id)
    if not fav:
        await callback.answer("Not found", show_alert=True)
        return
    await _quick_log_and_report(callback, fav["description"], fav["calories"],
                                fav["protein"], fav["fat"], fav["carbs"])


@router.callback_query(F.data.startswith("favdel_"))
async def quick_delete_favorite(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await get_lang(user_id)
    try:
        fav_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer()
        return
    await remove_favorite(fav_id, user_id)
    await callback.answer(t(lang, "fav_removed"))
    text, kb = await build_quick_menu(user_id, lang)
    await edit_html(callback.message, text, reply_markup=kb)


@router.callback_query(F.data == "repeat_yesterday")
async def repeat_yesterday(callback: CallbackQuery):
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    lang = profile.language if profile else "ru"
    yesterday = (user_now(profile) - timedelta(days=1)).strftime("%Y-%m-%d")
    entries = await get_daily_food(user_id, yesterday)
    if not entries:
        await callback.answer(t(lang, "yesterday_empty"), show_alert=True)
        return
    for e in entries:
        await _log_meal_for_today(user_id, profile, e["description"], e["calories"],
                                  e["protein"], e["fat"], e["carbs"])
    await callback.answer()
    _, cal, p, f, c = await today_totals(profile)
    await send_html(user_id, t(lang, "yesterday_copied", n=len(entries)) + "\n\n" +
                    today_status_text(profile, lang, cal, p, f, c))


# --- Редактирование профиля ---
CLEARABLE_FIELDS = {"allergies", "favorite_foods", "disliked_foods", "sport_types", "habits"}
CALORIE_AFFECTING_FIELDS = {"weight", "height", "age", "gender", "activity", "goal"}


def field_current_value(profile: UserProfile, field: str, lang: str) -> str:
    if field == "gender":
        return t(lang, f"gender.{profile.gender}")
    if field == "activity":
        return t(lang, f"activity.{profile.activity}")
    if field == "goal":
        return t(lang, f"goal.{profile.goal}")
    if field == "language":
        return profile.language
    val = getattr(profile, field, "")
    return str(val) if (not isinstance(val, str) or val) else "—"


@router.callback_query(F.data.startswith("edit_field_"))
async def edit_field_start(callback: CallbackQuery, state: FSMContext):
    field = callback.data[len("edit_field_"):]
    user_id = callback.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t("ru", "profile_not_found"), show_alert=True)
        return
    lang = profile.language
    prompt = (f"{t(lang, f'edit_field.{field}')}\n" +
              t(lang, "current_value", value=field_current_value(profile, field, lang)))
    await state.update_data(edit_field=field)
    await state.set_state(EditProfile.waiting_for_value)

    kb = None
    if field == "gender":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "gender.male"), callback_data="set_gender_male"),
             InlineKeyboardButton(text=t(lang, "gender.female"), callback_data="set_gender_female")]
        ])
    elif field == "activity":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, f"activity.{lvl}"), callback_data=f"set_activity_{lvl}")]
            for lvl in ["sedentary", "light", "moderate", "active"]
        ])
    elif field == "goal":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, f"goal.{g}"), callback_data=f"set_goal_{g}")]
            for g in ["lose", "gain", "maintain"]
        ])
    elif field == "language":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Русский", callback_data="set_language_ru"),
             InlineKeyboardButton(text="English", callback_data="set_language_en")]
        ])
    elif field in CLEARABLE_FIELDS:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "edit_field.clear"), callback_data=f"clear_field_{field}")]
        ])
    await callback.message.edit_text(prompt, reply_markup=kb)
    await callback.answer()


@router.message(StateFilter(EditProfile.waiting_for_value))
async def edit_field_value(message: Message, state: FSMContext):
    field = (await state.get_data()).get("edit_field")
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not field or not profile:
        await message.answer(t("ru", "profile_not_found"))
        await state.clear()
        return
    lang = profile.language
    value = message.text.strip()
    if field in ("weight", "height"):
        try:
            value = float(value.replace(",", "."))
        except ValueError:
            await message.answer(t(lang, f"edit_field.{field}"))
            return
    elif field == "age":
        try:
            value = int(value)
        except ValueError:
            await message.answer(t(lang, "edit_field.age"))
            return
    elif field == "allergies" and value.lower() in ("нет", "none"):
        value = ""

    setattr(profile, field, value)
    await save_user(profile)
    field_disp = t(lang, f"edit_field.{field}")
    if field in CALORIE_AFFECTING_FIELDS:
        msg = t(lang, "field_updated_with_norm", field=field_disp, calories=calculate_daily_calories(profile))
    else:
        msg = t(lang, "field_updated", field=field_disp)
    await message.answer(msg, reply_markup=await build_main_menu(user_id, lang))
    await state.clear()


async def _set_choice(callback: CallbackQuery, state: FSMContext, field: str, value: str):
    lang = await get_lang(callback.from_user.id, state)
    profile = await get_user(callback.from_user.id)
    if profile:
        setattr(profile, field, value)
        await save_user(profile)
        field_disp = t(lang, f"edit_field.{field}")
        if field == "language":
            lang = value
            await callback.message.edit_text(t(lang, "field_updated", field=t(lang, "edit_field.language")))
        else:
            await callback.message.edit_text(
                t(lang, "field_updated_with_norm", field=field_disp, calories=calculate_daily_calories(profile)))
    await state.clear()
    await callback.answer()


@router.callback_query(F.data.startswith("set_gender_"))
async def set_gender(callback: CallbackQuery, state: FSMContext):
    await _set_choice(callback, state, "gender", callback.data.split("_")[2])


@router.callback_query(F.data.startswith("set_activity_"))
async def set_activity(callback: CallbackQuery, state: FSMContext):
    lvl = callback.data.split("_")[2]
    if lvl not in ["sedentary", "light", "moderate", "active"]:
        await callback.answer("Invalid", show_alert=True)
        return
    await _set_choice(callback, state, "activity", lvl)


@router.callback_query(F.data.startswith("set_goal_"))
async def set_goal(callback: CallbackQuery, state: FSMContext):
    await _set_choice(callback, state, "goal", callback.data.split("_")[2])


@router.callback_query(F.data.startswith("set_language_"))
async def set_language(callback: CallbackQuery, state: FSMContext):
    await _set_choice(callback, state, "language", callback.data.split("_")[2])


@router.callback_query(F.data.startswith("clear_field_"))
async def clear_field_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = await get_lang(user_id, state)
    field = callback.data[len("clear_field_"):]
    if field not in CLEARABLE_FIELDS:
        await callback.answer("Cannot clear", show_alert=True)
        return
    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t(lang, "profile_not_found"), show_alert=True)
        return
    setattr(profile, field, "")
    await save_user(profile)
    await state.clear()
    await callback.message.edit_text(t(lang, "field_cleared", field=t(lang, f"edit_field.{field}")))
    await callback.answer()


# --- План питания ---
@router.callback_query(F.data.startswith("plan_full_"))
async def generate_full_plan(callback: CallbackQuery):
    days = int(callback.data.split("_")[2])
    profile = await get_user(callback.from_user.id)
    if not profile:
        await callback.answer("Create a profile first", show_alert=True)
        return
    lang = profile.language
    if not await ensure_premium_callback(callback, lang):
        return
    await callback.message.edit_text(t(lang, "plan_generating"))
    await callback.answer()
    plan_text, plan_data = await generate_meal_plan(profile, days)

    shopping_kb = None
    if plan_data:
        await save_shopping_list(callback.from_user.id, build_shopping_list(plan_data, lang))
        shopping_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "shopping_btn"), callback_data="shopping_list")]
        ])

    if len(plan_text) <= 3900:
        await edit_html(callback.message, plan_text, reply_markup=shopping_kb)
    else:
        await edit_html(callback.message, t(lang, "plan_generating"))
        chunks = [plan_text[i:i + 3900] for i in range(0, len(plan_text), 3900)]
        for idx, chunk in enumerate(chunks):
            await send_html(callback.from_user.id, chunk,
                            reply_markup=shopping_kb if idx == len(chunks) - 1 else None)


@router.callback_query(F.data == "shopping_list")
async def show_shopping_list(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await get_lang(user_id)
    if not await ensure_premium_callback(callback, lang):
        return
    content = await get_shopping_list(user_id)
    await callback.answer()
    if not content:
        await send_html(user_id, t(lang, "shopping_empty"))
    else:
        await send_html(user_id, content)


# --- Рецепт из холодильника ---
@router.callback_query(F.data == "fridge_recipe")
async def fridge_recipe_start(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    if not await ensure_premium_callback(callback, lang):
        return
    await callback.message.answer(t(lang, "fridge_prompt"))
    await state.set_state(FridgeInput.waiting)
    await callback.answer()


@router.message(StateFilter(FridgeInput.waiting))
async def fridge_recipe_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        await state.clear()
        return
    lang = profile.language
    items = (message.text or "").strip()
    if not items:
        await message.answer(t(lang, "fridge_prompt"))
        return
    status_msg = await message.answer(t(lang, "fridge_generating"))
    try:
        recipe = await fridge_recipe(items, profile, lang)
    except Exception as e:
        logging.error("fridge_recipe error: %s", e)
        recipe = t(lang, "error_generic")
    try:
        await bot.delete_message(message.chat.id, status_msg.message_id)
    except TelegramBadRequest:
        pass
    await state.clear()
    await message.answer(recipe, reply_markup=await build_main_menu(user_id, lang))


# --- Напоминания ---
@router.callback_query(F.data == "add_reminder_info")
async def add_reminder_info(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await callback.answer()
    await answer_html(callback.message, t(lang, "reminder_add_info"))


@router.callback_query(F.data.startswith("del_rem_"))
async def delete_reminder_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await get_lang(user_id)
    try:
        rem_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Invalid ID", show_alert=True)
        return
    if await delete_reminder(rem_id, user_id):
        await callback.answer(t(lang, "reminder_deleted"))
        reminders = await get_user_reminders(user_id)
        if not reminders:
            await edit_html(callback.message, t(lang, "reminders_empty"))
        else:
            items = "\n".join(f"⏰ {esc(r['time'])} — {esc(r['text'])}" for r in reminders)
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"❌ {r['time']} {r['text'][:20]}", callback_data=f"del_rem_{r['id']}")]
                for r in reminders
            ])
            kb.inline_keyboard.append([InlineKeyboardButton(text=t(lang, "add_reminder_btn"),
                                                            callback_data="add_reminder_info")])
            await edit_html(callback.message, t(lang, "reminders_list", items=items), reply_markup=kb)
    else:
        await callback.answer("Could not delete", show_alert=True)


@router.message(F.text.regexp(r"^\d{1,2}[:.]\d{2}\s+.+"))
async def add_reminder(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    parts = message.text.strip().split(" ", 1)
    time_str = parts[0].replace(".", ":")
    reminder_text = parts[1]
    try:
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        normalized = f"{hour:02d}:{minute:02d}"
    except ValueError:
        await answer_html(message, t(lang, "reminder_add_info"))
        return
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO reminders (user_id, time, text) VALUES (?,?,?)",
                         (user_id, normalized, reminder_text))
        await db.commit()
    await message.answer(t(lang, "reminder_added", time=normalized, text=reminder_text))


# --- Платежи (Telegram Stars) ---
@router.callback_query(F.data == "buy_subscription")
async def create_payment(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=t(lang, "payment_title"),
        description=t(lang, "payment_desc"),
        payload=f"subscription_{callback.from_user.id}",
        currency="XTR",
        prices=[LabeledPrice(label=t(lang, "payment_title"), amount=10)],
        start_parameter="subscription",
        provider_token="",
    )
    await callback.answer()


@router.pre_checkout_query()
async def pre_checkout_handler(q: PreCheckoutQuery):
    await q.answer(ok=True)


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    await update_subscription(message.from_user.id, 30)
    profile = await get_user(message.from_user.id)
    lang = profile.language if profile else "ru"
    await message.answer(t(lang, "payment_success"),
                         reply_markup=await build_main_menu(message.from_user.id, lang))


# --- Админ ---
@router.message(Command("gift"))
async def gift_subscription(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        _, target_id, days = message.text.split()
        await update_subscription(int(target_id), int(days))
        await message.answer(t("ru", "gift_success", id=int(target_id), days=int(days)))
    except Exception:
        await message.answer(t("ru", "gift_error"))


# --- Универсальный обработчик (текст/фото вне FSM) ---
@router.message(F.content_type.in_({'text', 'photo', 'voice', 'audio'}))
async def universal_food_input(message: Message, state: FSMContext):
    cur = await state.get_state() if state else None
    if cur and (cur.startswith("Onboarding") or cur.startswith("EditProfile")
                or cur.startswith("FoodInput") or cur.startswith("WeightInput")
                or cur.startswith("FridgeInput")):
        return
    if message.text:
        menu_texts = [
            t("ru", "food_diary_menu"), t("en", "food_diary_menu"),
            t("ru", "water_menu_btn"), t("en", "water_menu_btn"),
            t("ru", "weight_menu_btn"), t("en", "weight_menu_btn"),
            t("ru", "profile"), t("en", "profile"),
            t("ru", "meal_plan"), t("en", "meal_plan"),
            t("ru", "reminders"), t("en", "reminders"),
            t("ru", "subscription"), t("en", "subscription"),
        ]
        if message.text in menu_texts:
            return
        if re.match(r'^\d{1,2}[:.]\d{2}\s+.+', message.text):
            return await add_reminder(message)
        if is_nutrition_question(message.text):
            profile = await get_user(message.from_user.id)
            if not profile:
                await message.answer(t("ru", "create_profile_first"))
                return
            await handle_nutrition_question(message, state, profile, profile.language)
            return
    await handle_food_input(message, state)


@router.callback_query()
async def unhandled_callback(callback: CallbackQuery):
    await callback.answer("This action is outdated.")


# --- Фоновые задачи ---
async def reminder_scheduler():
    sent = set()  # (user_id, "HH:MM", "YYYY-MM-DD") чтобы не дублировать в течение минуты
    while True:
        now = datetime.now(timezone.utc)
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT * FROM reminders WHERE active=1") as cur:
                    rows = await cur.fetchall()
            for row in rows:
                user = await get_user(row['user_id'])
                offset = user.utc_offset if user else 3
                local = now + timedelta(hours=offset)
                hh, mm = map(int, row['time'].split(":"))
                if local.hour == hh and local.minute == mm:
                    key = (row['user_id'], row['time'], local.strftime("%Y-%m-%d %H:%M"))
                    if key in sent:
                        continue
                    sent.add(key)
                    try:
                        await bot.send_message(row['user_id'], f"⏰ {row['text']}")
                    except Exception:
                        pass
            # чистим старые ключи
            if len(sent) > 5000:
                sent.clear()
        except Exception as e:
            logging.warning("reminder_scheduler: %s", e)
        await asyncio.sleep(30)


async def daily_summary_scheduler():
    sent_dates = set()
    while True:
        now = datetime.now(timezone.utc)
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT user_id, utc_offset, language FROM users") as cur:
                    users = await cur.fetchall()
            for u in users:
                offset = u['utc_offset'] if u['utc_offset'] is not None else 3
                local = now + timedelta(hours=offset)
                if local.hour != 21:
                    continue
                today_str = local.strftime("%Y-%m-%d")
                marker = (u['user_id'], today_str)
                if marker in sent_dates:
                    continue
                profile = await get_user(u['user_id'])
                if not profile:
                    continue
                entries = await get_daily_food(u['user_id'], today_str)
                if not entries:
                    continue
                sent_dates.add(marker)
                lang = u['language'] or 'ru'
                cal = sum(e['calories'] or 0 for e in entries)
                p = sum(e['protein'] or 0 for e in entries)
                f = sum(e['fat'] or 0 for e in entries)
                c = sum(e['carbs'] or 0 for e in entries)
                norm = calculate_daily_calories(profile)
                diff = norm - cal
                if diff > 100:
                    advice = t(lang, "daily_summary_under")
                elif diff < -100:
                    advice = t(lang, "daily_summary_over")
                else:
                    advice = t(lang, "daily_summary_ok")
                text = t(lang, "daily_summary", date=today_str, consumed=cal, norm=norm,
                         bar=progress_bar(cal, norm), p=p, f=f, c=c, advice=advice)
                await send_html(u['user_id'], text)
            if len(sent_dates) > 10000:
                sent_dates.clear()
        except Exception as e:
            logging.warning("daily_summary_scheduler: %s", e)
        await asyncio.sleep(600)


# --- Запуск ---
async def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN не задан. Установи переменную окружения BOT_TOKEN.")
    if not GEMINI_API_KEYS:
        logging.warning("GEMINI_API_KEYS не заданы — функции ИИ будут недоступны.")
    await init_db()
    global BOT_USERNAME
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username or ""
    except Exception as e:
        logging.warning("Не удалось получить username бота: %s", e)
    try:
        from aiogram.types import BotCommand
        await bot.set_my_commands([
            BotCommand(command="start", description="Запуск / Start"),
            BotCommand(command="help", description="Помощь / Help"),
            BotCommand(command="invite", description="Пригласить друга / Invite a friend"),
        ])
    except Exception as e:
        logging.warning("set_my_commands: %s", e)
    asyncio.create_task(reminder_scheduler())
    asyncio.create_task(daily_summary_scheduler())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
