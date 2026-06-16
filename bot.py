import asyncio
import logging
import os
import re
import json
import base64
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Any
from dataclasses import dataclass

import asyncpg
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.redis import RedisStorage, DefaultKeyBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton,
    Message, CallbackQuery, PreCheckoutQuery, LabeledPrice
)
from aiogram.exceptions import TelegramBadRequest

import openai

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from dotenv import load_dotenv
load_dotenv()

# --- Настройки окружения ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
REDIS_URL = os.getenv("REDIS_URL", "")
GEMINI_WEB2API_URL = os.getenv("GEMINI_WEB2API_URL", "")
ADMIN_ID = 1183393935
DATABASE_URL = os.getenv("DATABASE_URL", "")

DEFAULT_MODEL = "gemini-3.5-flash"
FALLBACK_MODELS = ["gemini-3.1-pro", "gemini-3.5-flash-thinking-lite", "gemini-3.5-flash-thinking", "gemini-flash-lite"]

openai_client = openai.AsyncOpenAI(
    base_url=GEMINI_WEB2API_URL,
    api_key="any"
)

# --- Хранилище состояний ---
if REDIS_AVAILABLE:
    try:
        redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        storage = RedisStorage(redis=redis, key_builder=DefaultKeyBuilder(with_destiny=True))
    except Exception as e:
        logging.warning(f"Redis недоступен: {e}, переключаюсь на память")
        storage = MemoryStorage()
else:
    storage = MemoryStorage()

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# --- Пул подключений к PostgreSQL ---
db_pool: asyncpg.Pool = None

# --- Модели данных ---
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

# --- База данных (PostgreSQL) ---
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
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
                subscribed_until TIMESTAMPTZ,
                trial_used INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS food_diary (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                date DATE,
                meal_time TIME,
                description TEXT,
                calories REAL,
                protein REAL,
                fat REAL,
                carbs REAL,
                photo_id TEXT,
                UNIQUE(user_id, date, meal_time, description)
            );
            CREATE TABLE IF NOT EXISTS reminders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                time TIME,
                text TEXT,
                active INTEGER DEFAULT 1
            );
        """)

# --- Утилиты БД ---
async def get_user(user_id: int) -> Optional[UserProfile]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)
        if row:
            return UserProfile(
                user_id=row['user_id'],
                language=row['language'],
                weight=row['weight'],
                height=row['height'],
                age=row['age'],
                gender=row['gender'],
                activity=row['activity'],
                goal=row['goal'],
                allergies=row['allergies'],
                favorite_foods=row['favorite_foods'],
                disliked_foods=row['disliked_foods'],
                sport_types=row['sport_types'],
                habits=row['habits'],
                utc_offset=row['utc_offset']
            )
    return None

async def save_user(profile: UserProfile):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, language, weight, height, age, gender, activity, goal,
                               allergies, favorite_foods, disliked_foods, sport_types, habits, utc_offset)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (user_id) DO UPDATE SET
                language = EXCLUDED.language,
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender,
                activity = EXCLUDED.activity,
                goal = EXCLUDED.goal,
                allergies = EXCLUDED.allergies,
                favorite_foods = EXCLUDED.favorite_foods,
                disliked_foods = EXCLUDED.disliked_foods,
                sport_types = EXCLUDED.sport_types,
                habits = EXCLUDED.habits,
                utc_offset = EXCLUDED.utc_offset
        """, profile.user_id, profile.language, profile.weight, profile.height,
            profile.age, profile.gender, profile.activity, profile.goal,
            profile.allergies, profile.favorite_foods, profile.disliked_foods,
            profile.sport_types, profile.habits, profile.utc_offset)

async def update_subscription(user_id: int, days: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT subscribed_until FROM users WHERE user_id=$1", user_id)
        current = row['subscribed_until'] if row and row['subscribed_until'] else None
        if current:
            until = current + timedelta(days=days)
        else:
            until = datetime.now(timezone.utc) + timedelta(days=days)
        await conn.execute("UPDATE users SET subscribed_until=$1 WHERE user_id=$2", until, user_id)

async def is_subscribed(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT subscribed_until, trial_used FROM users WHERE user_id=$1", user_id)
        if not row:
            return False
        sub_until, trial_used = row['subscribed_until'], row['trial_used']
        if sub_until and sub_until > datetime.now(timezone.utc):
            return True
        if not trial_used:
            return True
    return False

async def activate_trial(user_id: int):
    async with db_pool.acquire() as conn:
        trial_end = datetime.now(timezone.utc) + timedelta(days=3)
        await conn.execute("UPDATE users SET subscribed_until=$1, trial_used=1 WHERE user_id=$2",
                           trial_end, user_id)

async def add_food_entry(user_id: int, date: str, meal_time: str, description: str,
                         calories: float, protein: float, fat: float, carbs: float, photo_id=None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO food_diary (user_id, date, meal_time, description, calories, protein, fat, carbs, photo_id)
            VALUES ($1, $2, $3::time, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (user_id, date, meal_time, description) DO UPDATE SET
                calories = EXCLUDED.calories,
                protein = EXCLUDED.protein,
                fat = EXCLUDED.fat,
                carbs = EXCLUDED.carbs,
                photo_id = EXCLUDED.photo_id
        """, user_id, date, meal_time, description, calories, protein, fat, carbs, photo_id)

async def delete_food_entry(entry_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM food_diary WHERE id=$1 AND user_id=$2 RETURNING id", entry_id, user_id
        )
        return row is not None

async def get_daily_food(user_id: int, date: str) -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM food_diary WHERE user_id=$1 AND date=$2 ORDER BY meal_time", user_id, date
        )
        return [dict(row) for row in rows]

async def get_user_reminders(user_id: int) -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM reminders WHERE user_id=$1 AND active=1 ORDER BY time", user_id
        )
        return [dict(row) for row in rows]

async def delete_reminder(reminder_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "DELETE FROM reminders WHERE id=$1 AND user_id=$2 RETURNING id", reminder_id, user_id
        )
        return row is not None

# --- КАЛЬКУЛЯТОР НОРМЫ ---
def calculate_daily_calories(profile: UserProfile) -> float:
    if profile.gender == "male":
        bmr = 10 * profile.weight + 6.25 * profile.height - 5 * profile.age + 5
    else:
        bmr = 10 * profile.weight + 6.25 * profile.height - 5 * profile.age - 161
    activity_mult = {
        "sedentary": 1.2,
        "light": 1.375,
        "moderate": 1.55,
        "active": 1.725,
        "very_active": 1.9
    }
    tdee = bmr * activity_mult.get(profile.activity, 1.2)
    if profile.goal == "lose":
        target = tdee * 0.8
        safe_min = bmr * 1.2
        return max(target, safe_min)
    elif profile.goal == "gain":
        target = tdee * 1.2
        safe_max = tdee * 1.5
        return min(target, safe_max)
    else:
        return tdee

# --- ЛОКАЛИЗАЦИЯ ---
LOCALE = {
    "ru": {
        "welcome": "Привет! Я твой персональный ИИ-нутрициолог. Выбери язык / Choose language:",
        "onboarding": {
            "weight": "Отлично! Введите ваш текущий вес (в кг):",
            "height": "Введите ваш рост (в см):",
            "age": "Введите ваш возраст (полных лет):",
            "gender": "Укажите ваш пол:",
            "activity": "Уровень физической активности:",
            "goal": "Ваша главная цель:",
            "allergies": "Есть ли пищевые аллергии? Перечислите через запятую или напишите 'нет':",
        },
        "gender": {"male": "Мужской", "female": "Женский"},
        "activity": {"sedentary": "Сидячий", "light": "Лёгкий", "moderate": "Умеренный", "active": "Активный"},
        "goal": {"lose": "Похудеть", "gain": "Набрать массу", "maintain": "Поддерживать"},
        "profile_created": "🎉 Профиль создан!\nЦель: {goal}\nВаша дневная норма: {calories:.0f} ккал.\nВам предоставлен пробный период на 3 дня.",
        "profile_created_admin": "🎉 Профиль создан!\nЦель: {goal}\nВаша дневная норма: {calories:.0f} ккал.\nБесконечная подписка 😎",
        "returning": "С возвращением! Ваш профиль загружен.\nЦель: {goal}, норма: {calories:.0f} ккал.",
        "subscription_expired": "🚫 Ваша подписка истекла. Для продолжения использования бота приобретите подписку:",
        "buy_subscription_btn": "💳 Купить подписку (30 дней / 10⭐)",
        "food_diary_menu": "📒 Дневник питания:",
        "add_food_btn": "➕ Добавить блюдо",
        "show_today_btn": "📋 Сегодня",
        "no_entries": "Сегодня ещё нет записей.",
        "today_summary": "🔹 Итого: {cal:.0f} ккал, Б: {p:.1f}г, Ж: {f:.1f}г, У: {c:.1f}г",
        "food_added": "✅ Блюдо '{food}' добавлено!",
        "food_cancelled": "Добавление отменено.",
        "profile": "📊 Профиль",
        "meal_plan": "📅 План питания",
        "reminders": "⏰ Напоминания",
        "subscription": "💳 Подписка",
        "send_food_prompt": "Отправьте описание блюда или фото тарелки.",
        "create_profile_first": "Сначала создайте профиль через /start",
        "profile_not_found": "Профиль не найден, используйте /start",
        "error_try_again": "Ошибка, попробуйте снова.",
        "entry_deleted": "🗑 Запись удалена.",
        "edit_field": {
            "weight": "Вес",
            "height": "Рост",
            "age": "Возраст",
            "gender": "Пол",
            "activity": "Активность",
            "goal": "Цель",
            "allergies": "Аллергии",
            "favorite_foods": "Любимая еда",
            "disliked_foods": "Нелюбимая еда",
            "sport_types": "Спорт",
            "habits": "Привычки",
            "language": "Язык",
            "clear": "🗑 Очистить"
        },
        "field_updated": "✅ Поле '{field}' обновлено!",
        "field_updated_with_norm": "✅ Поле '{field}' обновлено! Новая норма: {calories:.0f} ккал",
        "field_cleared": "✅ Поле '{field}' очищено!",
        "current_value": "Текущее значение: {value}",
        "plan_choose_days": "Выберите длительность плана:",
        "plan_1_day": "На 1 день",
        "plan_2_days": "На 2 дня",
        "plan_generating": "⏳ Генерирую меню...",
        "nutritionist_generating": "⏳ Спрашиваю нутрициолога...",
        "reminders_empty": "У вас нет активных напоминаний.\nЧтобы добавить, напишите время и текст, например:\n09:00 Выпить витамины",
        "reminders_list": "📋 *Ваши напоминания:*\n{items}\n\nЧтобы удалить, нажмите кнопку ниже.",
        "reminder_add_info": "Напишите напоминание в формате: ЧЧ:ММ текст\nПримеры: 09:00 Выпить витамины, 9:15 Тренировка, 7.30 Завтрак",
        "reminder_added": "⏰ Напоминание на {time} установлено: «{text}»",
        "reminder_deleted": "🗑 Напоминание удалено.",
        "payment_title": "Подписка на ИИ-нутрициолога",
        "payment_desc": "30 дней доступа к персональному диетологу за 10 Telegram Stars",
        "payment_success": "✅ Оплата прошла успешно! Подписка продлена на 30 дней. Спасибо!",
        "gift_success": "Пользователю {id} подарено {days} дней подписки.",
        "gift_error": "Ошибка: используйте /gift ID ДНИ",
        "not_food_danger": "⚠️ Это не еда! Если вы или кто-то рядом съел подобное, немедленно обратитесь к врачу или вызовите скорую помощь!",
        "not_food": "Я не распознал это как еду. Попробуйте другое описание или фото.",
        "analysis_failed": "❌ Не удалось распознать блюдо. Попробуйте ещё раз или укажите более точное описание.",
        "error_generic": "Произошла ошибка. Попробуйте позже.",
        "daily_summary": "🌙 *Итоги дня ({date})*\nПотреблено: {consumed:.0f} из {norm:.0f} ккал\nБ: {p:.1f} г, Ж: {f:.1f} г, У: {c:.1f} г\n{advice}",
        "daily_summary_ok": "✅ Вы уложились в норму! (±100 ккал)",
        "daily_summary_over": "⚠️ Превышение нормы калорий сегодня. Завтра постарайтесь сбалансировать питание.",
        "daily_summary_under": "⚠️ Вы недобрали калорий сегодня. Завтра стоит питаться полноценнее.",
        "subscription_menu": "Выберите действие:",
        "confirm_add": "✅ Добавить",
        "cancel_food": "❌ Отмена",
        "kcal_unit": "ккал",
        "meal_plan_labels": {
            "recommended": "Рекомендованная норма",
            "day": "День",
            "breakfast": "Завтрак",
            "lunch": "Обед",
            "dinner": "Ужин",
            "snacks": "Перекус",
            "daily_total": "Итого за день",
            "macros": "БЖУ",
            "preparation": "Приготовление"
        }
    },
    "en": {
        "welcome": "Hi! I'm your personal AI nutritionist. Choose your language / Выберите язык:",
        "onboarding": {
            "weight": "Great! Enter your current weight (in kg):",
            "height": "Enter your height (in cm):",
            "age": "Enter your age (full years):",
            "gender": "Please specify your gender:",
            "activity": "Physical activity level:",
            "goal": "Your main goal:",
            "allergies": "Do you have any food allergies? List them separated by commas or type 'none':",
        },
        "gender": {"male": "Male", "female": "Female"},
        "activity": {"sedentary": "Sedentary", "light": "Light", "moderate": "Moderate", "active": "Active"},
        "goal": {"lose": "Lose weight", "gain": "Gain mass", "maintain": "Maintain"},
        "profile_created": "🎉 Profile created!\nGoal: {goal}\nYour daily norm: {calories:.0f} kcal.\nYou got a 3-day trial period.",
        "profile_created_admin": "🎉 Profile created!\nGoal: {goal}\nYour daily norm: {calories:.0f} kcal.\nUnlimited subscription 😎",
        "returning": "Welcome back! Your profile is loaded.\nGoal: {goal}, norm: {calories:.0f} kcal.",
        "subscription_expired": "🚫 Your subscription has expired. To continue using the bot, purchase a subscription:",
        "buy_subscription_btn": "💳 Buy subscription (30 days / 10⭐)",
        "food_diary_menu": "📒 Food Diary:",
        "add_food_btn": "➕ Add meal",
        "show_today_btn": "📋 Today",
        "no_entries": "No entries today yet.",
        "today_summary": "🔹 Total: {cal:.0f} kcal, P: {p:.1f}g, F: {f:.1f}g, C: {c:.1f}g",
        "food_added": "✅ Meal '{food}' added!",
        "food_cancelled": "Adding cancelled.",
        "profile": "📊 Profile",
        "meal_plan": "📅 Meal Plan",
        "reminders": "⏰ Reminders",
        "subscription": "💳 Subscription",
        "send_food_prompt": "Send a meal description or a photo of your plate.",
        "create_profile_first": "Please create a profile first using /start",
        "profile_not_found": "Profile not found, use /start",
        "error_try_again": "Error, please try again.",
        "entry_deleted": "🗑 Entry deleted.",
        "edit_field": {
            "weight": "Weight",
            "height": "Height",
            "age": "Age",
            "gender": "Gender",
            "activity": "Activity",
            "goal": "Goal",
            "allergies": "Allergies",
            "favorite_foods": "Favorite foods",
            "disliked_foods": "Disliked foods",
            "sport_types": "Sports",
            "habits": "Habits",
            "language": "Language",
            "clear": "🗑 Clear"
        },
        "field_updated": "✅ '{field}' updated!",
        "field_updated_with_norm": "✅ '{field}' updated! New norm: {calories:.0f} kcal",
        "field_cleared": "✅ '{field}' cleared!",
        "current_value": "Current value: {value}",
        "plan_choose_days": "Choose the plan duration:",
        "plan_1_day": "1 day",
        "plan_2_days": "2 days",
        "plan_generating": "⏳ Generating menu...",
        "nutritionist_generating": "⏳ Asking nutritionist...",
        "reminders_empty": "You have no active reminders.\nTo add one, write time and text, e.g.:\n09:00 Take vitamins",
        "reminders_list": "📋 *Your reminders:*\n{items}\n\nPress a button to delete.",
        "reminder_add_info": "Write a reminder in the format: HH:MM text\nExamples: 09:00 Take vitamins, 9:15 Workout, 7.30 Breakfast",
        "reminder_added": "⏰ Reminder set for {time}: «{text}»",
        "reminder_deleted": "🗑 Reminder deleted.",
        "payment_title": "AI Nutritionist Subscription",
        "payment_desc": "30 days of access to a personal dietitian for 10 Telegram Stars",
        "payment_success": "✅ Payment successful! Subscription extended for 30 days. Thank you!",
        "gift_success": "User {id} received {days} days of subscription as a gift.",
        "gift_error": "Error: use /gift ID DAYS",
        "not_food_danger": "⚠️ This is not food! If you or someone else ate something like this, seek medical help immediately!",
        "not_food": "I didn't recognize this as food. Try another description or photo.",
        "analysis_failed": "❌ Could not recognize the meal. Please try again or provide a more precise description.",
        "error_generic": "An error occurred. Please try again later.",
        "daily_summary": "🌙 *Daily summary ({date})*\nConsumed: {consumed:.0f} out of {norm:.0f} kcal\nP: {p:.1f} g, F: {f:.1f} g, C: {c:.1f} g\n{advice}",
        "daily_summary_ok": "✅ You stayed within your norm! (±100 kcal)",
        "daily_summary_over": "⚠️ Calorie surplus today. Try to balance your meals tomorrow.",
        "daily_summary_under": "⚠️ You didn't eat enough today. Make sure to eat properly tomorrow.",
        "subscription_menu": "Choose an action:",
        "confirm_add": "✅ Add",
        "cancel_food": "❌ Cancel",
        "kcal_unit": "kcal",
        "meal_plan_labels": {
            "recommended": "Recommended daily intake",
            "day": "Day",
            "breakfast": "Breakfast",
            "lunch": "Lunch",
            "dinner": "Dinner",
            "snacks": "Snacks",
            "daily_total": "Total for the day",
            "macros": "Macros",
            "preparation": "Preparation"
        }
    }
}

def t(lang: str, key: str, **kwargs) -> str:
    parts = key.split(".")
    val = LOCALE.get(lang, LOCALE["en"])
    for part in parts:
        if isinstance(val, dict):
            val = val.get(part, {})
        else:
            return key
    if isinstance(val, str):
        return val.format(**kwargs) if kwargs else val
    return val

# --- AI-сервис ---
def extract_json(text: str) -> dict:
    text = re.sub(r'```(?:json)?\s*\n?', '', text)
    text = re.sub(r'\n?```', '', text).strip()
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        json_str = match.group()
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    logging.warning(f"Failed to parse JSON from: {text[:300]}")
    return {"raw": text, "error": "Could not parse JSON"}

async def ai_call(prompt: str, expect_json: bool = False, model: str = DEFAULT_MODEL, max_tokens: int = 1000) -> dict:
    models_to_try = [model] + [m for m in FALLBACK_MODELS if m != model]
    for m in models_to_try:
        try:
            response = await openai_client.chat.completions.create(
                model=m,
                messages=[
                    {"role": "system", "content": "You are a helpful nutrition assistant. Always respond with valid JSON if requested."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=max_tokens,
            )
            text = response.choices[0].message.content
            if expect_json:
                parsed = extract_json(text)
                if "error" in parsed:
                    logging.warning(f"Model {m} returned non-JSON: {text[:200]}")
                    continue
                return parsed
            return {"text": text}
        except Exception as e:
            logging.warning(f"Model {m} failed: {e}")
    return {"error": "All AI models failed"}

async def analyze_food_text(description: str, lang: str) -> dict:
    prompt = (
        "Analyze the following text. If it clearly describes edible food or drink, "
        "return a JSON object with keys: food_name, calories, protein, fat, carbs (all numbers). "
        "If it's a non-food object, poison, or dangerous item, return exactly: "
        '{"error":"not_food","message":"This is not edible! Seek medical help if ingested."}. '
        "For normal non-food words, return: {'error':'not_food','message':'Not food'}. "
        f"Language: {lang}. Description: '{description}'. Return ONLY the JSON object, no other text."
    )
    result = await ai_call(prompt, expect_json=True, max_tokens=300)
    if "error" in result and result["error"] != "not_food":
        logging.error(f"analyze_food_text failed: {result}")
        return {"error": "analysis_failed", "message": "Could not analyze food."}
    return result

async def analyze_food_photo(photo_bytes: bytes, lang: str) -> dict:
    image_base64 = base64.b64encode(photo_bytes).decode('utf-8')
    data_uri = f"data:image/jpeg;base64,{image_base64}"
    prompt = (
        "Identify what is shown on the photo. If it's edible food or drink, "
        "return JSON: {'food_name':'...', 'calories':..., 'protein':..., 'fat':..., 'carbs':...}. "
        "If it's a NON-FOOD object, poison, or dangerous item, return EXACTLY: "
        "{'error':'not_food','message':'This is not edible! Seek medical help if ingested.'}. "
        "For other non-food images return {'error':'not_food','message':'Not food'}. "
        "Return ONLY the JSON object, no extra text."
    )
    try:
        response = await openai_client.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=[{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": data_uri}}
            ]}],
            temperature=0.2,
            max_tokens=300,
        )
        parsed = extract_json(response.choices[0].message.content)
        if "error" in parsed and parsed["error"] != "not_food":
            logging.error(f"analyze_food_photo JSON error: {parsed}")
            return {"error": "analysis_failed", "message": "Could not analyze photo."}
        return parsed
    except Exception as e:
        logging.error(f"Vision error: {e}")
        return {"error": str(e)}

# --- План питания ---
def normalize_plan(data: dict) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        plan = data.get("plan", data)
        if isinstance(plan, list):
            return plan
        if isinstance(plan, dict):
            days = []
            for key in sorted(plan.keys()):
                if key.startswith("day_"):
                    try:
                        day_num = int(key.split("_")[1])
                        entry = plan[key]
                        entry["day"] = day_num
                        days.append(entry)
                    except:
                        pass
            return days
    return []

def format_meal_plan(plan_data: list, daily_cal: float, lang: str) -> str:
    labels = t(lang, "meal_plan_labels")
    if not isinstance(labels, dict):
        labels = LOCALE["en"]["meal_plan_labels"]
    lines = [f"🍽 *{labels['recommended']}: {daily_cal:.0f} {t(lang, 'kcal_unit')}*"]

    for day in plan_data:
        day_num = day.get("day", "?")
        lines.append(f"\n📅 *{labels['day']} {day_num}*")
        total_cal = 0
        for meal_key in ["breakfast", "lunch", "dinner", "snacks"]:
            meal = day.get(meal_key)
            if not meal:
                continue
            meal_name = meal.get("meal_name", meal_key)
            ingredients = meal.get("ingredients", [])
            cal = meal.get("calories", 0)
            total_cal += cal
            macros = meal.get("macros", {})
            p = macros.get("proteins", "?")
            f = macros.get("fats", "?")
            c = macros.get("carbohydrates", "?")
            prep = meal.get("preparation", "")
            lines.append(f"  • *{labels[meal_key]}*: {meal_name} ({cal} {t(lang, 'kcal_unit')})")
            if ingredients:
                ing_text = ", ".join(ingredients)
                lines.append(f"    🛒 _{ing_text}_")
            if prep:
                lines.append(f"    🥣 *{labels['preparation']}*: {prep}")
            lines.append(f"    {labels['macros']}: {p}, {f}, {c}")
        lines.append(f"  📊 *{labels['daily_total']}: {total_cal} {t(lang, 'kcal_unit')}*")

    return "\n".join(lines)

async def generate_meal_plan(profile: UserProfile, days: int = 1) -> str:
    if days not in [1, 2]:
        days = 1
    daily_cal = calculate_daily_calories(profile)
    prompt = (
        f"Create a {days}-day meal plan for user: goal={profile.goal}, calories≈{daily_cal:.0f} kcal, "
        f"allergies={profile.allergies}, likes={profile.favorite_foods}, dislikes={profile.disliked_foods}, "
        f"sports={profile.sport_types}. Language: {profile.language}. "
        "Use affordable, common ingredients. For each meal include a list of all ingredients and a very short preparation instruction (1-2 sentences). "
        "Return the response as a JSON object with the following structure: "
        '{"plan": [{"day": 1, "breakfast": {"meal_name": "...", "ingredients": [...], "calories": ..., "preparation": "...", "macros": {"proteins": "...", "fats": "...", "carbohydrates": "..."}}, "lunch": {...}, "dinner": {...}, "snacks": {...}}]} '
        "for each day. Include total daily calories and macros for each day. "
        "Respond ONLY with the JSON, no extra text."
    )
    result = await ai_call(prompt, expect_json=True, max_tokens=2000)
    if "error" in result:
        return t(profile.language, "error_generic") + f" ({result['error']})"

    plan_data = normalize_plan(result)
    if not plan_data:
        raw = result.get("raw", str(result))
        return f"Could not parse plan. AI response:\n{raw[:1500]}"

    text = format_meal_plan(plan_data, daily_cal, profile.language)
    return text

# --- Вопросы нутрициологу ---
def is_nutrition_question(text: str) -> bool:
    text_lower = text.lower()
    question_triggers = [
        "что приготовить", "что поесть", "посоветуй", "как приготовить", "рецепт",
        "что съесть", "подскажи", "какая еда", "чем перекусить", "что лучше",
        "как питаться", "диета", "рацион", "what to cook", "what to eat", "advice",
        "suggest", "recipe", "how to prepare", "what should i eat"
    ]
    return any(trigger in text_lower for trigger in question_triggers) or text_lower.endswith('?')

async def ask_nutritionist(message: Message, profile: UserProfile, lang: str) -> str:
    prompt = (
        f"You are a professional nutritionist. The user has the following profile: "
        f"goal={profile.goal}, weight={profile.weight}kg, height={profile.height}cm, age={profile.age}, "
        f"allergies={profile.allergies}, favorite foods={profile.favorite_foods}, disliked={profile.disliked_foods}, "
        f"sports={profile.sport_types}. "
        f"Answer the following question concisely in {lang} language, giving practical dietary advice: "
        f"{message.text}"
    )
    result = await ai_call(prompt, expect_json=False, max_tokens=600, model=DEFAULT_MODEL)
    if "error" in result:
        return t(lang, "error_generic")
    return result.get("text", "Sorry, I couldn't generate an answer.")

# --- Состояния ---
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

# --- Клавиатуры ---
async def build_main_menu(user_id: int, lang: str = "ru") -> ReplyKeyboardMarkup:
    sub = await is_subscribed(user_id)

    buttons = [
        [KeyboardButton(text=t(lang, "food_diary_menu"))],
        [KeyboardButton(text=t(lang, "profile")),
         KeyboardButton(text=t(lang, "meal_plan"))],
        [KeyboardButton(text=t(lang, "reminders"))]
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
        lang = data.get("language")
        if lang:
            return lang
    return "ru"

# --- Middleware подписки ---
async def subscription_middleware(handler, event, data):
    user_id = None
    if isinstance(event, Message):
        user_id = event.from_user.id
        text = event.text or ""
        if text.startswith("/start") or text in [
            t(await get_lang(event.from_user.id, data.get('state')), "subscription")
        ] or text == "/gift":
            return await handler(event, data)
    elif isinstance(event, CallbackQuery):
        user_id = event.from_user.id
        if event.data in ["buy_subscription"] or event.data.startswith("payment_"):
            return await handler(event, data)
    elif isinstance(event, PreCheckoutQuery):
        return await handler(event, data)
    else:
        return await handler(event, data)

    if user_id == ADMIN_ID:
        return await handler(event, data)

    state: FSMContext = data.get('state')
    if state:
        current_state = await state.get_state()
        if current_state and current_state.startswith("Onboarding"):
            return await handler(event, data)

    if not await is_subscribed(user_id):
        lang = await get_lang(user_id, state)
        buy_text = t(lang, "buy_subscription_btn")
        buy_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=buy_text, callback_data="buy_subscription")]
        ])
        msg = t(lang, "subscription_expired")
        if isinstance(event, Message):
            await event.answer(msg, reply_markup=buy_kb)
        elif isinstance(event, CallbackQuery):
            await event.answer("Subscription expired", show_alert=True)
            try:
                await bot.send_message(chat_id=event.from_user.id, text=msg, reply_markup=buy_kb)
            except:
                pass
        return True

    return await handler(event, data)

router.message.middleware.register(subscription_middleware)
router.callback_query.middleware.register(subscription_middleware)

# --- Хэндлеры ---
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(
            t("ru", "welcome"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Русский", callback_data="lang_ru"),
                 InlineKeyboardButton(text="English", callback_data="lang_en")]
            ])
        )
        await state.set_state(Onboarding.language)
    else:
        lang = profile.language
        sub = await is_subscribed(user_id)
        daily_cal = calculate_daily_calories(profile)
        goal_text = t(lang, f"goal.{profile.goal}")
        msg = t(lang, "returning", goal=goal_text, calories=daily_cal)
        if not sub:
            msg += "\n\n" + t(lang, "subscription_expired")
        await message.answer(msg, reply_markup=await build_main_menu(user_id, lang))

@router.callback_query(StateFilter(Onboarding.language))
async def process_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split("_")[1]
    await state.update_data(language=lang)
    await callback.message.edit_text(t(lang, "onboarding.weight"))
    await state.set_state(Onboarding.weight)
    await callback.answer()

@router.message(StateFilter(Onboarding.weight))
async def process_weight(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    try:
        weight = float(message.text.replace(",", "."))
        await state.update_data(weight=weight)
        await message.answer(t(lang, "onboarding.height"))
        await state.set_state(Onboarding.height)
    except ValueError:
        await message.answer(t(lang, "onboarding.weight"))

@router.message(StateFilter(Onboarding.height))
async def process_height(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    try:
        height = float(message.text.replace(",", "."))
        await state.update_data(height=height)
        await message.answer(t(lang, "onboarding.age"))
        await state.set_state(Onboarding.age)
    except ValueError:
        await message.answer(t(lang, "onboarding.height"))

@router.message(StateFilter(Onboarding.age))
async def process_age(message: Message, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    try:
        age = int(message.text)
        await state.update_data(age=age)
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
    data = await state.get_data()
    lang = data.get("language", "ru")
    await state.update_data(gender=callback.data.split("_")[1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, f"activity.{level}"), callback_data=f"activity_{level}")]
        for level in ["sedentary", "light", "moderate", "active"]
    ])
    await callback.message.edit_text(t(lang, "onboarding.activity"), reply_markup=kb)
    await state.set_state(Onboarding.activity)
    await callback.answer()

@router.callback_query(StateFilter(Onboarding.activity))
async def process_activity(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
    await state.update_data(activity=callback.data.split("_")[1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, f"goal.{goal}"), callback_data=f"goal_{goal}")]
        for goal in ["lose", "gain", "maintain"]
    ])
    await callback.message.edit_text(t(lang, "onboarding.goal"), reply_markup=kb)
    await state.set_state(Onboarding.goal)
    await callback.answer()

@router.callback_query(StateFilter(Onboarding.goal))
async def process_goal(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = data.get("language", "ru")
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
        user_id=message.from_user.id,
        language=lang,
        weight=data["weight"],
        height=data["height"],
        age=data["age"],
        gender=data["gender"],
        activity=data["activity"],
        goal=data["goal"],
        allergies=allergies,
    )
    await save_user(profile)
    daily_cal = calculate_daily_calories(profile)
    goal_text = t(lang, f"goal.{profile.goal}")

    if message.from_user.id == ADMIN_ID:
        await message.answer(
            t(lang, "profile_created_admin", goal=goal_text, calories=daily_cal),
            reply_markup=await build_main_menu(message.from_user.id, lang)
        )
    else:
        await activate_trial(message.from_user.id)
        await message.answer(
            t(lang, "profile_created", goal=goal_text, calories=daily_cal),
            reply_markup=await build_main_menu(message.from_user.id, lang)
        )
    await state.clear()

# --- Обработчики кнопок главного меню ---
@router.message(F.text.in_([t("ru", "food_diary_menu"), t("en", "food_diary_menu")]))
async def food_diary_menu(message: Message, state: FSMContext):
    user_id = message.from_user.id
    lang = await get_lang(user_id, state)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "add_food_btn"), callback_data="add_food")],
        [InlineKeyboardButton(text=t(lang, "show_today_btn"), callback_data="show_today")]
    ])
    await message.answer(t(lang, "food_diary_menu"), reply_markup=kb)

@router.message(F.text.in_([t("ru", "profile"), t("en", "profile")]))
async def profile_menu(message: Message):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(t("ru", "profile_not_found"))
        return
    lang = profile.language
    goal_map = {
        "lose": t(lang, "goal.lose"),
        "gain": t(lang, "goal.gain"),
        "maintain": t(lang, "goal.maintain")
    }
    activity_map = {
        "sedentary": t(lang, "activity.sedentary"),
        "light": t(lang, "activity.light"),
        "moderate": t(lang, "activity.moderate"),
        "active": t(lang, "activity.active")
    }
    text = (
        f"*{t(lang, 'profile')}*\n"
        f"{t(lang, 'edit_field.weight')}: {profile.weight} kg\n"
        f"{t(lang, 'edit_field.height')}: {profile.height} cm\n"
        f"{t(lang, 'edit_field.age')}: {profile.age}\n"
        f"{t(lang, 'edit_field.gender')}: {t(lang, f'gender.{profile.gender}')}\n"
        f"{t(lang, 'edit_field.activity')}: {activity_map.get(profile.activity, profile.activity)}\n"
        f"{t(lang, 'edit_field.goal')}: {goal_map.get(profile.goal, profile.goal)}\n"
        f"{t(lang, 'edit_field.allergies')}: {profile.allergies or 'none'}\n"
        f"{t(lang, 'edit_field.favorite_foods')}: {profile.favorite_foods or '—'}\n"
        f"{t(lang, 'edit_field.disliked_foods')}: {profile.disliked_foods or '—'}\n"
        f"{t(lang, 'edit_field.sport_types')}: {profile.sport_types or '—'}\n"
        f"{t(lang, 'edit_field.habits')}: {profile.habits or '—'}\n"
        f"{t(lang, 'edit_field.language')}: {profile.language}\n"
        f"Daily norm: {calculate_daily_calories(profile):.0f} {t(lang, 'kcal_unit')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚖️ " + t(lang, "edit_field.weight"), callback_data="edit_field_weight"),
         InlineKeyboardButton(text="📏 " + t(lang, "edit_field.height"), callback_data="edit_field_height"),
         InlineKeyboardButton(text="🎂 " + t(lang, "edit_field.age"), callback_data="edit_field_age")],
        [InlineKeyboardButton(text="🚻 " + t(lang, "edit_field.gender"), callback_data="edit_field_gender"),
         InlineKeyboardButton(text="🏃 " + t(lang, "edit_field.activity"), callback_data="edit_field_activity"),
         InlineKeyboardButton(text="🎯 " + t(lang, "edit_field.goal"), callback_data="edit_field_goal")],
        [InlineKeyboardButton(text="⚠️ " + t(lang, "edit_field.allergies"), callback_data="edit_field_allergies"),
         InlineKeyboardButton(text="🍕 " + t(lang, "edit_field.favorite_foods"), callback_data="edit_field_favorite_foods"),
         InlineKeyboardButton(text="🚫 " + t(lang, "edit_field.disliked_foods"), callback_data="edit_field_disliked_foods")],
        [InlineKeyboardButton(text="🏅 " + t(lang, "edit_field.sport_types"), callback_data="edit_field_sport_types"),
         InlineKeyboardButton(text="🌙 " + t(lang, "edit_field.habits"), callback_data="edit_field_habits")],
        [InlineKeyboardButton(text="🌐 " + t(lang, "edit_field.language"), callback_data="edit_field_language")]
    ])
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)

@router.message(F.text.in_([t("ru", "meal_plan"), t("en", "meal_plan")]))
async def meal_plan_menu(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "plan_1_day"), callback_data="plan_1")],
        [InlineKeyboardButton(text=t(lang, "plan_2_days"), callback_data="plan_2")]
    ])
    await message.answer(t(lang, "plan_choose_days"), reply_markup=kb)

@router.message(F.text.in_([t("ru", "reminders"), t("en", "reminders")]))
async def reminders_menu(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    reminders = await get_user_reminders(user_id)
    if not reminders:
        await message.answer(t(lang, "reminders_empty"))
        return

    lines = [f"⏰ {r['time']} – {r['text']}" for r in reminders]
    text = t(lang, "reminders_list", items="\n".join(lines))

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {r['time']} {r['text']}", callback_data=f"del_rem_{r['id']}")]
        for r in reminders
    ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="➕ " + t(lang, "add_food_btn").split()[-1], callback_data="add_reminder_info")])

    await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

@router.message(F.text.in_([t("ru", "subscription"), t("en", "subscription")]))
async def subscription_menu(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "buy_subscription_btn"), callback_data="buy_subscription")]
    ])
    await message.answer(t(lang, "subscription_menu"), reply_markup=kb)

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
    await callback.message.delete()
    lang = await get_lang(user_id)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entries = await get_daily_food(user_id, today_str)
    if not entries:
        await bot.send_message(user_id, t(lang, "no_entries"))
        await callback.answer()
        return

    total_cal = sum(e['calories'] for e in entries)
    total_prot = sum(e['protein'] for e in entries)
    total_fat = sum(e['fat'] for e in entries)
    total_carbs = sum(e['carbs'] for e in entries)
    unit = t(lang, "kcal_unit")

    lines = [f"{e['meal_time']} - {e['description']}: {e['calories']} {unit}" for e in entries]
    text = "\n".join(lines) + "\n\n" + t(lang, "today_summary", cal=total_cal, p=total_prot, f=total_fat, c=total_carbs)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {e['meal_time']} {e['description']}", callback_data=f"del_entry_{e['id']}")]
        for e in entries
    ])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔄 " + t(lang, "show_today_btn"), callback_data="show_today")])

    await bot.send_message(user_id, text, reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data.startswith("del_entry_"))
async def delete_entry_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await get_lang(user_id)
    try:
        entry_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Invalid entry ID.", show_alert=True)
        return

    success = await delete_food_entry(entry_id, user_id)
    if success:
        await callback.message.delete()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        entries = await get_daily_food(user_id, today_str)
        if not entries:
            await bot.send_message(user_id, t(lang, "no_entries"))
        else:
            total_cal = sum(e['calories'] for e in entries)
            total_prot = sum(e['protein'] for e in entries)
            total_fat = sum(e['fat'] for e in entries)
            total_carbs = sum(e['carbs'] for e in entries)
            unit = t(lang, "kcal_unit")
            lines = [f"{e['meal_time']} - {e['description']}: {e['calories']} {unit}" for e in entries]
            text = "\n".join(lines) + "\n\n" + t(lang, "today_summary", cal=total_cal, p=total_prot, f=total_fat, c=total_carbs)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"❌ {e['meal_time']} {e['description']}", callback_data=f"del_entry_{e['id']}")]
                for e in entries
            ])
            keyboard.inline_keyboard.append([InlineKeyboardButton(text="🔄 " + t(lang, "show_today_btn"), callback_data="show_today")])
            await bot.send_message(user_id, text, reply_markup=keyboard)
        await callback.answer(t(lang, "entry_deleted"))
    else:
        await callback.answer("Could not delete entry.", show_alert=True)

@router.message(StateFilter(FoodInput.waiting_for_text_or_photo))
async def handle_food_input(message: Message, state: FSMContext):
    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
        await message.answer(t("ru", "create_profile_first"))
        return
    lang = profile.language

    if message.text and is_nutrition_question(message.text):
        await handle_nutrition_question(message, state, profile, lang)
        return

    status_msg = await message.answer(t(lang, "plan_generating"))

    if message.photo:
        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        photo_bytes = await bot.download_file(file.file_path)
        result = await analyze_food_photo(photo_bytes.read(), lang)
    else:
        result = await analyze_food_text(message.text, lang)

    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
    except TelegramBadRequest:
        pass

    if "error" in result:
        error_type = result.get("error", "")
        if error_type == "not_food":
            msg = result.get("message", "")
            if "seek medical help" in msg.lower():
                await message.answer(t(lang, "not_food_danger"), reply_markup=await build_main_menu(user_id, lang))
            else:
                await message.answer(t(lang, "not_food"), reply_markup=await build_main_menu(user_id, lang))
        else:
            logging.error(f"Food analysis error: {result}")
            await message.answer(t(lang, "analysis_failed"), reply_markup=await build_main_menu(user_id, lang))
        await state.clear()
        return

    food_name = result.get('food_name', 'Meal')
    calories = result.get('calories', 0)
    protein = result.get('protein', 0)
    fat = result.get('fat', 0)
    carbs = result.get('carbs', 0)
    unit = t(lang, "kcal_unit")

    await state.update_data(pending_food={
        "description": food_name,
        "calories": calories,
        "protein": protein,
        "fat": fat,
        "carbs": carbs
    })

    text = (
        f"🍽 *{food_name}*\n"
        f"Calories: {calories} {unit}\n"
        f"Protein: {protein}g | Fat: {fat}g | Carbs: {carbs}g"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t(lang, "confirm_add"), callback_data="confirm_add")],
        [InlineKeyboardButton(text=t(lang, "cancel_food"), callback_data="cancel_food")]
    ])
    await message.answer(text, parse_mode="Markdown", reply_markup=kb)
    await state.set_state(FoodInput.confirm_add)

async def handle_nutrition_question(message: Message, state: FSMContext, profile: UserProfile, lang: str):
    status_msg = await message.answer(t(lang, "nutritionist_generating"))
    try:
        answer = await ask_nutritionist(message, profile, lang)
        await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
        await message.answer(answer, reply_markup=await build_main_menu(message.from_user.id, lang))
    except Exception as e:
        logging.error(f"Nutrition question error: {e}")
        await bot.delete_message(chat_id=message.chat.id, message_id=status_msg.message_id)
        await message.answer(t(lang, "error_generic"))
    finally:
        await state.clear()

@router.callback_query(F.data == "confirm_add", StateFilter(FoodInput.confirm_add))
async def confirm_add_food(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pending = data.get("pending_food")
    if not pending:
        await callback.answer("No data", show_alert=True)
        return
    user_id = callback.from_user.id
    lang = await get_lang(user_id, state)
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    meal_time = now.strftime("%H:%M")
    await add_food_entry(user_id, today_str, meal_time,
                         pending["description"], pending["calories"],
                         pending["protein"], pending["fat"], pending["carbs"])
    await callback.message.edit_text(t(lang, "food_added", food=pending['description']))
    await state.clear()
    await callback.answer()

@router.callback_query(F.data == "cancel_food", StateFilter(FoodInput.confirm_add))
async def cancel_food(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    await callback.message.edit_text(t(lang, "food_cancelled"))
    await state.clear()
    await callback.answer()

# --- Редактирование профиля ---
CLEARABLE_FIELDS = {"allergies", "favorite_foods", "disliked_foods", "sport_types", "habits"}

def get_current_field_value(profile: UserProfile, field: str, lang: str) -> str:
    if field == "gender":
        return t(lang, f"gender.{profile.gender}")
    elif field == "activity":
        return t(lang, f"activity.{profile.activity}")
    elif field == "goal":
        return t(lang, f"goal.{profile.goal}")
    elif field == "language":
        return profile.language
    else:
        val = getattr(profile, field, "")
        if isinstance(val, str) and not val:
            return "—"
        return str(val)

@router.callback_query(F.data.startswith("edit_field_"))
async def edit_field_start(callback: CallbackQuery, state: FSMContext):
    field = callback.data[len("edit_field_"):]
    user_id = callback.from_user.id
    lang = await get_lang(user_id, state)
    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t(lang, "profile_not_found"), show_alert=True)
        return

    current_val = get_current_field_value(profile, field, lang)
    field_display = t(lang, f"edit_field.{field}")
    current_text = t(lang, "current_value", value=current_val)

    prompts = {f: f"{t(lang, f'edit_field.{f}')}\n{current_text}" for f in ["weight","height","age","gender","activity","goal","allergies","favorite_foods","disliked_foods","sport_types","habits","language"]}

    await state.update_data(edit_field=field)
    await state.set_state(EditProfile.waiting_for_value)

    if field in ("gender", "activity", "goal", "language"):
        kb = None
        if field == "gender":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, "gender.male"), callback_data="set_gender_male"),
                 InlineKeyboardButton(text=t(lang, "gender.female"), callback_data="set_gender_female")]
            ])
        elif field == "activity":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, f"activity.{level}"), callback_data=f"set_activity_{level}")]
                for level in ["sedentary", "light", "moderate", "active"]
            ])
        elif field == "goal":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=t(lang, f"goal.{goal}"), callback_data=f"set_goal_{goal}")]
                for goal in ["lose", "gain", "maintain"]
            ])
        elif field == "language":
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Русский", callback_data="set_language_ru"),
                 InlineKeyboardButton(text="English", callback_data="set_language_en")]
            ])
        await callback.message.edit_text(prompts[field], reply_markup=kb)
    else:
        if field in CLEARABLE_FIELDS:
            clear_btn_text = t(lang, "edit_field.clear")
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=clear_btn_text, callback_data=f"clear_field_{field}")]
            ])
            await callback.message.edit_text(prompts[field], reply_markup=kb)
        else:
            await callback.message.edit_text(prompts[field])

    await callback.answer()

CALORIE_AFFECTING_FIELDS = {"weight", "height", "age", "gender", "activity", "goal"}

@router.message(StateFilter(EditProfile.waiting_for_value))
async def edit_field_value(message: Message, state: FSMContext):
    data = await state.get_data()
    field = data.get("edit_field")
    if not field:
        await message.answer(t("ru", "error_try_again"))
        await state.clear()
        return

    user_id = message.from_user.id
    profile = await get_user(user_id)
    if not profile:
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
    elif field == "allergies":
        if value.lower() in ("нет", "none"):
            value = ""

    setattr(profile, field, value)
    await save_user(profile)

    if field in CALORIE_AFFECTING_FIELDS:
        msg = t(lang, "field_updated_with_norm", field=field, calories=calculate_daily_calories(profile))
    else:
        msg = t(lang, "field_updated", field=field)

    await message.answer(msg, reply_markup=await build_main_menu(user_id, lang))
    await state.clear()

@router.callback_query(F.data.startswith("set_gender_"))
async def set_gender(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    gender = callback.data.split("_")[2]
    profile = await get_user(callback.from_user.id)
    if profile:
        profile.gender = gender
        await save_user(profile)
        await callback.message.edit_text(t(lang, "field_updated_with_norm", field="gender", calories=calculate_daily_calories(profile)))
    await state.clear()
    await callback.answer()

@router.callback_query(F.data.startswith("set_activity_"))
async def set_activity(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    activity = callback.data.split("_")[2]
    if activity not in ["sedentary", "light", "moderate", "active"]:
        await callback.answer("Invalid activity level", show_alert=True)
        return
    profile = await get_user(callback.from_user.id)
    if profile:
        profile.activity = activity
        await save_user(profile)
        await callback.message.edit_text(t(lang, "field_updated_with_norm", field="activity", calories=calculate_daily_calories(profile)))
    await state.clear()
    await callback.answer()

@router.callback_query(F.data.startswith("set_goal_"))
async def set_goal(callback: CallbackQuery, state: FSMContext):
    lang = await get_lang(callback.from_user.id, state)
    goal = callback.data.split("_")[2]
    profile = await get_user(callback.from_user.id)
    if profile:
        profile.goal = goal
        await save_user(profile)
        await callback.message.edit_text(t(lang, "field_updated_with_norm", field="goal", calories=calculate_daily_calories(profile)))
    await state.clear()
    await callback.answer()

@router.callback_query(F.data.startswith("set_language_"))
async def set_language(callback: CallbackQuery, state: FSMContext):
    lang = callback.data.split("_")[2]
    profile = await get_user(callback.from_user.id)
    if profile:
        profile.language = lang
        await save_user(profile)
        await callback.message.edit_text(t(lang, "field_updated", field="language"))
    await state.clear()
    await callback.answer()

# --- Очистка полей ---
@router.callback_query(F.data.startswith("clear_field_"))
async def clear_field_handler(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    lang = await get_lang(user_id, state)
    field = callback.data[len("clear_field_"):]

    if field not in CLEARABLE_FIELDS:
        await callback.answer("Cannot clear this field", show_alert=True)
        return

    profile = await get_user(user_id)
    if not profile:
        await callback.answer(t(lang, "profile_not_found"), show_alert=True)
        return

    setattr(profile, field, "")
    await save_user(profile)
    await state.clear()

    field_display = t(lang, f"edit_field.{field}")
    await callback.message.edit_text(t(lang, "field_cleared", field=field_display))
    await callback.answer()

# --- План питания ---
@router.callback_query(F.data.in_(["plan_1", "plan_2"]))
async def generate_plan(callback: CallbackQuery):
    days = int(callback.data.split("_")[1])
    profile = await get_user(callback.from_user.id)
    if not profile:
        await callback.answer("Create a profile first", show_alert=True)
        return
    lang = profile.language
    await callback.message.edit_text(t(lang, "plan_generating"))
    await callback.answer()

    plan_text = await generate_meal_plan(profile, days)

    if len(plan_text) <= 4000:
        await callback.message.edit_text(plan_text, parse_mode="Markdown")
    else:
        await callback.message.edit_text("Plan ready, sending in parts...")
        for i in range(0, len(plan_text), 4000):
            await bot.send_message(callback.from_user.id, plan_text[i:i+4000], parse_mode="Markdown")

# --- Напоминания ---
@router.callback_query(F.data == "add_reminder_info")
async def add_reminder_info(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await callback.answer()
    await callback.message.answer(t(lang, "reminder_add_info"))

@router.callback_query(F.data.startswith("del_rem_"))
async def delete_reminder_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    lang = await get_lang(user_id)
    try:
        rem_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("Invalid ID.", show_alert=True)
        return

    success = await delete_reminder(rem_id, user_id)
    if success:
        await callback.answer(t(lang, "reminder_deleted"))
        reminders = await get_user_reminders(user_id)
        if not reminders:
            await callback.message.edit_text(t(lang, "reminders_empty"), reply_markup=None)
        else:
            lines = [f"⏰ {r['time']} – {r['text']}" for r in reminders]
            text = t(lang, "reminders_list", items="\n".join(lines))
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"❌ {r['time']} {r['text']}", callback_data=f"del_rem_{r['id']}")]
                for r in reminders
            ])
            keyboard.inline_keyboard.append([InlineKeyboardButton(text="➕ " + t(lang, "add_food_btn").split()[-1], callback_data="add_reminder_info")])
            await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)
    else:
        await callback.answer("Could not delete.", show_alert=True)

@router.message(F.regexp(r"^\d{1,2}[:.]\d{2}\s+.+"))
async def add_reminder(message: Message):
    user_id = message.from_user.id
    lang = await get_lang(user_id)
    text = message.text.strip()
    parts = text.split(" ", 1)
    time_str = parts[0].replace(".", ":")
    reminder_text = parts[1]

    try:
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        normalized_time = f"{hour:02d}:{minute:02d}"
    except:
        await message.answer(t(lang, "reminder_add_info"))
        return

    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO reminders (user_id, time, text) VALUES ($1, $2::time, $3)",
                           user_id, normalized_time, reminder_text)

    await message.answer(t(lang, "reminder_added", time=normalized_time, text=reminder_text))

# --- Платежи ---
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
        need_email=False,
        send_email_to_provider=False,
    )
    await callback.answer()

@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)

@router.message(F.successful_payment)
async def successful_payment(message: Message):
    await update_subscription(message.from_user.id, 30)
    profile = await get_user(message.from_user.id)
    lang = profile.language if profile else "ru"
    await message.answer(t(lang, "payment_success"), reply_markup=await build_main_menu(message.from_user.id, lang))

# --- Административная команда ---
@router.message(Command("gift"))
async def gift_subscription(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        _, target_id, days = message.text.split()
        target_id = int(target_id)
        days = int(days)
        await update_subscription(target_id, days)
        await message.answer(t("ru", "gift_success", id=target_id, days=days))
    except Exception as e:
        await message.answer(t("ru", "gift_error"))

# --- Универсальный обработчик ---
@router.message(F.content_type.in_({'text', 'photo'}))
async def universal_food_input(message: Message, state: FSMContext):
    if state and await state.get_state() in Onboarding.__states__:
        return
    if state and await state.get_state() == "EditProfile:waiting_for_value":
        return

    if message.text:
        t_lower = message.text.lower()
        if message.text in [
            t("ru", "food_diary_menu"), t("en", "food_diary_menu"),
            t("ru", "profile"), t("en", "profile"),
            t("ru", "meal_plan"), t("en", "meal_plan"),
            t("ru", "reminders"), t("en", "reminders"),
            t("ru", "subscription"), t("en", "subscription")
        ]:
            return
        if re.match(r'^\d{1,2}[:.]\d{2}\s+.+', message.text):
            return await add_reminder(message)

        if is_nutrition_question(message.text):
            profile = await get_user(message.from_user.id)
            if not profile:
                await message.answer(t("ru", "create_profile_first"))
                return
            lang = profile.language
            await handle_nutrition_question(message, state, profile, lang)
            return

    await handle_food_input(message, state)

@router.callback_query()
async def unhandled_callback(callback: CallbackQuery):
    await callback.answer("This action is not supported or outdated.")

# --- Фоновые задачи ---
async def reminder_scheduler():
    while True:
        now = datetime.now(timezone.utc)
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM reminders WHERE active=1")
            for row in rows:
                rem_time = row['time']
                user = await get_user(row['user_id'])
                offset = user.utc_offset if user else 0
                local_now = now + timedelta(hours=offset)
                if local_now.hour == rem_time.hour and local_now.minute == rem_time.minute:
                    try:
                        await bot.send_message(row['user_id'], f"⏰ {row['text']}")
                    except:
                        pass
        await asyncio.sleep(60)

async def daily_summary_scheduler():
    while True:
        now = datetime.now(timezone.utc)
        async with db_pool.acquire() as conn:
            users = await conn.fetch("SELECT user_id, utc_offset, language FROM users")
            for user in users:
                user_id = user['user_id']
                offset = user['utc_offset']
                lang = user['language'] or 'ru'
                local_hour = (now + timedelta(hours=offset)).hour
                if local_hour == 21:
                    today_str = now.strftime("%Y-%m-%d")
                    entries = await get_daily_food(user_id, today_str)
                    if entries:
                        total_cal = sum(e['calories'] for e in entries)
                        total_prot = sum(e['protein'] for e in entries)
                        total_fat = sum(e['fat'] for e in entries)
                        total_carbs = sum(e['carbs'] for e in entries)
                        profile = await get_user(user_id)
                        if profile:
                            norm = calculate_daily_calories(profile)
                            diff = norm - total_cal
                            if diff > 100:
                                advice = t(lang, "daily_summary_under")
                            elif diff < -100:
                                advice = t(lang, "daily_summary_over")
                            else:
                                advice = t(lang, "daily_summary_ok")
                            text = t(lang, "daily_summary", date=today_str, consumed=total_cal, norm=norm,
                                     p=total_prot, f=total_fat, c=total_carbs, advice=advice)
                            try:
                                await bot.send_message(user_id, text, parse_mode="Markdown")
                            except:
                                pass
        await asyncio.sleep(3600)

# --- Запуск ---
async def main():
    await init_db()
    asyncio.create_task(reminder_scheduler())
    asyncio.create_task(daily_summary_scheduler())
    try:
        await openai_client.models.list()
        logging.info("gemini-web2api доступен")
    except Exception as e:
        logging.warning(f"gemini-web2api недоступен: {e}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())