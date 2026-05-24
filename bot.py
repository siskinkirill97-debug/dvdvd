import asyncio
import sqlite3
import json
import logging
import time
from datetime import datetime
from io import BytesIO
import google.generativeai as genai

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, LabeledPrice, PreCheckoutQuery

# Импортируем планировщик задач
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# =====================================================================
# НАСТРОЙКА КЛЮЧЕЙ И ДОСТУПОВ (ЗАПОЛНИ ЭТИ ПОЛЯ)
# =====================================================================
BOT_TOKEN = "СЮДА_ВСТАВЬ_ТОКЕН_ИЗ_BOTFATHER"
GEMINI_API_KEY = "СЮДА_ВСТАВЬ_API_КЛЮЧ_ИЗ_GOOGLE_AI_STUDIO"

ADMIN_ID = 123456789  # Вставь свой Telegram ID
VIP_USERS = [123456789]  # Твой ID и ID друзей для вечного безлимита

SUBSCRIPTION_PRICE_STARS = 100  # Цена подписки на 30 дней

# =====================================================================
# ИНИЦИАЛИЗАЦИЯ И ЛОГИРОВАНИЕ
# =====================================================================
logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
genai.configure(api_key=GEMINI_API_KEY)

# =====================================================================
# БАЗА ДАННЫХ (SQLite)
# =====================================================================
def init_db():
    conn = sqlite3.connect("nutrition_bot.db")
    cursor = conn.cursor()
    # Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            weight INTEGER,
            height INTEGER,
            age INTEGER,
            goal TEXT,
            allergies TEXT,
            kcal_target INTEGER,
            subscription_expires INTEGER DEFAULT 0
        )
    """)
    # НОВАЯ ТАБЛИЦА: Логи еды за день
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS food_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            log_date TEXT,
            kcal INTEGER,
            b INTEGER,
            z INTEGER,
            u INTEGER
        )
    """)
    conn.commit()
    conn.close()

# =====================================================================
# МАШИНА СОСТОЯНИЙ (FSM) ДЛЯ ОПРОСА
# =====================================================================
class OnboardingStates(StatesGroup):
    waiting_for_weight = State()
    waiting_for_height = State()
    waiting_for_age = State()
    waiting_for_goal = State()
    waiting_for_allergies = State()

# =====================================================================
# ФУНКЦИЯ ВЗАИМОДЕЙСТВИЯ С GEMINI API
# =====================================================================
async def ask_gemini_nutrition(user_food_data: str, user_allergies: str, is_photo: bool = False, photo_bytes: bytes = None):
    system_instruction = f"""
    Ты — профессиональный, приземленный к реальной жизни ИИ-нутрициолог. Твоя задача — проанализировать еду пользователя по фото или тексту.
    Ты обязан вернуть ответ строго в формате JSON (и ничего кроме него):
    {{
      "dish_name": "Название блюда",
      "kcal": 320,
      "b": 15, "z": 10, "u": 42,
      "comment": "Текст краткого анализа и рекомендаций"
    }}
    
    КРИТИЧЕСКИЕ ПРАВИЛА для поля "comment":
    1. У пользователя АЛЛЕРГИЯ на: {user_allergies}. Категорически запрещено рекомендовать эти продукты!
    2. Твои советы должны быть реалистичными и бюджетными. Избегай дорогих "суперфудов". 
    3. Если ты рекомендуешь дорогой продукт, ты ОБЯЗАН в скобках сразу же написать его доступный и дешевый аналог, выполняющий ту же функцию.
    """
    
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config={"response_mime_type": "application/json"},
        system_instruction=system_instruction
    )

    if is_photo:
        cookie_picture = {'mime_type': 'image/jpeg', 'data': photo_bytes}
        response = model.generate_content([cookie_picture, "Что на этой картинке? Оцени блюдо и посчитай КБЖУ."])
    else:
        response = model.generate_content(user_food_data)
        
    clean_json = response.text.replace("```json", "").replace("```", "").strip()
    return json.loads(clean_json)

# =====================================================================
# ХЭНДЛЕРЫ ОПРОСА (ОНБОРДИНГ)
# =====================================================================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await message.answer(
        "Привет! 🍏 Я твой умный ИИ-нутрициолог.\n"
        "Давай настроим твой профиль, чтобы расчеты были точными.\n\n"
        "Введи твой текущий вес (в кг), например: 75",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(OnboardingStates.waiting_for_weight)

@router.message(OnboardingStates.waiting_for_weight)
async def process_weight(message: Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("Пожалуйста, введи только число.")
    await state.update_data(weight=int(message.text))
    await message.answer("Отлично! Теперь введи свой рост (в см), например: 178")
    await state.set_state(OnboardingStates.waiting_for_height)

@router.message(OnboardingStates.waiting_for_height)
async def process_height(message: Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("Пожалуйста, введи только число.")
    await state.update_data(height=int(message.text))
    await message.answer("И твой возраст (полных лет):")
    await state.set_state(OnboardingStates.waiting_for_age)

@router.message(OnboardingStates.waiting_for_age)
async def process_age(message: Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("Пожалуйста, введи только число.")
    await state.update_data(age=int(message.text))
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Похудеть"), KeyboardButton(text="Набрать массу")], [KeyboardButton(text="Здоровый баланс")]],
        resize_keyboard=True
    )
    await message.answer("Какая у тебя главная цель?", reply_markup=kb)
    await state.set_state(OnboardingStates.waiting_for_goal)

@router.message(OnboardingStates.waiting_for_goal)
async def process_goal(message: Message, state: FSMContext):
    if message.text not in ["Похудеть", "Набрать массу", "Здоровый баланс"]:
        return await message.answer("Выбери вариант кнопкой.")
    await state.update_data(goal=message.text)
    
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Нет аллергий")]], resize_keyboard=True)
    await message.answer("Есть ли у тебя аллергия на продукты? Напиши текстом или нажми кнопку:", reply_markup=kb)
    await state.set_state(OnboardingStates.waiting_for_allergies)

@router.message(OnboardingStates.waiting_for_allergies)
async def process_allergies(message: Message, state: FSMContext):
    user_data = await state.get_data()
    allergies = message.text if message.text != "Нет аллергий" else "Нет"
    
    weight = user_data['weight']
    height = user_data['height']
    age = user_data['age']
    
    bmr = int(10 * weight + 6.25 * height - 5 * age - 80)
    if user_data['goal'] == "Похудеть":
        kcal_target = int(bmr * 1.2 - 300)
    elif user_data['goal'] == "Набрать массу":
        kcal_target = int(bmr * 1.4 + 300)
    else:
        kcal_target = int(bmr * 1.2)

    trial_expires = int(time.time()) + (3 * 24 * 60 * 60)

    conn = sqlite3.connect("nutrition_bot.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO users (user_id, weight, height, age, goal, allergies, kcal_target, subscription_expires)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (message.from_user.id, weight, height, age, user_data['goal'], allergies, kcal_target, trial_expires))
    conn.commit()
    conn.close()

    trial_date = datetime.fromtimestamp(trial_expires).strftime('%d.%m.%Y')

    await message.answer(
        f"🎉 *Профиль успешно настроен!*\n\n"
        f"📊 Твой суточный ориентир: *{kcal_target} ккал*\n\n"
        f"🎁 Тебе начислен бесплатный тест-драйв! Полный доступ открыт до: *{trial_date}*.\n"
        f"Просто пришли фото тарелки или напиши текст!",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.clear()

# =====================================================================
# ОПЛАТА ПОДПИСКИ (30 ДНЕЙ)
# =====================================================================
@router.message(Command("buy"))
async def buy_subscription(message: Message):
    await message.answer_invoice(
        title="Подписка на 30 дней",
        description=f"Безлимитный доступ к ИИ-нутрициологу на 1 месяц.",
        payload="buy_30_days_sub",
        currency="XTR",
        prices=[LabeledPrice(label="Премиум доступ", amount=SUBSCRIPTION_PRICE_STARS)]
    )

@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    await pre_checkout_query.answer(ok=True)

@router.message(F.successful_payment)
async def success_payment_handler(message: Message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload
    
    if payload == "buy_30_days_sub":
        current_time = int(time.time())
        seconds_in_30_days = 30 * 24 * 60 * 60
        
        conn = sqlite3.connect("nutrition_bot.db")
        cursor = conn.cursor()
        cursor.execute("SELECT subscription_expires FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        
        current_expires = row[0] if row else 0
        if current_expires > current_time:
            new_expires = current_expires + seconds_in_30_days
        else:
            new_expires = current_time + seconds_in_30_days
            
        cursor.execute("UPDATE users SET subscription_expires = ? WHERE user_id = ?", (new_expires, user_id))
        conn.commit()
        conn.close()
        
        readable_date = datetime.fromtimestamp(new_expires).strftime('%d.%m.%Y')
        await message.answer(f"🎉 *Оплата прошла успешно!* Подписка активна и действует до: *{readable_date}*. Спасибо, что вы с нами!")

# =====================================================================
# АДМИН-КОМАНДА /GIFT
# =====================================================================
@router.message(Command("gift"))
async def admin_gift_days(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        args = message.text.split()
        target_user_id = int(args[1])
        days = int(args[2])
        seconds_to_add = days * 24 * 60 * 60
        current_time = int(time.time())

        conn = sqlite3.connect("nutrition_bot.db")
        cursor = conn.cursor()
        cursor.execute("SELECT subscription_expires FROM users WHERE user_id = ?", (target_user_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return await message.answer("❌ Пользователь не найден.")

        current_expires = row[0]
        if current_expires > current_time:
            new_expires = current_expires + seconds_to_add
        else:
            new_expires = current_time + seconds_to_add

        cursor.execute("UPDATE users SET subscription_expires = ? WHERE user_id = ?", (new_expires, target_user_id))
        conn.commit()
        conn.close()

        readable_date = datetime.fromtimestamp(new_expires).strftime('%d.%m.%Y')
        await message.answer(f"✅ Успешно добавлено {days} дней. Новая дата окончания: {readable_date}")
        
        try:
            await bot.send_message(target_user_id, f"🎁 Администратор продлил вашу подписку на *{days}* дней! Она активна до {readable_date}")
        except Exception:
            pass
    except Exception:
        await message.answer("Формат: `/gift ID КОЛИЧЕСТВО_ДНЕЙ`")

# =====================================================================
# ОСНОВНОЙ ХЭНДЛЕР: ОБРАБОТКА ФОТО И ТЕКСТА ЕДЫ
# =====================================================================
@router.message(F.text | F.photo)
async def handle_food_input(message: Message):
    user_id = message.from_user.id
    current_time = int(time.time())
    
    conn = sqlite3.connect("nutrition_bot.db")
    cursor = conn.cursor()
    cursor.execute("SELECT allergies, subscription_expires FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    
    if not row:
        conn.close()
        return await message.answer("Пожалуйста, сначала настройте профиль с помощью команды /start")
    
    allergies, expires = row
    
    if user_id in VIP_USERS:
        is_sub_active = True
    else:
        is_sub_active = expires > current_time
    
    if not is_sub_active:
        conn.close()
        await message.answer(
            "⚠️ *Ваша подписка истекла или еще не была куплена.*\n\n"
            f"Оформите подписку на 30 дней за **{SUBSCRIPTION_PRICE_STARS} ⭐️** прямо сейчас, чтобы получить безлимитный доступ к разборам вашей еды:"
        )
        await message.answer_invoice(
            title="Подписка на 30 дней",
            description=f"Безлимитный доступ к ИИ-нутрициологу на 1 месяц.",
            payload="buy_30_days_sub",
            currency="XTR",
            prices=[LabeledPrice(label="Премиум доступ", amount=SUBSCRIPTION_PRICE_STARS)]
        )
        return

    await message.answer("🤖 Секунду, ИИ внимательно изучает ваше блюдо...")

    try:
        if message.photo:
            photo = message.photo[-1]
            file_info = await bot.get_file(photo.file_id)
            photo_file = BytesIO()
            await bot.download_file(file_info.file_path, destination=photo_file)
            result = await ask_gemini_nutrition("", allergies, is_photo=True, photo_bytes=photo_file.getvalue())
        else:
            result = await ask_gemini_nutrition(message.text, allergies)
        
        # ЗАПИСЫВАЕМ ДАННЫЕ В ТАБЛИЦУ ЛОГОВ ЕДЫ ДЛЯ ВЕЧЕРНЕГО ИТОГА
        today_str = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO food_logs (user_id, log_date, kcal, b, z, u)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, today_str, int(result.get('kcal', 0)), int(result.get('b', 0)), int(result.get('z', 0)), int(result.get('u', 0))))
        conn.commit()

        if user_id in VIP_USERS:
            sub_status = "Безлимит 😎"
        else:
            sub_status = datetime.fromtimestamp(expires).strftime('%d.%m.%Y')

        response_text = (
            f"🍳 *Блюдо:* {result.get('dish_name')}\n"
            f"🔥 *Калорийность:* {result.get('kcal')} ккал\n"
            f"🧬 *БЖУ:* Б: {result.get('b')}г | Ж: {result.get('z')}г | У: {result.get('u')}г\n\n"
            f"💡 *Разбор нутрициолога:* {result.get('comment')}\n\n"
            f"📅 _Подписка активна до: {sub_status}_"
        )
        await message.answer(response_text, parse_mode="Markdown")

    except Exception as e:
        await message.answer("Извините, произошла ошибка. Попробуйте описать еду обычным текстом.")
        logging.error(f"Ошибка: {e}")
    finally:
        conn.close()

# =====================================================================
# АВТОМАТИЧЕСКИЕ ПЕРИОДИЧЕСКИЕ ЗАДАЧИ (РАССЫЛКА)
# =====================================================================

# 1. Дневной пуш-пинок (Проверяет, присылал ли юзер еду сегодня)
async def send_daytime_reminders():
    today_str = datetime.now().strftime('%Y-%m-%d')
    current_time = int(time.time())
    
    conn = sqlite3.connect("nutrition_bot.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, subscription_expires FROM users")
    users = cursor.fetchall()
    
    for user_id, expires in users:
        # Проверяем, активна ли подписка, чтобы не беспокоить тех, кто не платил
        if user_id not in VIP_USERS and expires <= current_time:
            continue
            
        # Проверяем, есть ли хоть одна запись от юзера за сегодня
        cursor.execute("SELECT COUNT(*) FROM food_logs WHERE user_id = ? AND log_date = ?", (user_id, today_str))
        has_logs = cursor.fetchone()[0]
        
        if has_logs == 0:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text="🔔 *Напоминание!*\nДень уже в разгаре, а ты ещё ничего не записал в свой дневник питания. Что у тебя сегодня на обед? Отправь мне фото тарелки или текст! 🍏",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
    conn.close()

# 2. Вечерний подсчет итогов
async def send_evening_summaries():
    today_str = datetime.now().strftime('%Y-%m-%d')
    current_time = int(time.time())
    
    conn = sqlite3.connect("nutrition_bot.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, kcal_target, subscription_expires FROM users")
    users = cursor.fetchall()
    
    for user_id, kcal_target, expires in users:
        if user_id not in VIP_USERS and expires <= current_time:
            continue
            
        # Считаем сумму КБЖУ за день (TOTAL возвращает 0.0 вместо NULL, если записей нет)
        cursor.execute("""
            SELECT TOTAL(kcal), TOTAL(b), TOTAL(z), TOTAL(u) 
            FROM food_logs 
            WHERE user_id = ? AND log_date = ?
        """, (user_id, today_str))
        t_kcal, t_b, t_z, t_u = map(int, cursor.fetchone())
        
        if t_kcal == 0:
            text = "🌙 *Вечерний итог:*\nСегодня ты ничего не записывал. Надеюсь, ты просто дал себе отдохнуть от подсчетов! Жду тебя завтра. 💪"
        else:
            diff = kcal_target - t_kcal
            if diff >= 0:
                status_text = f"Ты отлично уложился в норму! До твоей цели осталось ещё *{diff} ккал*."
            else:
                status_text = f"Сегодня получился профицит (перебор) на *{abs(diff)} ккал*. Ничего страшного, завтра сделаем упор на активность!"
                
            text = (
                f"🌙 *Итоги твоего дня по питанию:*\n\n"
                f"📊 *Всего съедено за сегодня:*\n"
                f"🔥 Калории: *{t_kcal}* / {kcal_target} ккал\n"
                f"🧬 БЖУ: Б: *{t_b}г* | Ж: *{t_z}г* | У: *{t_u}г*\n\n"
                f"💡 {status_text}\n\n"
                f"Приятных снов! Ложись спать вовремя, чтобы восстановить силы. ⏰"
            )
            
        try:
            await bot.send_message(chat_id=user_id, text=text, parse_mode="Markdown")
        except Exception:
            pass
            
    conn.close()

# =====================================================================
# ЗАПУСК БОТА
# =====================================================================
async def main():
    init_db()
    dp.include_router(router)
    
    # Настраиваем планировщик фоновых задач по Московскому времени (при желании смени часовой пояс)
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    
    # Запускаем дневное напоминание в 14:00 каждый день
    scheduler.add_job(send_daytime_reminders, 'cron', hour=14, minute=0)
    
    # Запускаем вечерний итог в 21:00 каждый день
    scheduler.add_job(send_evening_summaries, 'cron', hour=21, minute=0)
    
    scheduler.start()
    logging.info("Планировщик рассылок успешно запущен!")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
