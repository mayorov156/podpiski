import asyncio
import logging
import re
from typing import Optional
import html
from decimal import Decimal
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.utils.callback_data import CallbackData

import config
from db import (
    init_db,
    get_or_create_user,
    get_user_subscriptions,
    get_user_payments,
    list_active_products,
    list_plans,
    get_plan,
    get_product,
    add_payment,
    activate_subscription,
    assign_promo_code_to_payment,
    set_payment_status,
    get_settings,
    set_setting,
    add_product,
    update_product,
    delete_product,
    list_all_products,
    add_plan,
    delete_plan,
    add_promo_codes,
    count_unused_promos,
    get_referrals_count,
    add_referral_bonus,
    get_user_by_telegram_id,
    update_user,
    add_material,
    get_materials_for_product,
    delete_material,
    get_material,
    get_all_users,
    get_plan_with_product,
    get_unused_promocode,
    attach_check,
    update_payment,
    set_product_active,
    get_payment_by_id,
    get_user_by_id,
    add_withdrawal_request,
    get_withdrawal_request_by_id,
    set_withdrawal_request_status,
    get_user_withdrawal_requests,
    get_promo_code_by_code,
    list_payments,
    get_pending_withdrawal_requests,
    get_promo_codes_by_product,
    add_promo_codes_bulk,
    delete_promo_code
)

# --- Basic setup ---
logging.basicConfig(level=logging.INFO)
bot = Bot(token=config.API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
init_db()

# --- Helper functions for consistent menu display and state management ---
async def send_main_menu(chat_id: int, message_text: str = "Главное меню:", state: FSMContext = None):
    if state:
        await state.finish()
    await bot.send_message(chat_id, message_text, reply_markup=main_user_menu())

async def send_admin_menu(chat_id: int, message_text: str = "Добро пожаловать в админ-панель!", state: FSMContext = None):
    if state:
        await state.finish()
    # Устанавливаем состояние Admin.AdminMenu
    if state:
        await state.set_state(Admin.AdminMenu)
    await bot.send_message(chat_id, message_text, reply_markup=admin_main_kb())

# --- Custom MarkdownV2 escape function ---
def markdown_escape(text: Optional[str]) -> str:
    if text is None:
        return ""
    # Characters to escape in MarkdownV2
    escape_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + char if char in escape_chars else char for char in str(text)])

# --- Custom HTML escape function ---
def html_escape(text: Optional[str]) -> str:
    if text is None:
        return ""
    return html.escape(str(text))

# --- CallbackData ---
cb_broadcast_confirm = CallbackData("broadcast", "action")
cb_admin_confirm = CallbackData("payment_action", "action", "payment_id")
cb_promo_action = CallbackData("promo", "action", "promo_id")
cb_withdraw_confirm = CallbackData("withdraw_action", "action", "request_id")
cb_admin_payments = CallbackData("admin_payments", "action", "status", "payment_id")

# --- FSM States ---
class Purchase(StatesGroup):
    EnterEmail = State()
    UploadCheck = State()
    # ShowRequisites state removed, direct transition to UploadCheck

class SubscriptionCheck(StatesGroup):
    WaitingForCheck = State()

class Admin(StatesGroup):
    AdminMenu = State() # This will be the primary admin menu state
    Requisites = State()
    EditRequisites = State()
    # ProductList state removed, navigation handled by callbacks
    AddProduct_Name = State()
    AddProduct_Desc = State()
    AddProduct_Photo = State()
    EditProduct_Desc = State()
    EditProduct_Photo = State()
    AddPlan = State()
    AddPromoCodes = State()
    # MaterialList state removed, navigation handled by callbacks
    AddMaterial_Title = State()
    AddMaterial_Content = State()
    BroadcastMessage = State()
    ConfirmBroadcast = State()
    # ApprovePayment, DeclinePayment, ApproveWithdrawal, DeclineWithdrawal are handled by callback_data directly
    # PaymentsList state removed, navigation handled by callbacks
    # ViewPaymentDetails state removed, navigation handled by callbacks
    EnterPromoCodes = State()
    # ListPromoCodes state removed, navigation handled by callbacks
    ConfirmProductDeletion = State()

class Withdrawal(StatesGroup):
    EnterAmount = State()
    EnterPhone = State()
    EnterBank = State()
    ConfirmAmount = State()

# --- Helper Functions ---
def is_admin(user_id: int) -> bool:
    return user_id in config.ADMIN_IDS

async def is_user_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=config.CHANNEL_ID, user_id=user_id)
        return member.status in ["creator", "administrator", "member"]
    except Exception as e:
        logging.error(f"Error checking subscription for {user_id}: {e}")
        return False

def subscription_required(func):
    async def wrapper(message: types.Message, state: FSMContext, *args, **kwargs):
        if not await is_user_subscribed(message.from_user.id):
            await message.answer(
                "Для доступа к этому разделу, пожалуйста, подпишитесь на наш канал. После этого нажмите кнопку ниже.",
                reply_markup=subscription_check_kb()
            )
            await SubscriptionCheck.WaitingForCheck.set()
            return
        await func(message, state, *args, **kwargs)
    return wrapper

# --- Validation Functions ---
def validate_user_exists(telegram_id: int) -> bool:
    """Проверяет существование пользователя в базе данных"""
    user = get_user_by_telegram_id(telegram_id)
    return user is not None

def validate_email(email: str) -> bool:
    """Проверяет корректность email адреса"""
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

def validate_amount(amount: str) -> bool:
    """Проверяет корректность суммы"""
    try:
        amount_float = float(amount.replace(',', '.'))
        return amount_float > 0
    except ValueError:
        return False

def validate_promo_code(code: str) -> bool:
    """Проверяет формат промокода"""
    return len(code) >= 3 and code.replace('-', '').replace('_', '').isalnum()

def validate_phone(phone: str) -> bool:
    """Проверяет формат номера телефона"""
    phone_clean = phone.replace('+', '').replace(' ', '').replace('-', '')
    return len(phone_clean) == 11 and phone_clean.startswith('7')

# --- Access Control Functions ---
def can_access_material(user_id: int, product_name: str) -> bool:
    """Проверяет права доступа пользователя к материалам продукта"""
    user = get_user_by_telegram_id(user_id)
    if not user:
        return False
    
    # Проверяем активные подписки
    subscriptions = get_user_subscriptions(user['id'])
    return any(sub['product'] == product_name and sub['active'] for sub in subscriptions)

def can_manage_product(user_id: int) -> bool:
    """Проверяет права администратора на управление товарами"""
    return is_admin(user_id)

def can_approve_payments(user_id: int) -> bool:
    """Проверяет права администратора на одобрение платежей"""
    return is_admin(user_id)

# --- Logging Functions ---
def log_user_action(user_id: int, action: str, details: str = ""):
    """Логирует действия пользователя"""
    logging.info(f"User {user_id}: {action} - {details}")

def log_admin_action(admin_id: int, action: str, details: str = ""):
    """Логирует действия администратора"""
    logging.info(f"Admin {admin_id}: {action} - {details}")

def log_payment_action(payment_id: int, action: str, details: str = ""):
    """Логирует действия с платежами"""
    logging.info(f"Payment {payment_id}: {action} - {details}")

# --- Database Safety Functions ---
@contextmanager
def safe_db_operation():
    """Контекстный менеджер для безопасных операций с БД"""
    try:
        yield
    except Exception as e:
        logging.error(f"Database error: {e}")
        raise

def safe_get_user(telegram_id: int) -> Optional[dict]:
    """Безопасно получает пользователя с обработкой ошибок"""
    try:
        return get_user_by_telegram_id(telegram_id)
    except Exception as e:
        logging.error(f"Error getting user {telegram_id}: {e}")
        return None

def safe_get_payment(payment_id: int) -> Optional[dict]:
    """Безопасно получает платеж с обработкой ошибок"""
    try:
        return get_payment_by_id(payment_id)
    except Exception as e:
        logging.error(f"Error getting payment {payment_id}: {e}")
        return None

# --- Keyboards ---
def main_user_menu() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("🛍️ Витрина"), KeyboardButton("📂 Мои подписки"))
    kb.add(KeyboardButton("👥 Реферальная программа"))
    kb.add(KeyboardButton("🎁 Материалы"))
    kb.add(KeyboardButton("💬 Поддержка"), KeyboardButton("⚙️ Настройки"))
    kb.add(KeyboardButton("🏠 Главное меню")) # Added for explicit navigation
    return kb

def store_products_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for prod in list_active_products():
        kb.add(InlineKeyboardButton(prod["name"], callback_data=f"store_prod:{prod['name']}"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu")) # Changed to back to main menu
    return kb

def plans_kb(product_name: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for plan in list_plans(product_name):
        kb.add(InlineKeyboardButton(f"{plan['name']} - {plan['price']}₽", callback_data=f"select_plan:{plan['id']}"))
    kb.add(InlineKeyboardButton("🔙 К витрине", callback_data="back_to_store_products")) # Changed to back to store
    return kb

def my_subs_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("История заказов", callback_data="subs_history"),
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu"),
    )
    return kb

def upload_check_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("❌ Отменить оплату", callback_data="cancel_purchase"))
    return kb

def admin_main_kb() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(KeyboardButton("📦 Управление товарами"), KeyboardButton("💳 Реквизиты"))
    kb.add(KeyboardButton("📊 Оплаты"), KeyboardButton("📢 Рассылка"))
    kb.add(KeyboardButton("Выход из админ-панели"))
    return kb

def admin_requisites_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📝 Изменить реквизиты", callback_data="admin_req:edit"))
    kb.add(InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_back_to_main"))
    return kb

def admin_payments_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
    InlineKeyboardButton("За сегодня", callback_data=cb_admin_payments.new(action="filter", status="today", payment_id="0")),
    InlineKeyboardButton("За неделю", callback_data=cb_admin_payments.new(action="filter", status="week", payment_id="0")),
    InlineKeyboardButton("За месяц", callback_data=cb_admin_payments.new(action="filter", status="month", payment_id="0")),
    )
    kb.add(InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_back_to_main"))
    return kb

def admin_broadcast_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Отправить", callback_data="broadcast:confirm"),
           InlineKeyboardButton("❌ Отмена", callback_data="broadcast:cancel"))
    return kb

def admin_products_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for p in list_all_products():
        status = "✅" if p["active"] else "❌"
        kb.add(InlineKeyboardButton(f"{status} {p['name']}", callback_data=f"admin_prod:view:{p['name']}"))
    kb.add(InlineKeyboardButton("➕ Добавить товар", callback_data="admin_prod:add"))
    kb.add(InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_back_to_main"))
    return kb

def admin_manage_product_kb(product_data: dict) -> InlineKeyboardMarkup:
    toggle_text = "❌ Скрыть из витрины" if product_data['active'] else "✅ Показать на витрине"
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("📝 Изменить описание", callback_data=f"admin_prod:edit_desc:{product_data['name']}"),
        InlineKeyboardButton("📸 Изменить фото", callback_data=f"admin_prod:edit_photo:{product_data['name']}"),
        InlineKeyboardButton(toggle_text, callback_data=f"admin_prod:toggle:{product_data['name']}"),
        InlineKeyboardButton("🗑️ Удалить товар (необратимо)", callback_data=f"admin_prod:delete:{product_data['name']}"),
        InlineKeyboardButton("📊 Управлять тарифами", callback_data=f"admin_plan:list:{product_data['name']}"),
        InlineKeyboardButton("➕ Добавить промокоды", callback_data=f"admin_promo:add:{product_data['name']}"),
        InlineKeyboardButton("🎟️ Промокоды", callback_data=f"admin_promo:list:{product_data['name']}"),
        InlineKeyboardButton("📚 Управлять материалами", callback_data=f"admin_mat:list:{product_data['name']}"),
        InlineKeyboardButton("🔙 К списку товаров", callback_data="admin_prod:back_list"),
    )
    return kb

def admin_promo_codes_list_kb(product_name: str, current_filter: str = "all") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("Все", callback_data=f"admin_promo:filter:{product_name}:all"),
        InlineKeyboardButton("Невыданные", callback_data=f"admin_promo:filter:{product_name}:unused"),
        InlineKeyboardButton("Выданные", callback_data=f"admin_promo:filter:{product_name}:used"),
    )
    
    kb.add(InlineKeyboardButton("➕ Добавить", callback_data=f"admin_promo:add:{product_name}"))
    kb.add(InlineKeyboardButton("🔙 К товару", callback_data=f"admin_prod:view:{product_name}"))
    return kb

def admin_plans_kb(product_name: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    plans = list_plans(product_name)
    for plan in plans:
        days = f"({plan['days']} дн.)" if plan['days'] else "(бессрочно)"
        kb.add(InlineKeyboardButton(
            f"{plan['name']} {days} - {plan['price']}₽",
            callback_data=f"admin_plan:dummy" # just for show
        ))
        kb.add(InlineKeyboardButton(
            f"🗑️ Удалить {plan['name']}",
            callback_data=f"admin_plan:delete:{plan['id']}"
        ))
    kb.add(InlineKeyboardButton("➕ Добавить тариф", callback_data=f"admin_plan:add:{product_name}"))
    kb.add(InlineKeyboardButton("🔙 К товару", callback_data=f"admin_prod:view:{product_name}"))
    return kb

def materials_for_user_kb(user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    subs = get_user_subscriptions(user_id)
    active_products = {s["product"] for s in subs if s["active"]}
    for product_name in active_products:
        kb.add(InlineKeyboardButton(product_name, callback_data=f"user_mat:list:{product_name}"))
    kb.add(InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu"))
    return kb
    
def materials_list_kb(product_name: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    materials = get_materials_for_product(product_name)
    for mat in materials:
        kb.add(InlineKeyboardButton(mat['title'], callback_data=f"user_mat:get:{mat['id']}"))
    kb.add(InlineKeyboardButton("🔙 К выбору продукта", callback_data="user_mat:back_products"))
    return kb

def subscription_check_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔗 Подписаться на канал", url=config.CHANNEL_URL))
    kb.add(InlineKeyboardButton("✅ Я подписался, получить гайд", callback_data="sub_check"))
    return kb

def referral_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔗 Моя реферальная ссылка", callback_data="referral:link"),
        InlineKeyboardButton("📊 Мой баланс", callback_data="referral:balance"),
        InlineKeyboardButton("💸 Вывести средства", callback_data="referral:withdraw_start"),
        InlineKeyboardButton("📜 История выплат", callback_data="referral:history"),
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu"),
    )
    return kb

def support_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("💬 Написать в поддержку", url=config.SUPPORT_LINK),
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu"),
    )
    return kb

def settings_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔙 Назад", callback_data="back_to_main_menu"),
    )
    return kb

def withdraw_confirm_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Подтвердить", callback_data="withdraw:confirm"),
        InlineKeyboardButton("❌ Отмена", callback_data="withdraw:cancel"),
    )
    return kb

def pay_and_upload_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Я оплатил, загрузить чек", callback_data="pay_and_upload"))
    kb.add(InlineKeyboardButton("❌ Отменить оплату", callback_data="cancel_purchase"))
    return kb

def admin_delete_confirm_kb(product_name: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"admin_prod:delete_confirm:{product_name}"),
        InlineKeyboardButton("❌ Нет, отмена", callback_data=f"admin_prod:delete_cancel:{product_name}"),
    )
    return kb

# --- Start & Main Menu ---
@dp.message_handler(commands=['start', 'menu'], state="*") # Added /menu command
@dp.message_handler(text="🏠 Главное меню", state="*") # Added for explicit button
async def cmd_start_or_menu(message: types.Message, state: FSMContext):
    await state.finish()
    
    referrer_id = None
    try:
        args = message.get_args()
        if args and args.startswith('r_'):
            try:
                referrer_id = int(args.split('_')[1])
            except (ValueError, IndexError):
                referrer_id = None
    except Exception:
        referrer_id = None
        
    user = get_or_create_user(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        referrer_id=referrer_id
    )
    
    # Check if this is the first time this user is being created with this referrer
    if referrer_id and not user['referrer_id']:
        referrer_user = get_user_by_telegram_id(referrer_id)
        if referrer_user:
            add_referral_bonus(referrer_user['id'], 50) 
            # Now, set the referrer for the new user
            update_user(user['id'], referrer_id=referrer_user['id'])
            
    if await is_user_subscribed(message.from_user.id):
        await send_main_menu(message.chat.id, "Добро пожаловать!", state)
    else:
        await message.answer(
            "Для доступа к боту, пожалуйста, подпишитесь на наш канал. После этого нажмите кнопку ниже.",
            reply_markup=subscription_check_kb()
        )
        await SubscriptionCheck.WaitingForCheck.set()

@dp.callback_query_handler(lambda c: c.data == 'sub_check', state=SubscriptionCheck.WaitingForCheck)
async def handle_subscription_check(call: types.CallbackQuery, state: FSMContext):
    if await is_user_subscribed(call.from_user.id):
        await call.message.edit_text("Вы успешно подписались! Добро пожаловать!")
        await send_main_menu(call.message.chat.id, state=state) # Send ReplyKeyboardMarkup
    else:
        await call.answer("Вы еще не подписались на канал.", show_alert=True)

# --- Purchase Flow ---
@dp.message_handler(text="🛍️ Витрина", state="*")
@subscription_required
async def show_store(message: types.Message, state: FSMContext):
    await message.answer("Наши продукты:", reply_markup=store_products_kb())

@dp.callback_query_handler(lambda c: c.data.startswith("store_prod:"), state="*")
async def show_plans(call: types.CallbackQuery):
    product_name = call.data.split(":")[1]
    product_data = get_product(product_name)
    if not product_data:
        await call.answer("Такой продукт не найден.", show_alert=True)
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.")
        return

    text = (
        f"**{markdown_escape(product_data['name'])}**\n\n"
        f"Описание: {markdown_escape(product_data['description'])}\n\n"
        f"Выберите тариф:\n"
    )
    # Delete the previous message to prevent "message is not modified" errors or sending too many photos
    try:
        await call.message.delete()
    except Exception:
        pass
    
    if product_data['photo_file_id']:
        await bot.send_photo(
            call.message.chat.id,
            product_data['photo_file_id'],
            caption=text,
            parse_mode="MarkdownV2",
            reply_markup=plans_kb(product_name)
        )
    else:
        await bot.send_message(call.message.chat.id, text, parse_mode="MarkdownV2", reply_markup=plans_kb(product_name))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("select_plan:"), state="*")
async def start_purchase(call: types.CallbackQuery, state: FSMContext):
    plan_id = int(call.data.split(":")[1])
    
    plan_data = get_plan(plan_id)
    if not plan_data:
        await call.answer("Такой тарифный план не найден.", show_alert=True)
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.")
        return

    product = get_product(plan_data['product']) # Get product directly using product name from plan
    if not product:
        await call.answer("Ошибка: не удалось найти информацию о товаре.", show_alert=True)
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.")
        return

    plan = plan_data # Теперь это словарь

    # Получаем или создаем пользователя, чтобы получить внутренний ID
    user_data = get_or_create_user(call.from_user.id, call.from_user.username)
    user_id = user_data['id']

    promo_code = get_unused_promocode(product['name'])

    payment_status = "pending" # Default status
    if plan['price'] == 0: # If price is 0, no payment is needed
        payment_status = "completed"

    payment = add_payment(user_id, product['name'], plan['name'], plan['price'], payment_status, plan['id'], promo_code=promo_code)
    
    if payment_status == "completed":
        # If no payment needed, activate subscription directly
        sub = activate_subscription(user_id, plan['id'])
        await call.answer("Подписка успешно активирована!", show_alert=True)
        await send_main_menu(call.message.chat.id, "Подписка на " + markdown_escape(product['name']) + " (" + markdown_escape(plan['name']) + ") успешно активирована!")
    else:
        await state.update_data(payment_id=payment['id'], product_name=product['name'], plan_name=plan['name'], email_required=True)
        await Purchase.EnterEmail.set()
        
        text_to_send = (
            f"Для оформления подписки на **{markdown_escape(product['name'])} \\({markdown_escape(plan['name'])}\\)** "
            f"стоимостью **{plan['price']}₽**\n\n"
            f"Пожалуйста, введите ваш Email для связи\\. На него будет отправлена информация по подписке\\."
        )
        reply_markup_to_send = upload_check_kb()

        try:
            # Try to edit existing message first
            if call.message.caption:
                await call.message.edit_caption(
                    caption=text_to_send,
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup_to_send
                )
            elif call.message.text:
                await call.message.edit_text(
                    text=text_to_send,
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup_to_send
                )
            else:
                # If message has no text/caption (e.g., just a photo), send a new message
                await bot.send_message(
                    chat_id=call.message.chat.id,
                    text=text_to_send,
                    parse_mode="MarkdownV2",
                    reply_markup=reply_markup_to_send
                )
        except Exception as e:
            logging.error(f"Error editing message in start_purchase: {e}")
            await bot.send_message(
                chat_id=call.message.chat.id,
                text=text_to_send,
                parse_mode="MarkdownV2",
                reply_markup=reply_markup_to_send
            )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_purchase", state="*")
async def cancel_purchase_flow(call: types.CallbackQuery, state: FSMContext):
    await call.answer("Оплата отменена.", show_alert=True)
    await send_main_menu(call.message.chat.id, "Оплата отменена.")

@dp.message_handler(state=Purchase.EnterEmail)
async def process_email(message: types.Message, state: FSMContext):
    user_email = message.text.strip()
    
    # Логируем попытку ввода email
    log_user_action(message.from_user.id, "email_input", f"Email: {user_email}")
    
    if not validate_email(user_email):
        await message.reply("Пожалуйста, введите корректный Email.")
        log_user_action(message.from_user.id, "email_validation_failed", f"Invalid email: {user_email}")
        return
    
    async with state.proxy() as data:
        data["user_email"] = user_email # Store email in state
        payment_id = data.get("payment_id")
        product_name = data.get("product_name")
        plan_name = data.get("plan_name")

    if payment_id:
        # Update the payment with the email
        updated_payment = update_payment(payment_id, email=user_email)
        if updated_payment:
            # Update user email if it's new (only if the user doesn't have an email yet)
            user_data = get_user_by_telegram_id(message.from_user.id)
            if user_data and not user_data['email']:
                update_user(user_data['id'], email=user_email)

            # Get requisites and send them to the user
            settings = get_settings()
            current_reqs = settings.get("requisites", "Реквизиты еще не установлены.")

            await message.answer(
                f"Для оформления подписки на <b>{html_escape(str(product_name))}</b> ({html_escape(str(plan_name))})\n\n"
                f"<b>Реквизиты для оплаты:</b>\n<code>{html_escape(str(current_reqs))}</code>\n\n"
                f"Пожалуйста, произведите оплату и нажмите кнопку 'Отправить чек'.",
                parse_mode="HTML",
                reply_markup=pay_and_upload_kb()
            )
            await Purchase.UploadCheck.set() # Directly transition to UploadCheck
            log_user_action(message.from_user.id, "email_accepted", f"Payment: {payment_id}")
        else:
            await message.answer("Ошибка: платеж не найден. Пожалуйста, попробуйте снова.")
            await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.")
    else:
        await message.answer("Ошибка: Не удалось найти платеж. Пожалуйста, попробуйте снова.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.")

@dp.callback_query_handler(lambda c: c.data == "pay_and_upload", state="*")
async def handle_pay_and_upload(call: types.CallbackQuery, state: FSMContext):
    # This handler can be simplified. If the state is already Purchase.UploadCheck, this callback is redundant.
    # If it's not, it means the user clicked this button from a different state, so we should transition.
    
    # Check if we are already in the correct state, if not, transition.
    current_state = await state.get_state()
    if current_state != Purchase.UploadCheck.state:
        # This part might need more context from state if we're coming from unexpected places
        await Purchase.UploadCheck.set() 
    
    await call.message.edit_text(
        "Пожалуйста, отправьте фото чека об оплате. "
        "После проверки администратором вы получите доступ к материалам.",
        reply_markup=upload_check_kb()
    )
    await call.answer()

@dp.message_handler(state=Purchase.UploadCheck, content_types=types.ContentType.PHOTO)
async def process_check(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        payment_id = data.get("payment_id")
        product_name = data.get("product_name")
        plan_name = data.get("plan_name")
        email = data.get("user_email") # Retrieve email from state

    if not payment_id: 
        await message.answer("Ошибка: Не удалось найти платеж. Пожалуйста, попробуйте снова.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.")
        return

    file_id = message.photo[-1].file_id
    payment = attach_check(payment_id, file_id)

    if payment:
        await message.answer("Ваш чек отправлен на проверку админу. Ожидайте подтверждения.")
        await send_main_menu(message.chat.id)

        if config.ADMIN_CHAT_ID:
            # Escape special HTML characters in user's full_name and username
            escaped_full_name = html.escape(message.from_user.full_name)
            escaped_username = html.escape(message.from_user.username if message.from_user.username else 'N/A')
            
            # Escape other potentially problematic data
            escaped_product_name = html.escape(product_name)
            escaped_plan_name = html.escape(plan_name)
            escaped_email = html.escape(email if email else 'Не указан')
            escaped_payment_id = html.escape(str(payment['id']))

            admin_text = (
                f"🔔 Новый чек на проверку от {escaped_full_name} @{escaped_username}\n\n"
                f"Товар: {escaped_product_name}\n"
                f"Тариф: {escaped_plan_name}\n"
                f"Email: {escaped_email}\n\n"
                f"ID платежа: {escaped_payment_id}"
            )
            try:
                await bot.send_photo(
                    chat_id=config.ADMIN_CHAT_ID,
                    photo=file_id,
                    caption=admin_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(row_width=2).add(
                        InlineKeyboardButton("✅ Подтвердить", callback_data=cb_admin_confirm.new(action="approve", payment_id=payment_id)),
                        InlineKeyboardButton("❌ Отклонить", callback_data=cb_admin_confirm.new(action="reject", payment_id=payment_id))
                    )
                )
            except Exception as e:
                logging.error(f"Failed to send check notification to admin chat {config.ADMIN_CHAT_ID}: {e}")
    else:
        await message.answer("Произошла ошибка при прикреплении чека. Пожалуйста, попробуйте снова.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.")

# --- My Subscriptions ---
@dp.message_handler(text="📂 Мои подписки", state="*")
async def show_my_subs(message: types.Message, state: FSMContext):
    await state.finish() # Clear any pending states
    
    # Получаем внутренний ID пользователя из telegram_id
    user_data = get_user_by_telegram_id(message.from_user.id)
    if not user_data:
        await message.reply("Ошибка: не удалось найти ваш профиль. Пожалуйста, свяжитесь с поддержкой.")
        return
    
    user_id = user_data['id']
    user_payments = get_user_payments(user_id)
    user_subscriptions = get_user_subscriptions(user_id)
    
    if user_payments or user_subscriptions:
        text = "<b>История ваших платежей и подписок:</b>\n\n"
        
        # Показываем информацию о подписках и платежах
        await bot.send_message(
            message.chat.id,
            "Загружаю информацию о ваших подписках...",
            reply_markup=my_subs_menu()
        )
        
        # Вызываем callback напрямую
        await bot.send_message(
            message.chat.id,
            "Нажмите кнопку 'История заказов' для просмотра подробной информации."
        )
    else:
        await bot.send_message(
            message.chat.id,
            "У вас пока нет истории платежей и активных подписок.",
            reply_markup=my_subs_menu()
        )

# Обработчики удалены, так как кнопки больше не используются

@dp.callback_query_handler(lambda c: c.data == 'subs_history', state="*")
async def handle_subs_history(call: types.CallbackQuery):
    # Получаем внутренний ID пользователя из telegram_id
    user_data = get_user_by_telegram_id(call.from_user.id)
    if not user_data:
        await call.message.edit_text("Ошибка: не удалось найти ваш профиль. Пожалуйста, свяжитесь с поддержкой.")
        await call.answer()
        return
    
    user_id = user_data['id']
    user_payments = get_user_payments(user_id)
    user_subscriptions = get_user_subscriptions(user_id)
    
    if user_payments or user_subscriptions:
        text = "<b>История ваших платежей и подписок:</b>\n\n"
        messages = []
        
        # Создаем список для отслеживания уже показанных продуктов и тарифов
        shown_products = set()
        
        # Сначала показываем активные подписки
        if user_subscriptions:
            active_subs = [sub for sub in user_subscriptions if sub['active']]
            if active_subs:
                text += "<b>🟢 АКТИВНЫЕ ПОДПИСКИ:</b>\n\n"
                
                for sub in active_subs:
                    # Получаем информацию о продукте
                    product_data = get_product(sub['product'])
                    product_photo = None
                    
                    if product_data:
                        product_photo = product_data.get('photo_file_id')
                    
                    # Находим соответствующий платеж для получения промокода
                    related_payment = None
                    for payment in user_payments:
                        if payment['product'] == sub['product'] and payment['tariff'] == sub['tariff'] and payment['status'] == 'completed':
                            related_payment = payment
                            break
                    
                    # Формируем промокод
                    promo_code = "Не назначен"
                    if related_payment and related_payment.get('promo_code'):
                        promo_code = related_payment['promo_code']
                    
                    # Формируем информацию о сроке действия
                    end_date = sub['end_date'].strftime("%d.%m.%Y") if sub['end_date'] else "Бессрочно"
                    
                    # Создаем блок в нужном формате для активной подписки
                    sub_block = (
                        f"📦 <b>Товар:</b> {html.escape(str(sub['product']))}\n"
                        f"💡 <b>Тариф:</b> {html.escape(str(sub['tariff']))}\n"
                        f"📅 <b>Действует до:</b> {end_date}\n"
                        f"🔑 <b>Промокод:</b> <code>{html.escape(str(promo_code))}</code>\n"
                        f"<b>Статус:</b> Активна\n\n"
                    )
                    
                    # Добавляем этот продукт+тариф в список показанных
                    product_tariff_key = f"{sub['product']}:{sub['tariff']}"
                    shown_products.add(product_tariff_key)
                    
                    # Если у продукта есть фото, добавляем его в список для отправки
                    if product_photo:
                        messages.append({
                            "type": "photo",
                            "file_id": product_photo,
                            "caption": sub_block,
                            "product": sub['product']
                        })
                    else:
                        text += sub_block
        
        # Затем показываем историю платежей
        text += "<b>📊 ИСТОРИЯ ПЛАТЕЖЕЙ:</b>\n\n"
        for payment in user_payments:
            if payment['status'] == "completed":
                # Получаем информацию о продукте
                product_data = get_product(payment['product'])
                product_photo = None
                
                if product_data:
                    product_photo = product_data.get('photo_file_id')
                
                # Формируем промокод
                promo_code = payment.get('promo_code', 'Не назначен')
                
                # Форматируем дату платежа
                payment_date = payment['created_at'].strftime("%d.%m.%Y")
                
                # Проверяем, не показывали ли мы уже этот продукт+тариф
                product_tariff_key = f"{payment['product']}:{payment['tariff']}"
                if product_tariff_key in shown_products:
                    continue
                
                # Добавляем этот продукт+тариф в список показанных
                shown_products.add(product_tariff_key)
                
                # Создаем блок в нужном формате для завершенного платежа
                payment_block = (
                    f"📦 <b>Товар:</b> {html.escape(str(payment['product']))}\n"
                    f"💡 <b>Тариф:</b> {html.escape(str(payment['tariff']))}\n"
                    f"💰 <b>Оплачено:</b> {payment['price']}₽\n"
                    f"📅 <b>Дата оплаты:</b> {payment_date}\n"
                    f"🔑 <b>Промокод:</b> <code>{html.escape(str(promo_code))}</code>\n"
                    f"<b>Статус:</b> Истекла\n\n"
                )
                
                # Если у продукта есть фото, добавляем его в список для отправки
                if product_photo:
                    messages.append({
                        "type": "photo",
                        "file_id": product_photo,
                        "caption": payment_block,
                        "product": payment['product']
                    })
                else:
                    text += payment_block
            elif payment['status'] == "pending":
                # Платежи со статусом "pending" не показываем в истории
                continue
            elif payment['status'] == "rejected":
                # Для отклоненных платежей просто добавляем информацию в текст
                payment_date = payment['created_at'].strftime("%d.%m.%Y %H:%M")
                text += (
                    f"• <b>Товар:</b> {html.escape(str(payment['product']))} ({html.escape(str(payment['tariff']))})\n"
                    f"  <b>Цена:</b> {payment['price']}₽\n"
                    f"  <b>Статус:</b> ❌ Отклонен\n"
                    f"  <b>Дата:</b> {payment_date}\n\n"
                )
        
        # Если есть фотографии для отправки
        if messages:
            try:
                await call.message.delete()  # Удаляем предыдущее сообщение
            except Exception:
                pass
            
            # Отправляем первое сообщение с фото
            first_message = messages[0]
            await bot.send_photo(
                call.message.chat.id,
                first_message["file_id"],
                caption=first_message["caption"],
                parse_mode="HTML"
            )
            
            # Отправляем остальные сообщения с фото, если они есть
            for msg in messages[1:]:
                await bot.send_photo(
                    call.message.chat.id,
                    msg["file_id"],
                    caption=msg["caption"],
                    parse_mode="HTML"
                )
            
            # Отправляем оставшийся текст, если он есть
            if text and text != "<b>История ваших платежей и подписок:</b>\n\n<b>🟢 АКТИВНЫЕ ПОДПИСКИ:</b>\n\n<b>📊 ИСТОРИЯ ПЛАТЕЖЕЙ:</b>\n\n":
                await bot.send_message(
                    call.message.chat.id,
                    text,
                    parse_mode="HTML",
                    reply_markup=my_subs_menu()
                )
            else:
                # Если текста нет, отправляем только кнопки
                await bot.send_message(
                    call.message.chat.id,
                    "Используйте кнопки ниже для навигации:",
                    reply_markup=my_subs_menu()
                )
        else:
            # Если нет фото, отправляем только текст
            try:
                await call.message.edit_text(text, parse_mode="HTML", reply_markup=my_subs_menu())
            except Exception as e:
                logging.error(f"Error editing message: {e}")
                try:
                    await call.message.delete()
                except Exception:
                    pass
                await bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=my_subs_menu())
    else:
        text = "У вас пока нет истории платежей и активных подписок."
        try:
            await call.message.edit_text(text, reply_markup=my_subs_menu())
        except Exception:
            await bot.send_message(call.message.chat.id, text, reply_markup=my_subs_menu())
    
    await call.answer()

# --- Back handlers ---
@dp.callback_query_handler(lambda c: c.data == "back_to_main_menu", state="*")
async def back_to_main_menu_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await send_main_menu(call.message.chat.id, "Возвращаю вас в главное меню.", state)

@dp.callback_query_handler(lambda c: c.data == "back_to_store_products", state="*")
async def back_to_store_products_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    # Delete previous message to prevent "message not modified" errors
    try:
        await call.message.delete()
    except Exception:
        pass
    await show_store(call.message, state) # Re-use show_store handler

# --- Admin ---
@dp.callback_query_handler(cb_admin_confirm.filter(), state="*")
async def handle_payment_confirmation(call: types.CallbackQuery, callback_data: dict):
    action = callback_data["action"]
    item_id = int(callback_data["payment_id"]) # This will be either payment_id or withdrawal_request_id

    logging.info(f"Admin action: {action} for ID: {item_id}")

    if not is_admin(call.from_user.id):
        await call.answer("У вас нет прав для этого действия.", show_alert=True)
        return

    if action == "approve":
        set_payment_status(item_id, "completed")
        payment = get_payment_by_id(item_id)
        if payment:
            user = get_user_by_id(payment['user_id'])
            
            # Активируем подписку для пользователя
            if payment['plan_id']:
                sub = activate_subscription(payment['user_id'], payment['plan_id'])
                logging.info(f"Activated subscription for user {payment['user_id']}, plan {payment['plan_id']}: {sub}")
            
            if user:
                try:
                    # Используем HTML-форматирование вместо MarkdownV2
                    product_name = html.escape(payment['product'])
                    promo_code = html.escape(payment['promo_code']) if payment['promo_code'] else "Не назначен"
                    
                    message_text = (
                        f"✅ Ваш платеж ID {item_id} на сумму {payment['price'] / 100:.2f}₽ подтвержден!\n\n"
                        f"Товар: <b>{product_name}</b>\n"
                        f"Тариф: <b>{html.escape(payment['tariff'])}</b>\n"
                        f"<b>Ваш промокод:</b> <code>{promo_code}</code>\n\n"
                        f"Инструкция по активации:\n"
                        f"1. Перейдите на сайт {product_name}\n"
                        f"2. Введите промокод в личном кабинете\n"
                        f"3. Наслаждайтесь всеми возможностями подписки!\n\n"
                        f"Если возникнут вопросы — напишите в поддержку."
                    )
                    
                    # Отправляем сообщение с HTML-форматированием
                    await bot.send_message(
                        user['telegram_id'],
                        message_text,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(row_width=1).add(
                            InlineKeyboardButton("🎁 Получить материалы", callback_data=f"user_mat:list:{payment['product']}"),
                            InlineKeyboardButton("💬 Поддержка", url=config.SUPPORT_LINK),
                            InlineKeyboardButton("🏠 Главное меню", callback_data="back_to_main_menu")
                        )
                    )
                except Exception as e:
                    logging.error(f"Failed to send payment confirmation to user {user['telegram_id']}: {e}")
            
            # Update the admin's message to reflect the action
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"✅ Платеж {item_id} подтвержден.\nID пользователя: {payment['user_id']}.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
        else:
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"Ошибка: Платеж {item_id} не найден после подтверждения.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
    elif action == "reject":
        set_payment_status(item_id, "rejected")
        payment = get_payment_by_id(item_id)
        if payment:
            user = get_user_by_id(payment['user_id'])
            if user:
                try:
                    # Используем HTML вместо MarkdownV2 для избежания проблем с экранированием
                    await bot.send_message(
                        user['telegram_id'], 
                        f"❌ Ваш платеж ID {item_id} на сумму {payment['price'] / 100:.2f}₽ отклонен. Пожалуйста, свяжитесь с поддержкой для уточнения.", 
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logging.error(f"Failed to send payment rejection to user {user['telegram_id']}: {e}")
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"❌ Платеж {item_id} отклонен.\nID пользователя: {payment['user_id']}.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
        else:
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"Ошибка: Платеж {item_id} не найден после отклонения.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)

    elif action == "approve_withdrawal":
        withdrawal_request = get_withdrawal_request_by_id(item_id)
        if withdrawal_request and withdrawal_request['status'] == "pending":
            user = get_user_by_id(withdrawal_request['user_id'])
            if user:
                try:
                    # Используем HTML вместо MarkdownV2 для избежания проблем с экранированием
                    await bot.send_message(
                        user['telegram_id'], 
                        f"✅ Ваша заявка на вывод средств ID {item_id} на сумму {withdrawal_request['amount'] / 100:.2f}₽ одобрена! Средства будут отправлены в ближайшее время.", 
                        parse_mode="HTML"
                    )
                    # Только после успешной отправки уведомления меняем статус
                    set_withdrawal_request_status(item_id, "approved")
                except Exception as e:
                    logging.error(f"Failed to send withdrawal approval to user {user['telegram_id']}: {e}")
                    # Если не удалось отправить уведомление, всё равно меняем статус
                    set_withdrawal_request_status(item_id, "approved")
            else:
                # Если пользователя не нашли, всё равно меняем статус
                set_withdrawal_request_status(item_id, "approved")
                
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"✅ Заявка на вывод средств {item_id} одобрена.\nID пользователя: {withdrawal_request['user_id']}.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
        else:
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"Ошибка: Заявка на вывод средств {item_id} не найдена или уже обработана.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)

    elif action == "reject_withdrawal":
        withdrawal_request = get_withdrawal_request_by_id(item_id)
        if withdrawal_request and withdrawal_request['status'] == "pending":
            user = get_user_by_id(withdrawal_request['user_id'])
            if user:
                try:
                    # Сначала отправляем уведомление с использованием HTML
                    await bot.send_message(
                        user['telegram_id'], 
                        f"❌ Ваша заявка на вывод средств ID {item_id} на сумму {withdrawal_request['amount'] / 100:.2f}₽ отклонена. Средства возвращены на ваш баланс.", 
                        parse_mode="HTML"
                    )
                    # Затем возвращаем средства на баланс
                    update_user(user['id'], referral_balance=user['referral_balance'] + withdrawal_request['amount'])
                    # И только потом меняем статус заявки
                    set_withdrawal_request_status(item_id, "rejected")
                except Exception as e:
                    logging.error(f"Failed to send withdrawal rejection to user {user['telegram_id']}: {e}")
                    # Даже если не удалось отправить уведомление, возвращаем средства и меняем статус
                    update_user(user['id'], referral_balance=user['referral_balance'] + withdrawal_request['amount'])
                    set_withdrawal_request_status(item_id, "rejected")
            else:
                # Если пользователя не нашли, просто меняем статус
                set_withdrawal_request_status(item_id, "rejected")
                
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"❌ Заявка на вывод средств {item_id} отклонена.\nID пользователя: {withdrawal_request['user_id']}. Средства возвращены на баланс.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
        else:
            current_caption = call.message.caption if call.message.caption else call.message.text
            new_caption = f"Ошибка: Заявка на вывод средств {item_id} не найдена или уже обработана.\n\n{current_caption}"
            if call.message.photo:
                await call.message.edit_caption(caption=new_caption, reply_markup=None)
            else:
                await call.message.edit_text(text=new_caption, reply_markup=None)
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_promo:add:"), state="*")
async def admin_add_promo_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_for_promos=product_name)
    await Admin.EnterPromoCodes.set()
    # Delete previous message and send new one to avoid BadRequest errors with photos
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        chat_id=call.message.chat.id,
        text=f"Отправьте список промокодов для товара **{markdown_escape(product_name)}**, каждый с новой строки:",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Отмена", callback_data=f"admin_prod:view:{product_name}"))
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_promo:list:"), state="*")
@dp.callback_query_handler(lambda c: c.data.startswith("admin_promo:filter:"), state="*")
async def admin_list_promos(call: types.CallbackQuery, state: FSMContext):
    # Extract product name and filter from callback data
    parts = call.data.split(":")
    action = parts[1]
    product_name = parts[2]
    status_filter = parts[3] if action == "filter" else "all"

    # Получаем промокоды для данного продукта с учетом фильтра
    promo_codes = get_promo_codes_by_product(product_name, status_filter)
    
    # Логируем полученные промокоды для отладки
    logging.info(f"Получено {len(promo_codes)} промокодов для {product_name} с фильтром {status_filter}")
    for promo in promo_codes:
        logging.info(f"Промокод: {promo['code']}, статус: {promo['status']}")

    message_lines = []
    if not promo_codes:
        message_lines.append(f"Нет {('невыданных' if status_filter == 'unused' else ('выданных' if status_filter == 'used' else 'промокодов'))} для товара <b>{html.escape(product_name)}</b>.")
    else:
        message_lines.append(f"<b>Промокоды для {html.escape(product_name)} ({status_filter}):</b>\n")
        for promo in promo_codes:
            status_text = "Выдан" if promo["status"] == "issued" else "Не выдан"
            message_lines.append(f"<code>{html.escape(promo['code'])}</code> - Статус: {status_text}")
            if promo["status"] == "issued" and promo.get("payment_id"):
                payment = get_payment_by_id(promo["payment_id"])
                if payment:
                    user = get_user_by_id(payment["user_id"])
                    user_info = f"@{html.escape(user['username'])}" if user and user['username'] else f"ID: {payment['user_id']}"
                    message_lines.append(f"  Кому выдан: {user_info} (Email: {html.escape(payment['email'] or 'N/A')}) ")
                    message_lines.append(f"  Дата выдачи: {payment['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            message_lines.append("") # Add an empty line for spacing

    # Use send_message instead of edit_text to avoid BadRequest when coming from photo messages
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        chat_id=call.message.chat.id,
        text="\n".join(message_lines),
        parse_mode="HTML",
        reply_markup=admin_promo_codes_list_kb(product_name, status_filter)
    )
    await call.answer()

@dp.message_handler(state=Admin.EnterPromoCodes)
async def admin_process_promo_codes(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_for_promos")

    if not product_name:
        await message.answer("Ошибка: Не удалось определить товар для добавления промокодов.")
        await send_admin_menu(message.chat.id, "Возвращаю вас в админ-панель.", state)
        return

    new_codes = [code.strip() for code in message.text.split('\n') if code.strip()]

    if not new_codes:
        await message.answer("Вы не отправили ни одного промокода. Пожалуйста, попробуйте еще раз.")
        return

    added_count, duplicates = add_promo_codes_bulk(product_name, new_codes)

    response_text = f"Добавлено {added_count} новых промокодов.\n"
    if duplicates:
        response_text += "Следующие промокоды уже существуют и не были добавлены:\n"
        for dup in duplicates:
            response_text += f"- `{dup}`\n"
    
    await message.answer(markdown_escape(response_text), parse_mode="MarkdownV2")
    await state.finish()
    # Simulate a callback_query for admin_list_promos
    simulated_call = types.CallbackQuery(id='fake', from_user=message.from_user, chat_instance='fake', data=f"admin_promo:list:{product_name}")
    await admin_list_promos(simulated_call, state)

@dp.message_handler(text="👥 Реферальная программа", state="*")
async def referral_program(message: types.Message, state: FSMContext):
    await state.finish()
    user = get_or_create_user(message.from_user.id)
    bot_info = await bot.get_me()
    referral_link = f"https://t.me/{(await bot.get_me()).username}?start=r_{user['telegram_id']}" # Use telegram_id for link
    
    referrals_count = get_referrals_count(user['id'])
    
    # Используем HTML-форматирование вместо MarkdownV2
    text = (
        f"<b>Ваша реферальная программа</b>\n\n"
        f"Приглашайте друзей и получайте бонусы!\n\n"
        f"🔗 <b>Ваша ссылка:</b>\n<code>{html.escape(referral_link)}</code>\n\n"
        f"👤 <b>Приглашено:</b> {referrals_count} чел.\n"
        f"💰 <b>Баланс:</b> {user['referral_balance'] / 100:.2f} ₽" # Format balance
    )
    
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=referral_menu_kb())

@dp.message_handler(text="🎁 Материалы", state="*")
@subscription_required
async def user_materials_start(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Выберите продукт для просмотра материалов:", reply_markup=materials_for_user_kb(message.from_user.id))

@dp.message_handler(text="💬 Поддержка", state="*")
async def show_support_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "Если у вас возникли вопросы или проблемы, вы можете связаться с нашей службой поддержки.",
        reply_markup=support_menu_kb()
    )

@dp.message_handler(text="⚙️ Настройки", state="*")
async def show_settings_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer(
        "Здесь будут находиться настройки пользователя.",
        reply_markup=settings_menu_kb()
    )

@dp.callback_query_handler(lambda c: c.data.startswith("referral:"), state="*")
async def handle_referral_callbacks(call: types.CallbackQuery, state: FSMContext):
    action = call.data.split(":")[1]
    text_to_send = ""
    
    if action == "link":
        user = get_or_create_user(call.from_user.id, username=call.from_user.username)
        referral_link = f"https://t.me/{(await bot.get_me()).username}?start=r_{user['telegram_id']}"
        # Используем HTML-форматирование вместо MarkdownV2
        text_to_send = f"Ваша реферальная ссылка: <code>{html.escape(str(referral_link))}</code>"
    elif action == "balance": # Added balance action
        user = get_or_create_user(call.from_user.id, username=call.from_user.username)
        text_to_send = f"Ваш текущий баланс: {user['referral_balance'] / 100:.2f}₽"
    elif action == "withdraw_start": # Renamed to be more descriptive
        user_id = call.from_user.id
        user = get_or_create_user(user_id, username=call.from_user.username)
        if user['referral_balance'] >= config.MIN_WITHDRAWAL_AMOUNT:
            await call.message.edit_text(
                f"Ваш текущий баланс: {user['referral_balance'] / 100:.2f}₽\n\nВведите сумму для вывода (минимум {config.MIN_WITHDRAWAL_AMOUNT / 100:.2f}₽):",
                parse_mode="HTML"
            )
            await Withdrawal.EnterAmount.set()
            await call.answer()
            return # Prevent further processing in this handler
        else:
            text_to_send = f"Минимальная сумма для вывода {config.MIN_WITHDRAWAL_AMOUNT / 100:.2f}₽. Ваш баланс: {user['referral_balance'] / 100:.2f}₽."
    elif action == "history":
        user_id = call.from_user.id
        user = get_or_create_user(user_id, username=call.from_user.username)
        withdrawal_requests = get_user_withdrawal_requests(user['id'])

        if not withdrawal_requests:
            text_to_send = "У вас пока нет истории выплат."
        else:
            text_to_send = "<b>История выплат:</b>\n\n"
            for req in withdrawal_requests:
                status_map = {
                    "pending": "⏳ В ожидании",
                    "approved": "✅ Одобрено",
                    "rejected": "❌ Отклонено"
                }
                status = status_map.get(req['status'], req['status'])
                request_date = req['request_date'].strftime("%d.%m.%Y %H:%M")
                admin_decision_date = req['admin_decision_date'].strftime("%d.%m.%Y %H:%M") if req['admin_decision_date'] else "Нет"
                
                text_to_send += (
                    f"Сумма: <b>{req['amount'] / 100:.2f}₽</b>\n"
                    f"Статус: {status}\n"
                    f"Дата запроса: {request_date}\n"
                    f"Дата решения: {admin_decision_date}\n"
                    f"---\n"
                )

    try:
        if call.message.caption:
            await call.message.edit_caption(
                caption=text_to_send,
                parse_mode="HTML",
                reply_markup=referral_menu_kb()
            )
        elif call.message.text:
            await call.message.edit_text(
                text=text_to_send,
                parse_mode="HTML",
                reply_markup=referral_menu_kb()
            )
        else:
            # Fallback if message type is unexpected
            await bot.send_message(
                chat_id=call.message.chat.id,
                text=text_to_send,
                parse_mode="HTML",
                reply_markup=referral_menu_kb()
            )
    except Exception as e:
        logging.error(f"Error handling referral callback: {e}")
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.", state)
    await call.answer()

@dp.message_handler(state=Withdrawal.EnterAmount)
async def process_withdrawal_amount(message: types.Message, state: FSMContext):
    try:
        amount_str = message.text.replace(',', '.')
        amount = int(float(amount_str) * 100) # Convert to cents
        if amount <= 0:
            raise ValueError("Сумма должна быть положительным числом.")

        user = get_or_create_user(message.from_user.id)
        if user['referral_balance'] < amount:
            await message.reply(f"Недостаточно средств на балансе. Ваш баланс: {user['referral_balance'] / 100:.2f}₽.")
            return

        if amount < config.MIN_WITHDRAWAL_AMOUNT:
            await message.reply(f"Минимальная сумма для вывода: {config.MIN_WITHDRAWAL_AMOUNT / 100:.2f}₽.")
            return

        await state.update_data(withdrawal_amount=amount)
        # Запрашиваем номер телефона для СБП
        await Withdrawal.EnterPhone.set()
        await message.answer("Введите номер телефона для перевода по СБП (в формате +79XXXXXXXXX):")
    except ValueError as e:
        await message.reply(f"Ошибка ввода: {str(e)}. Пожалуйста, введите сумму числом.")
    except Exception as e:
        logging.error(f"Error processing withdrawal amount: {e}")
        await message.answer("Произошла ошибка при обработке суммы. Пожалуйста, попробуйте позже.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.", state)

@dp.message_handler(state=Withdrawal.EnterPhone)
async def process_withdrawal_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    # Простая проверка формата телефона
    if not (phone.startswith('+7') or phone.startswith('8')) or not re.match(r'^\+?[78]\d{10}$', phone):
        await message.reply("Пожалуйста, введите корректный номер телефона в формате +79XXXXXXXXX или 89XXXXXXXXX")
        return
    
    await state.update_data(withdrawal_phone=phone)
    await Withdrawal.EnterBank.set()
    await message.answer("Введите название вашего банка:")

@dp.message_handler(state=Withdrawal.EnterBank)
async def process_withdrawal_bank(message: types.Message, state: FSMContext):
    bank = message.text.strip()
    if not bank or len(bank) < 3:
        await message.reply("Пожалуйста, введите корректное название банка")
        return
    
    await state.update_data(withdrawal_bank=bank)
    
    # Получаем все данные из состояния
    data = await state.get_data()
    amount = data.get("withdrawal_amount")
    phone = data.get("withdrawal_phone")
    bank = data.get("withdrawal_bank")
    
    await Withdrawal.ConfirmAmount.set()
    await message.answer(
        f"Вы хотите вывести {amount / 100:.2f}₽\n"
        f"Номер телефона: {phone}\n"
        f"Банк: {bank}\n\n"
        f"Подтвердите операцию.",
        reply_markup=withdraw_confirm_kb()
    )

@dp.callback_query_handler(lambda c: c.data.startswith("withdraw:"), state="*")
async def handle_withdrawal_confirm(call: types.CallbackQuery, state: FSMContext):
    action = call.data.split(":")[1]
    data = await state.get_data()
    amount = data.get("withdrawal_amount")
    phone = data.get("withdrawal_phone")
    bank = data.get("withdrawal_bank")
    user = get_or_create_user(call.from_user.id)

    # Проверяем, что у нас есть все необходимые данные
    if not amount or not phone or not bank:
        await call.answer("Ошибка: недостаточно данных для вывода средств", show_alert=True)
        await call.message.edit_text("Произошла ошибка. Пожалуйста, начните процесс вывода заново.", reply_markup=referral_menu_kb())
        await state.finish()
        return

    if action == "confirm" and amount:
        if user['referral_balance'] < amount:
            await call.message.edit_text("Недостаточно средств на балансе.", reply_markup=referral_menu_kb())
            await call.answer()
            await send_main_menu(call.message.chat.id)
            return

        # Сохраняем данные о телефоне и банке в запросе на вывод
        withdrawal_request = add_withdrawal_request(user['id'], amount, phone=phone, bank=bank)
        update_user(user['id'], referral_balance=user['referral_balance'] - amount) # Deduct from balance
        
        await call.message.edit_text(
            f"Заявка на вывод {amount / 100:.2f}₽ создана и отправлена администратору. Ожидайте подтверждения.",
            reply_markup=referral_menu_kb()
        )
        await send_main_menu(call.message.chat.id)

        # Notify admin
        if config.ADMIN_CHAT_ID:
            escaped_username = html.escape(user['username'] if user['username'] else 'N/A')
            admin_text = (
                f"🔔 Новая заявка на вывод средств от @{escaped_username}\n\n"
                f"Сумма: {amount / 100:.2f}₽\n"
                f"Телефон СБП: {html.escape(phone)}\n"
                f"Банк: {html.escape(bank)}\n"
                f"ID пользователя: {user['id']}\n"
                f"ID запроса: {withdrawal_request['id']}"
            )
            try:
                await bot.send_message(
                    chat_id=config.ADMIN_CHAT_ID,
                    text=admin_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(row_width=2).add(
                        InlineKeyboardButton("✅ Одобрить", callback_data=cb_admin_confirm.new(action="approve_withdrawal", payment_id=withdrawal_request['id'])), # Re-using payment_id field for request_id
                        InlineKeyboardButton("❌ Отклонить", callback_data=cb_admin_confirm.new(action="reject_withdrawal", payment_id=withdrawal_request['id']))
                    )
                )
            except Exception as e:
                logging.error(f"Failed to send withdrawal notification to admin chat {config.ADMIN_CHAT_ID}: {e}")

    elif action == "cancel":
        await call.message.edit_text("Вывод средств отменен.", reply_markup=referral_menu_kb())
        await send_main_menu(call.message.chat.id)
    
    await state.finish()
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "user_mat:back_products", state="*")
async def user_back_to_mat_products(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    user_id = call.from_user.id
    text_to_send = "Материалы по вашим продуктам:"
    reply_markup_to_send = materials_for_user_kb(user_id)

    try:
        if call.message.caption:
            await call.message.edit_caption(
                caption=text_to_send,
                parse_mode="MarkdownV2",
                reply_markup=reply_markup_to_send
            )
        elif call.message.text:
            await call.message.edit_text(
                text=text_to_send,
                parse_mode="MarkdownV2",
                reply_markup=reply_markup_to_send
            )
        else:
            await bot.send_message(
                chat_id=call.message.chat.id,
                text=text_to_send,
                parse_mode="MarkdownV2",
                reply_markup=reply_markup_to_send
            )
    except Exception as e:
        logging.error(f"Error navigating back to user materials products: {e}")
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.", state)

@dp.callback_query_handler(lambda c: c.data.startswith("user_mat:list:"), state="*")
async def user_list_materials(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    # Delete previous message to prevent "message not modified" errors or sending too many photos
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        call.message.chat.id,
        f"Материалы для **{markdown_escape(product_name)}**:",
        reply_markup=materials_list_kb(product_name),
        parse_mode="MarkdownV2"
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("user_mat:get:"), state="*")
async def user_get_material(call: types.CallbackQuery, state: FSMContext):
    material_id = int(call.data.split(":")[2])
    material = get_material(material_id)
    if material:
        text = f"**{markdown_escape(material['title'])}**\n\n{markdown_escape(material['text'] or '')}"
        # Delete previous message
        try:
            await call.message.delete()
        except Exception:
            pass
        if material['file_id']:
            await bot.send_document(call.message.chat.id, material['file_id'], caption=text, parse_mode="MarkdownV2")
        else:
            await bot.send_message(call.message.chat.id, text, parse_mode="MarkdownV2")
        # After showing material, return to the material list for that product
        await user_list_materials(types.CallbackQuery(id='fake', from_user=call.from_user, chat_instance='fake', data=f"user_mat:list:{material['product_name']}"), state)
    else:
        await call.answer("Материал не найден.", show_alert=True)
        await send_main_menu(call.message.chat.id, "Произошла ошибка, возвращаю в главное меню.", state)
    await call.answer()

# --- Admin Handlers for Materials ---

def admin_materials_kb(product_name: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    materials = get_materials_for_product(product_name)
    for mat in materials:
        kb.add(InlineKeyboardButton(f"🗑️ {mat['title']}", callback_data=f"admin_mat:delete:{mat['id']}"))
    kb.add(InlineKeyboardButton("➕ Добавить материал", callback_data=f"admin_mat:add:{product_name}"))
    kb.add(InlineKeyboardButton("🔙 К товару", callback_data=f"admin_prod:view:{product_name}"))
    return kb

@dp.callback_query_handler(lambda c: c.data.startswith("admin_mat:list:"), state="*")
async def admin_list_materials(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(current_product_name=product_name)
    materials = get_materials_for_product(product_name)

    message_lines = []
    message_lines.append(f"<b>Материалы для {html.escape(str(product_name))}:</b>\n")
    if not materials:
        message_lines.append("Нет доступных материалов.")
    else:
        for mat in materials:
            title = html.escape(str(mat['title']))
            material_id = mat['id']
            message_lines.append(f"- <b>{title}</b> [ID: {material_id}]")
    
    # Use send_message instead of edit_text to avoid BadRequest errors with photos
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        chat_id=call.message.chat.id,
        text="\n".join(message_lines),
        parse_mode="HTML",
        reply_markup=admin_materials_kb(product_name)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_mat:delete:"), state="*")
async def admin_delete_material(call: types.CallbackQuery, state: FSMContext):
    material_id = int(call.data.split(":")[2])
    material = get_material(material_id)
    if not material: 
        await call.answer("Материал не найден.", show_alert=True)
        await send_admin_menu(call.message.chat.id, "Возвращаю в админ-панель.", state)
        return
    product_name = material['product_name']
    
    delete_material(material_id)
    
    await call.answer("Материал удален.", show_alert=True)
    # Re-display materials list for the product
    # Simulate a callback query to re-list materials for the product
    simulated_call = types.CallbackQuery(id='fake', from_user=call.from_user, chat_instance='fake', data=f"admin_mat:list:{product_name}")
    await admin_list_materials(simulated_call, state)
    
@dp.callback_query_handler(lambda c: c.data.startswith("admin_mat:add:"), state="*")
async def admin_add_material_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_for_material=product_name)
    await Admin.AddMaterial_Title.set()
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(call.message.chat.id, f"Введите заголовок для нового материала для товара **{markdown_escape(product_name)}**:", parse_mode="MarkdownV2")
    await call.answer()
    
@dp.message_handler(state=Admin.AddMaterial_Title)
async def admin_add_material_title(message: types.Message, state: FSMContext):
    await state.update_data(material_title=message.text)
    await state.set_state(Admin.AddMaterial_Content)
    await message.answer("Отправьте текст и/или файл для материала. Если чего-то нет, напишите 'нет'.")

@dp.message_handler(state=Admin.AddMaterial_Content, content_types=types.ContentType.ANY)
async def admin_add_material_content(message: types.Message, state: FSMContext):
    text = message.text if message.text and message.text.lower() != 'нет' else None
    
    file_id = None
    if message.photo: file_id = message.photo[-1].file_id
    elif message.document: file_id = message.document.file_id
    elif message.video: file_id = message.video.file_id
    
    if not text and not file_id:
        await message.reply("Вы должны предоставить либо текст, либо файл.")
        return

    data = await state.get_data()
    product_name = data.get("product_for_material")
    title = data.get("material_title")

    if not product_name or not title:
        await message.reply("Ошибка: не удалось получить данные о товаре или заголовке.")
        await state.finish()
        return

    material = add_material(product_name, title, text, file_id)
    
    await state.finish() # Finish FSM after material is added
    await message.answer(f"✅ Материал '{html.escape(title)}' добавлен.", parse_mode="HTML")
    
    # Simulate a callback query to re-list materials for the product
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📋 Список материалов", callback_data=f"admin_mat:list:{product_name}"))
    kb.add(InlineKeyboardButton("🔙 К товару", callback_data=f"admin_prod:view:{product_name}"))
    await message.answer("Выберите дальнейшее действие:", reply_markup=kb)

@dp.message_handler(commands=['admin'], state="*")
@dp.message_handler(text="Админ панель", state="*") # New text handler for admin menu button
async def admin_panel(message: types.Message, state: FSMContext):
    if is_admin(message.from_user.id):
        await send_admin_menu(message.chat.id, state=state)
    else:
        await message.answer("У вас нет доступа к админ-панели.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.", state)

@dp.message_handler(text="📦 Управление товарами", state="*")
async def admin_products_menu_handler(message: types.Message, state: FSMContext):
    logging.info(f"User {message.from_user.id} entered admin_products_menu.")
    # Проверка на админа
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.", state)
        return
    # No need to set ProductList state, just display products
    await message.answer("Выберите товар для управления или добавьте новый:", reply_markup=admin_products_kb())

@dp.callback_query_handler(lambda c: c.data == "admin_prod:back_list", state="*")
async def admin_products_back_list(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    # Simulate message from admin_products_menu_handler to show product list
    await admin_products_menu_handler(call.message, state)

@dp.callback_query_handler(lambda c: c.data == "admin_back_to_main", state="*")
async def admin_back_to_main_menu_callback(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await send_admin_menu(call.message.chat.id, "Возвращаю вас в админ-панель.", state)

@dp.message_handler(text="💳 Реквизиты", state="*")
async def requisites_menu(message: types.Message, state: FSMContext):
    # Проверка на админа
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.", state)
        return
    settings = get_settings()
    current_reqs = settings.get("requisites", "Реквизиты еще не установлены.")
    await state.set_state(Admin.Requisites)
    await message.answer(
        f"Текущие реквизиты:\n<code>{html.escape(str(current_reqs))}</code>",
        parse_mode="HTML",
        reply_markup=admin_requisites_kb()
    )

@dp.callback_query_handler(lambda c: c.data == "admin_req:edit", state=Admin.Requisites)
async def admin_edit_requisites_start(call: types.CallbackQuery, state: FSMContext):
    await Admin.EditRequisites.set()
    await call.message.edit_text("Отправьте новые реквизиты:")
    await call.answer()

@dp.message_handler(state=Admin.EditRequisites)
async def admin_edit_requisites_save(message: types.Message, state: FSMContext):
    new_requisites = message.text
    # Проверяем, что в реквизитах есть информация о банке
    if "банк" not in message.text.lower() and "bank" not in message.text.lower():
        await message.answer("Пожалуйста, укажите банк в реквизитах. Например: 'Сбербанк: 1234 5678 9012 3456'")
        return
    
    set_setting("requisites", new_requisites)
    await state.finish()
    await message.answer("Реквизиты обновлены!")
    await send_admin_menu(message.chat.id)

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:edit_desc:"), state="*")
async def admin_edit_desc_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_name=product_name)
    # Use send_message instead of edit_text to avoid BadRequest
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        chat_id=call.message.chat.id,
        text=f"Введите новое описание для товара <b>{html.escape(str(product_name))}</b>:",
        parse_mode="HTML"
    )
    await Admin.EditProduct_Desc.set()
    await call.answer()

@dp.message_handler(state=Admin.EditProduct_Desc)
async def admin_edit_desc_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_name")
    new_description = message.text

    if product_name and new_description:
        update_product(product_name, description=new_description)
        await message.answer("Описание товара успешно обновлено!")
        # Go back to product management menu for this product
        product_data = get_product(product_name)
        if product_data:
            text = f"Управление товаром: <b>{html.escape(str(product_data['name']))}</b>\n\n"
            text += f"Описание: {html.escape(str(product_data['description']))}"

            if product_data['photo_file_id']:
                await bot.send_photo(
                    message.chat.id,
                    product_data['photo_file_id'],
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=admin_manage_product_kb(product_data)
                )
            else:
                await message.answer(
                    text,
                    parse_mode="HTML",
                    reply_markup=admin_manage_product_kb(product_data)
                )
        else:
            await admin_products_menu_handler(message, state) # Fallback to product list if product not found
    else:
        await message.answer("Ошибка при обновлении описания. Пожалуйста, попробуйте еще раз.")
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:edit_photo:"), state="*")
async def admin_edit_photo_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_name=product_name)
    try:
        await call.message.delete() # Delete the previous message with product details
    except Exception:
        pass
    await bot.send_message(
        call.message.chat.id,
        f"Отправьте новое фото для товара <b>{html.escape(str(product_name))}</b>:",
        parse_mode="HTML"
    )
    await Admin.EditProduct_Photo.set()
    await call.answer()

@dp.message_handler(state=Admin.EditProduct_Photo, content_types=types.ContentType.PHOTO)
async def admin_edit_photo_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_name")
    photo_file_id = message.photo[-1].file_id

    if product_name and photo_file_id:
        update_product(product_name, photo_file_id=photo_file_id)
        await message.answer("Фото товара успешно обновлено!")
        # Go back to product management menu for this product
        product_data = get_product(product_name)
        if product_data:
            text = f"Управление товаром: <b>{html.escape(str(product_data['name']))}</b>\n\n"
            text += f"Описание: {html.escape(str(product_data['description']))}"

            await bot.send_photo(
                message.chat.id,
                product_data['photo_file_id'],
                caption=text,
                parse_mode="HTML",
                reply_markup=admin_manage_product_kb(product_data)
            )
        else:
            await admin_products_menu_handler(message, state) # Fallback to product list if product not found
    else:
        await message.answer("Ошибка при обновлении фото. Пожалуйста, попробуйте еще раз.")
    await state.finish()

@dp.message_handler(text="📢 Рассылка", state="*")
async def admin_broadcast_start(message: types.Message, state: FSMContext):
    # Проверка на админа
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        await send_main_menu(message.chat.id, "Возвращаю вас в главное меню.", state)
        return
    await Admin.BroadcastMessage.set()
    await message.answer("Отправьте сообщение для рассылки всем пользователям:")

@dp.message_handler(state=Admin.BroadcastMessage)
async def admin_broadcast_message(message: types.Message, state: FSMContext):
    await state.update_data(broadcast_text=message.text)
    await Admin.ConfirmBroadcast.set()
    await message.answer(
        f"Вы собираетесь отправить следующее сообщение:\n\n<code>{html.escape(str(message.text))}</code>\n\nПодтвердите отправку:",
        parse_mode="HTML",
        reply_markup=admin_broadcast_confirm_kb()
    )

@dp.callback_query_handler(lambda c: c.data.startswith("broadcast:"), state=Admin.ConfirmBroadcast)
async def admin_broadcast_confirm(call: types.CallbackQuery, state: FSMContext):
    action = call.data.split(":")[1]
    if action == "confirm":
        data = await state.get_data()
        broadcast_text = data.get("broadcast_text")

        if broadcast_text:
            await call.message.edit_text("Начинаю рассылку...")
            
            users = get_all_users()
            for user in users:
                try:
                    await bot.send_message(user['telegram_id'], broadcast_text)
                except Exception as e:
                    logging.error(f"Failed to send broadcast to user {user['telegram_id']}: {e}")
            
            await call.message.answer("Рассылка завершена.")
            await send_admin_menu(call.message.chat.id)
        else:
            await call.message.answer("Ошибка: текст рассылки не найден.")
            await send_admin_menu(call.message.chat.id)
    elif action == "cancel":
        await call.message.edit_text("Рассылка отменена.")
        await send_admin_menu(call.message.chat.id)
    
    await state.finish()
    await call.answer()

@dp.message_handler(text="📊 Оплаты", state="*")
async def admin_payments_list_handler(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("У вас нет доступа к этому разделу.")
        await send_admin_menu(message.chat.id, state=state) # Ensure admin menu is returned
        return
    
    # No need for Admin.PaymentsList state, directly show filter options
    await message.answer("Выберите статус платежей для просмотра:", reply_markup=admin_payments_kb())

@dp.callback_query_handler(cb_admin_payments.filter(action="filter"), state="*") # Removed Admin.PaymentsList state
async def admin_filter_payments(call: types.CallbackQuery, state: FSMContext, callback_data: dict):
    # Добавляем отладочную информацию
    logging.info(f"admin_filter_payments called with callback_data: {callback_data}")
    
    status_filter = callback_data["status"]
    
    # Handle time period filters
    from datetime import datetime, timedelta
    time_filter = None
    
    if status_filter == "today":
        # Today: from 00:00 of the current day
        time_filter = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        status_filter = "completed"  # Only show completed payments for time filters
        logging.info(f"Filtering for today: {time_filter}")
    elif status_filter == "week":
        # Week: from Monday of the current week
        today = datetime.now()
        time_filter = today - timedelta(days=today.weekday())
        time_filter = time_filter.replace(hour=0, minute=0, second=0, microsecond=0)
        status_filter = "completed"
        logging.info(f"Filtering for week: {time_filter}")
    elif status_filter == "month":
        # Month: from the 1st day of the current month
        time_filter = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        status_filter = "completed"
        logging.info(f"Filtering for month: {time_filter}")
    
    # Get payments based on filters
    payments = list_payments(status_filter)
    logging.info(f"Found {len(payments)} payments with status '{status_filter}'")
    
    # Apply time filter if needed
    if time_filter:
        payments = [p for p in payments if p['created_at'] >= time_filter]
        logging.info(f"After time filter: {len(payments)} payments")
    
    if not payments:
        filter_name = {
            "today": "сегодня",
            "week": "за неделю",
            "month": "за месяц"
        }.get(callback_data["status"], callback_data["status"])
        
        await call.message.edit_text(f"Нет платежей за период: {filter_name}", reply_markup=admin_payments_kb())
        await call.answer()
        return
    
    # Новый красивый HTML-вывод успешных оплат
    period_names = {"today": "за сегодня", "week": "за неделю", "month": "за месяц"}
    period = period_names.get(callback_data["status"], callback_data["status"])
    payments_text = f"📝 <b>Успешные оплаты {period}:</b>\n\n"
    for idx, payment in enumerate(payments, 1):
        user = get_user_by_id(payment["user_id"])
        username = f"@{user['username']}" if user and user['username'] else f"ID: {payment['user_id']}"
        product = get_product(payment["product"])
        plan = get_plan(payment["plan_id"])
        promo = payment.get("promo_code", "—")
        date = payment["created_at"].strftime("%d.%m.%Y") if payment["created_at"] else "—"
        price = int(payment["price"]) if payment["price"] else 0
        payments_text += (
            f"{idx}. <b>#{payment['id']}</b> | 👤 <b>{username}</b> | "
            f"📦 <b>{product['name'] if product else payment['product']}</b> ({plan['name'] if plan else ''}) | "
            f"🪙 <b>{price}₽</b> | 📅 <b>{date}</b>\n"
            f"🔑 <b>Промокод:</b> <code>{promo}</code>\n\n"
        )
    payments_text += "..."
    
    payments_kb = InlineKeyboardMarkup(row_width=4)
    payments_kb.add(
        InlineKeyboardButton("⬅️ Назад", callback_data="admin_back_to_main"),
        InlineKeyboardButton("[Сегодня]", callback_data=cb_admin_payments.new(action="filter", status="today", payment_id="0")),
        InlineKeyboardButton("[Неделя]", callback_data=cb_admin_payments.new(action="filter", status="week", payment_id="0")),
        InlineKeyboardButton("[Месяц]", callback_data=cb_admin_payments.new(action="filter", status="month", payment_id="0")),
    )
    try:
        await call.message.edit_text(payments_text, reply_markup=payments_kb, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error editing message in admin_filter_payments: {e}")
        try:
            await call.message.delete()
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, payments_text, reply_markup=payments_kb, parse_mode="HTML")
    await call.answer()

@dp.callback_query_handler(cb_admin_payments.filter(action="view"), state="*") # Removed Admin.PaymentsList state
async def admin_view_payment_details(call: types.CallbackQuery, state: FSMContext, callback_data: dict):
    payment_id = int(callback_data["payment_id"])
    payment = get_payment_by_id(payment_id)
    
    if not payment:
        await call.answer("Ошибка: Платеж не найден.", show_alert=True)
        await admin_payments_list_handler(call.message, state) # Go back to payments list
        return
        
    user = get_user_by_id(payment["user_id"])
    username = f"@{markdown_escape(user['username'])}" if user and user['username'] else f"ID: {payment['user_id']}"
    
    product_data = get_product(payment["product"])
    product_name = markdown_escape(product_data["name"]) if product_data else "N/A"
    
    plan_data = get_plan(payment["plan_id"])
    plan_name = markdown_escape(plan_data["name"]) if plan_data else "N/A"
    
    promo_code_data = get_promo_code_by_code(payment["promo_code"])
    promo_code_val = markdown_escape(promo_code_data["code"]) if promo_code_data else "N/A"

    updated_at_formatted = payment['updated_at'].strftime('%Y-%m-%d %H:%M:%S') if payment['updated_at'] else "N/A"
    created_at_formatted = payment['created_at'].strftime('%Y-%m-%d %H:%M:%S')

    message_lines = [
        f"ID платежа: {payment['id']}",
        f"Пользователь: {username}",
        f"Продукт: {product_name}",
        f"Тариф: {plan_name}",
        f"Email: {markdown_escape(payment['email'] or 'N/A')}",
        f"Promo-code: {promo_code_val}",
        f"Дата создания: {markdown_escape(created_at_formatted)}",
        f"Дата обновления: {markdown_escape(updated_at_formatted)} (если есть)"
    ]
    message_text = "\n".join(message_lines)
    
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔙 К списку платежей", callback_data=cb_admin_payments.new(action="filter", status="all", payment_id="0")))
    
    await call.message.edit_text(message_text, reply_markup=kb, parse_mode="MarkdownV2")
    await call.answer()

@dp.message_handler(text="Выход из админ-панели", state="*")
async def admin_exit(message: types.Message, state: FSMContext):
    await send_main_menu(message.chat.id, "Выход из админ панели. Возвращаю вас в главное меню.", state)

@dp.callback_query_handler(lambda c: c.data == "admin_prod:add", state="*")
async def admin_add_product_start(call: types.CallbackQuery, state: FSMContext):
    await Admin.AddProduct_Name.set()
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(call.message.chat.id, "Введите название нового товара:")
    await call.answer()

@dp.message_handler(state=Admin.AddProduct_Name)
async def admin_add_product_name(message: types.Message, state: FSMContext):
    await state.update_data(product_name=message.text)
    await Admin.AddProduct_Desc.set()
    await message.answer("Введите описание для нового товара:")

@dp.message_handler(state=Admin.AddProduct_Desc)
async def admin_add_product_desc(message: types.Message, state: FSMContext):
    await state.update_data(product_description=message.text)
    await Admin.AddProduct_Photo.set()
    await message.answer("Теперь отправьте фото для нового товара:")

@dp.message_handler(state=Admin.AddProduct_Photo, content_types=types.ContentType.PHOTO)
async def admin_add_product_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    name = data['product_name']
    description = data['product_description']
    photo_file_id = message.photo[-1].file_id

    add_product(name, description, photo_file_id)
    await state.finish()
    await message.answer("Товар успешно добавлен!")
    # Call the product list menu to refresh and show new product
    await admin_products_menu_handler(message, state) # Pass message and state to the function

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:view:"), state="*")
async def admin_view_product(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    product_data = get_product(product_name)
    if not product_data:
        await call.answer("Продукт не найден.", show_alert=True)
        await admin_products_menu_handler(call.message, state) # Go back to products list
        return

    text = f"Управление товаром: <b>{html.escape(str(product_data['name']))}</b>\n\n"
    text += f"Описание: {html.escape(str(product_data['description']))}"

    try:
        if call.message.photo: # If the previous message was a photo, edit caption or send new
            await call.message.edit_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=admin_manage_product_kb(product_data)
            )
        elif call.message.text: # If previous message was text, edit it
            await call.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=admin_manage_product_kb(product_data)
            )
        else: # If unexpected message type, send a new photo or text message
            if product_data['photo_file_id']:
                await bot.send_photo(
                    call.message.chat.id,
                    product_data['photo_file_id'],
                    caption=text,
                    parse_mode="HTML",
                    reply_markup=admin_manage_product_kb(product_data)
                )
            else:
                await bot.send_message(
                    call.message.chat.id,
                    text,
                    parse_mode="HTML",
                    reply_markup=admin_manage_product_kb(product_data)
                )
    except Exception as e:
        logging.error(f"Error editing/sending product details: {e}")
        # Fallback: delete previous message and send new one
        try:
            await call.message.delete()
        except Exception:
            pass
        if product_data['photo_file_id']:
            await bot.send_photo(
                call.message.chat.id,
                product_data['photo_file_id'],
                caption=text,
                parse_mode="HTML",
                reply_markup=admin_manage_product_kb(product_data)
            )
        else:
            await bot.send_message(
                call.message.chat.id,
                text,
                parse_mode="HTML",
                reply_markup=admin_manage_product_kb(product_data)
            )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:toggle:"), state="*")
async def admin_toggle_product_visibility(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    product_data = get_product(product_name)

    if not product_data:
        await call.answer("Продукт не найден.", show_alert=True)
        await admin_products_menu_handler(call.message, state)
        return
    
    new_status = not product_data['active']
    update_product(product_name, active=new_status)

    status_text = "скрыт из витрины" if not new_status else "показан на витрине"
    await call.answer(f"Товар **{markdown_escape(product_name)}** теперь {status_text}.", show_alert=True)

    # Refresh product management menu
    updated_product_data = get_product(product_name)
    if updated_product_data:
        text = f"Управление товаром: **{markdown_escape(updated_product_data['name'])}**\n\n"
        text += f"Описание: {markdown_escape(updated_product_data['description'])}"

        try:
            if call.message.photo:
                await call.message.edit_caption(
                    caption=text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(updated_product_data)
                )
            else:
                await call.message.edit_text(
                    text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(updated_product_data)
                )
        except Exception as e:
            logging.error(f"Error editing message after toggle: {e}")
            try:
                await call.message.delete()
            except Exception:
                pass
            if updated_product_data['photo_file_id']:
                await bot.send_photo(
                    call.message.chat.id,
                    updated_product_data['photo_file_id'],
                    caption=text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(updated_product_data)
                )
            else:
                await bot.send_message(
                    call.message.chat.id,
                    text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(updated_product_data)
                )
    else:
        await admin_products_menu_handler(call.message, state)

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:delete:") and not c.data.startswith("admin_prod:delete_confirm:") and not c.data.startswith("admin_prod:delete_cancel:"), state="*")
async def admin_delete_product_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_to_delete=product_name)
    try:
        await call.message.delete() # Delete the previous message with product details
    except Exception:
        pass
    await bot.send_message(
        call.message.chat.id,
        f"Вы уверены, что хотите удалить товар **{markdown_escape(product_name)}**? Это действие необратимо!",
        parse_mode="MarkdownV2",
        reply_markup=admin_delete_confirm_kb(product_name)
    )
    await Admin.ConfirmProductDeletion.set()
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:delete_confirm:"), state=Admin.ConfirmProductDeletion)
async def admin_delete_product_confirmed(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_to_delete")

    if product_name:
        success = delete_product(product_name)
        if success:
            await call.message.edit_text(
                f"Товар **{markdown_escape(product_name)}** успешно удален.",
                parse_mode="MarkdownV2"
            )
        else:
            await call.message.edit_text(
                f"Ошибка: не удалось удалить товар **{markdown_escape(product_name)}**.",
                parse_mode="MarkdownV2"
            )
    else:
        await call.message.edit_text("Ошибка: не удалось удалить товар.")

    await state.finish()
    await admin_products_menu_handler(call.message, state) # Go back to products list
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_prod:delete_cancel:"), state=Admin.ConfirmProductDeletion)
async def admin_delete_product_cancelled(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_to_delete")
    await call.message.edit_text(
        f"Удаление товара **{markdown_escape(product_name)}** отменено.",
        parse_mode="MarkdownV2",
        reply_markup=ReplyKeyboardRemove() # Remove inline keyboard temporarily
    )
    # Re-display the product management menu if product still exists
    product_data = get_product(product_name)
    if product_data:
        text = f"Управление товаром: **{markdown_escape(product_data['name'])}**\n\n"
        text += f"Описание: {markdown_escape(product_data['description'])}"

        try:
            if product_data['photo_file_id']:
                await bot.send_photo(
                    call.message.chat.id,
                    product_data['photo_file_id'],
                    caption=text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(product_data)
                )
            else:
                await bot.send_message(
                    call.message.chat.id,
                    text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(product_data)
                )
        except Exception as e:
            logging.error(f"Error re-displaying product menu after cancel: {e}")
            try:
                await call.message.delete()
            except Exception:
                pass
            if product_data['photo_file_id']:
                await bot.send_photo(
                    call.message.chat.id,
                    product_data['photo_file_id'],
                    caption=text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(product_data)
                )
            else:
                await bot.send_message(
                    call.message.chat.id,
                    text,
                    parse_mode="MarkdownV2",
                    reply_markup=admin_manage_product_kb(product_data)
                )
    else:
        # If product was somehow not found after cancellation, go back to product list
        await admin_products_menu_handler(call.message, state)
    await state.finish()
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_plan:list:"), state="*")
async def admin_list_plans(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    product_data = get_product(product_name)
    if not product_data:
        await call.answer("Продукт не найден.", show_alert=True)
        await admin_products_menu_handler(call.message, state)
        return

    await state.update_data(current_product_name=product_name)

    text = f"Тарифные планы для товара **{markdown_escape(product_name)}**:\n"

    try:
        if call.message.photo:
            await call.message.edit_caption(
                caption=text,
                parse_mode="MarkdownV2",
                reply_markup=admin_plans_kb(product_name)
            )
        else:
            await call.message.edit_text(
                text,
                parse_mode="MarkdownV2",
                reply_markup=admin_plans_kb(product_name)
            )
    except Exception as e:
        logging.error(f"Error editing message for plan list: {e}")
        try:
            await call.message.delete()
        except Exception:
            pass
        if product_data['photo_file_id']: # Send photo if product has one, otherwise just text
            await bot.send_photo(
                call.message.chat.id,
                product_data['photo_file_id'],
                caption=text,
                parse_mode="MarkdownV2",
                reply_markup=admin_plans_kb(product_name)
            )
        else:
            await bot.send_message(
                call.message.chat.id,
                text,
                parse_mode="MarkdownV2",
                reply_markup=admin_plans_kb(product_name)
            )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin_plan:add:"), state="*")
async def admin_add_plan_start(call: types.CallbackQuery, state: FSMContext):
    product_name = call.data.split(":")[2]
    await state.update_data(product_for_plan=product_name)
    try:
        await call.message.delete()
    except Exception:
        pass
    await bot.send_message(
        chat_id=call.message.chat.id,
        text=f"Введите данные для нового тарифа для товара **{markdown_escape(product_name)}** в формате:\n\n`Название тарифа;Количество дней;Цена`\n\n*Пример: Базовый;30;1000*\n*Для бессрочного тарифа: Премиум;;2500*\n",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Отмена", callback_data=f"admin_plan:list:{product_name}"))
    )
    await Admin.AddPlan.set()
    await call.answer()

@dp.message_handler(state=Admin.AddPlan)
async def admin_process_add_plan(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_name = data.get("product_for_plan")

    if not product_name:
        await message.answer("Ошибка: Не удалось определить товар для добавления тарифа.")
        await send_admin_menu(message.chat.id, "Возвращаю вас в админ-панель.", state)
        return

    try:
        parts = message.text.split(';')
        if len(parts) != 3:
            raise ValueError("Неверный формат ввода. Используйте: Название;Дни;Цена")

        plan_name = parts[0].strip()
        days_str = parts[1].strip()
        plan_days = int(days_str) if days_str else None
        plan_price = int(parts[2].strip())  # Store price in cents

        if not plan_name:
            raise ValueError("Название тарифа не может быть пустым.")
        if plan_days is not None and plan_days <= 0:
            raise ValueError("Количество дней должно быть положительным числом или пустым для бессрочного тарифа.")
        if plan_price <= 0:
            raise ValueError("Цена должна быть положительным числом.")

        add_plan(product_name, plan_name, plan_days, plan_price)
        await message.answer(f"Тариф **{markdown_escape(plan_name)}** для товара **{markdown_escape(product_name)}** успешно добавлен!", parse_mode="MarkdownV2")
        
        await state.finish()
        # Simulate a callback_query to return to the plans list
        simulated_call = types.CallbackQuery(id='fake', from_user=message.from_user, chat_instance='fake', data=f"admin_plan:list:{product_name}")
        await admin_list_plans(simulated_call, state)

    except ValueError as e:
        await message.answer(f"Ошибка ввода: {markdown_escape(str(e))}. Пожалуйста, попробуйте еще раз.", parse_mode="MarkdownV2")
    except Exception as e:
        logging.error(f"Error adding plan: {e}")
        await message.answer("Произошла неизвестная ошибка при добавлении тарифа. Пожалуйста, попробуйте позже.")
        await send_admin_menu(message.chat.id, "Возвращаю вас в админ-панель.", state)

@dp.callback_query_handler(lambda c: c.data.startswith("admin_plan:delete:"), state="*")
async def admin_delete_plan(call: types.CallbackQuery, state: FSMContext):
    plan_id = int(call.data.split(":")[2])
    plan = get_plan(plan_id)
    if not plan:
        await call.answer("Тариф не найден.", show_alert=True)
        await send_admin_menu(call.message.chat.id, "Возвращаю в админ-панель.", state)
        return

    product_name = plan['product']
    delete_plan(plan_id)
    
    await call.answer("Тариф удален.", show_alert=True)
    # Simulate a callback query to re-list plans for the product
    simulated_call = types.CallbackQuery(id='fake', from_user=call.from_user, chat_instance='fake', data=f"admin_plan:list:{product_name}")
    await admin_list_plans(simulated_call, state)

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)