from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Boolean, Text, JSON, Float
from sqlalchemy.orm import sessionmaker, relationship, joinedload
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional, List

import logging

import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = config.DATABASE_URL
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(Integer, unique=True, nullable=False, index=True)
    username = Column(String(64))
    email = Column(String(256))
    referrer_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    referral_balance = Column(Integer, default=0)  # копейки / центы
    created_at = Column(DateTime, default=datetime.utcnow)

    referrals = relationship("User", backref="referrer", remote_side=[id])
    subscriptions = relationship("Subscription", back_populates="user")

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    product = Column(String(32), nullable=False)
    tariff = Column(String(32), nullable=False)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime)
    active = Column(Boolean, default=False)

    user = relationship("User", back_populates="subscriptions")

class Payment(Base):
    __tablename__ = "payments"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    product = Column(String(32), nullable=False)
    tariff = Column(String(32), nullable=False)
    email = Column(String(256), nullable=True)
    price = Column(Float, nullable=False)
    check_file_id = Column(Text, nullable=True)
    status = Column(String(16), default="pending")  # pending/completed/rejected
    created_at = Column(DateTime, default=datetime.utcnow)
    plan_id = Column(Integer, ForeignKey("plans.id"))
    promo_code = Column(String(128), nullable=True)

    user = relationship("User")
    plan = relationship("Plan")

class Product(Base):
    __tablename__ = "products"

    name = Column(String(32), primary_key=True)
    active = Column(Boolean, default=True)
    description = Column(Text)
    price = Column(Integer)  # базовая цена по умолчанию
    photo_file_id = Column(String(256))

class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(64), primary_key=True)
    value = Column(Text)

class PromoCode(Base):
    __tablename__ = "promo_codes"

    id = Column(Integer, primary_key=True)
    product_id = Column(String(32), ForeignKey("products.name"), nullable=False)
    plan_id = Column(Integer, ForeignKey("plans.id"), nullable=True)
    code = Column(String(128), unique=True, nullable=False)
    status = Column(String(16), default="not issued")
    issued_to_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    issued_to_email = Column(String(256), nullable=True)
    issued_at = Column(DateTime, nullable=True)
    payment_id = Column(Integer, ForeignKey("payments.id"), nullable=True)

    product_rel = relationship("Product")
    plan_rel = relationship("Plan")
    user_rel = relationship("User")
    payment_rel = relationship("Payment")

class Broadcast(Base):
    __tablename__ = "broadcasts"

    id = Column(Integer, primary_key=True)
    text = Column(Text, nullable=False)
    sent_at = Column(DateTime, default=datetime.utcnow)
    delivered = Column(Integer, default=0)

class Plan(Base):
    __tablename__ = "plans"

    id = Column(Integer, primary_key=True)
    product = Column(String(32), ForeignKey("products.name"))
    name = Column(String(32))
    days = Column(Integer, nullable=True)  # None = бессрочно
    price = Column(Integer, nullable=False)

    product_rel = relationship("Product")

class Material(Base):
    __tablename__ = 'materials'
    id = Column(Integer, primary_key=True)
    product_name = Column(String, ForeignKey('products.name'), nullable=False)
    title = Column(String, nullable=False)
    text = Column(Text, nullable=True)
    file_id = Column(String, nullable=True)
    
    product = relationship("Product")

class WithdrawalRequest(Base):
    __tablename__ = 'withdrawal_requests'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    amount = Column(Integer, nullable=False)  # Сумма в копейках/центах
    status = Column(String(16), default='pending')  # pending/approved/rejected
    request_date = Column(DateTime, default=datetime.utcnow)
    admin_decision_date = Column(DateTime, nullable=True)
    phone = Column(String(20), nullable=True)  # Номер телефона для СБП
    bank = Column(String(100), nullable=True)  # Название банка

    user = relationship("User")

# --- helpers -----------------------------------------------------------------

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# ----------------------------- util functions --------------------------------

def get_or_create_user(telegram_id: int, username: Optional[str] = None, referrer_id: Optional[int] = None) -> dict:
    with session_scope() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if not user:
            # Check if referrer exists
            if referrer_id:
                referrer = session.query(User).filter_by(telegram_id=referrer_id).first()
                if not referrer:
                    referrer_id = None # Referrer not found, ignore
            
            user = User(
                telegram_id=telegram_id, 
                username=username or "",
                referrer_id=referrer.id if referrer_id else None
            )
            session.add(user)
            session.commit()
        
        return {
            "id": user.id,
            "telegram_id": user.telegram_id,
            "username": user.username,
            "email": user.email,
            "referrer_id": user.referrer_id,
            "referral_balance": user.referral_balance,
            "created_at": user.created_at,
        }


def add_payment(user_id: int, product: str, tariff: str, price: float, status: str, plan_id: int, check_file_id: Optional[str] = None, email: Optional[str] = None, promo_code: Optional[str] = None) -> dict:
    with session_scope() as session:
        user = session.query(User).filter_by(id=user_id).first()
        if not user:
            raise ValueError("User not found")

        new_payment = Payment(
            user_id=user.id,
            product=product,
            tariff=tariff,
            price=price,
            status=status,
            check_file_id=check_file_id,
            plan_id=plan_id,
            email=email,
            promo_code=promo_code
        )
        session.add(new_payment)
        session.commit()
        
        # Возвращаем словарь вместо объекта Payment
        return {
            "id": new_payment.id,
            "user_id": new_payment.user_id,
            "product": new_payment.product,
            "tariff": new_payment.tariff,
            "email": new_payment.email,
            "price": new_payment.price,
            "check_file_id": new_payment.check_file_id,
            "status": new_payment.status,
            "created_at": new_payment.created_at,
            "plan_id": new_payment.plan_id,
            "promo_code": new_payment.promo_code,
        }

def get_payment_by_id(payment_id: int) -> Optional[dict]:
    with session_scope() as session:
        payment = session.get(Payment, payment_id)
        if payment:
            return {
                "id": payment.id,
                "user_id": payment.user_id,
                "product": payment.product,
                "tariff": payment.tariff,
                "email": payment.email,
                "price": payment.price,
                "check_file_id": payment.check_file_id,
                "status": payment.status,
                "created_at": payment.created_at,
                "plan_id": payment.plan_id,
                "promo_code": payment.promo_code,
            }
        return None


def attach_check(payment_id: int, file_id: str) -> Optional[dict]:
    with session_scope() as session:
        payment = session.get(Payment, payment_id)
        if payment:
            payment.check_file_id = file_id
            payment.status = "pending"
            session.commit()
            return {
                "id": payment.id,
                "user_id": payment.user_id,
                "product": payment.product,
                "tariff": payment.tariff,
                "email": payment.email,
                "price": payment.price,
                "check_file_id": payment.check_file_id,
                "status": payment.status,
                "created_at": payment.created_at,
                "plan_id": payment.plan_id,
                "promo_code": payment.promo_code,
            }
        return None


def set_payment_status(payment_id: int, status: str):
    with session_scope() as session:
        payment = session.get(Payment, payment_id)
        if payment:
            payment.status = status
            if status == "completed" and payment.promo_code:
                promo = session.query(PromoCode).filter_by(code=payment.promo_code).first()
                if promo:
                    promo.status = "issued"
                    promo.payment_id = payment_id  # Устанавливаем связь с платежом
                    promo.issued_at = datetime.utcnow()  # Устанавливаем время выдачи
                    promo.issued_to_user_id = payment.user_id  # Устанавливаем пользователя
                    if payment.email:
                        promo.issued_to_email = payment.email  # Устанавливаем email
            session.commit()

def update_payment(payment_id: int, **kwargs) -> Optional[dict]:
    with session_scope() as session:
        payment = session.get(Payment, payment_id)
        if payment:
            for key, value in kwargs.items():
                setattr(payment, key, value)
            session.commit()
            return {
                "id": payment.id,
                "user_id": payment.user_id,
                "product": payment.product,
                "tariff": payment.tariff,
                "email": payment.email,
                "price": payment.price,
                "check_file_id": payment.check_file_id,
                "status": payment.status,
                "created_at": payment.created_at,
                "plan_id": payment.plan_id,
            }
        return None



def activate_subscription(user_id: int, plan_id: int) -> Optional[dict]:
    with session_scope() as session:
        plan = session.get(Plan, plan_id)
        if not plan:
            return None # Or raise error

        start = datetime.utcnow()
        end = None if plan.days is None else start + timedelta(days=plan.days)
        
        # Deactivate old subscriptions for the same product
        session.query(Subscription).filter(
            Subscription.user_id == user_id,
            Subscription.product == plan.product
        ).update({"active": False})

        subscription = Subscription(
            user_id=user_id,
            product=plan.product,
            tariff=plan.name,
            start_date=start,
            end_date=end,
            active=True,
        )
        session.add(subscription)
        session.commit()
        
        # Возвращаем словарь вместо объекта Subscription
        return {
            "id": subscription.id,
            "user_id": subscription.user_id,
            "product": subscription.product,
            "tariff": subscription.tariff,
            "start_date": subscription.start_date,
            "end_date": subscription.end_date,
            "active": subscription.active,
        }

# -----------------------------------------------------------------------------

def init_db():
    Base.metadata.create_all(engine)

# -------------------------- settings helpers ---------------------------------

def get_setting(key: str) -> Optional[str]:
    with session_scope() as session:
        setting: Optional[Setting] = session.query(Setting).filter_by(key=key).first()
        return setting.value if setting else None

def set_setting(key: str, value: str):
    with session_scope() as session:
        setting = session.query(Setting).filter_by(key=key).first()
        if setting:
            setting.value = value
        else:
            setting = Setting(key=key, value=value)
            session.add(setting)
        session.commit()

def get_all_users() -> List[dict]:
    with session_scope() as session:
        users = session.query(User).all()
        return [
            {
                "id": u.id,
                "telegram_id": u.telegram_id,
                "username": u.username,
                "email": u.email,
                "referrer_id": u.referrer_id,
                "referral_balance": u.referral_balance,
                "created_at": u.created_at,
            }
            for u in users
        ]

def get_user_by_telegram_id(telegram_id: int) -> Optional[dict]:
    with session_scope() as session:
        user = session.query(User).filter_by(telegram_id=telegram_id).first()
        if user:
            return {
                "id": user.id,
                "telegram_id": user.telegram_id,
                "username": user.username,
                "email": user.email,
                "referrer_id": user.referrer_id,
                "referral_balance": user.referral_balance,
                "created_at": user.created_at,
            }
        return None

def get_user_by_id(user_id: int) -> Optional[dict]:
    with session_scope() as session:
        user = session.query(User).filter_by(id=user_id).first()
        if user:
            return {
                "id": user.id,
                "telegram_id": user.telegram_id,
                "username": user.username,
                "email": user.email,
                "referrer_id": user.referrer_id,
                "referral_balance": user.referral_balance,
                "created_at": user.created_at,
            }
        return None

def add_product(name: str, description: str, photo_file_id: str, active: bool = True) -> dict:
    with session_scope() as session:
        product = Product(
            name=name,
            description=description,
            photo_file_id=photo_file_id,
            active=active
        )
        session.add(product)
        session.commit()
        return {
            "name": product.name,
            "description": product.description,
            "photo_file_id": product.photo_file_id,
            "active": product.active,
            "price": product.price # Ensure price is included for consistency
        }

def ensure_product(name: str):
    with session_scope() as session:
        product = session.query(Product).filter_by(name=name).first()
        if not product:
            # Create a basic product if it doesn't exist, e.g., for testing or initial setup
            product = Product(name=name, description="Default product", photo_file_id="", active=True, price=0)
            session.add(product)
            session.commit()

def list_active_products() -> List[dict]:
    with session_scope() as session:
        products = session.query(Product).filter_by(active=True).all()
        return [
            {
                "name": p.name,
                "active": p.active,
                "description": p.description,
                "price": p.price,
                "photo_file_id": p.photo_file_id,
            }
            for p in products
        ]

def set_product_active(name: str, active: bool):
    with session_scope() as session:
        product = session.query(Product).filter_by(name=name).first()
        if product:
            product.active = active
            session.commit()

def update_product(name: str, **kwargs):
    with session_scope() as session:
        product = session.query(Product).filter_by(name=name).first()
        if product:
            for key, value in kwargs.items():
                setattr(product, key, value)
            session.commit()

def add_promo_codes(product: str, codes: list[str]):
    with session_scope() as session:
        for code_str in codes:
            promo = PromoCode(product_id=product, code=code_str, status="not issued")
            session.add(promo)
        session.commit()

def get_unused_promocode(product: str) -> Optional[str]:
    with session_scope() as session:
        promo = session.query(PromoCode).filter_by(product_id=product, status="not issued").first()
        if promo:
            # Просто возвращаем код, но не меняем статус
            # Статус будет изменен в set_payment_status при подтверждении платежа
            return promo.code
        return None

def get_product(name: str) -> Optional[dict]:
    with session_scope() as session:
        product = session.query(Product).filter_by(name=name).first()
        if product:
            return {
                "name": product.name,
                "active": product.active,
                "description": product.description,
                "price": product.price,
                "photo_file_id": product.photo_file_id,
            }
        return None

def get_promo_code_by_code(code: str) -> Optional[dict]:
    with session_scope() as session:
        promo = session.query(PromoCode).filter_by(code=code).first()
        if promo:
            return {
                "id": promo.id,
                "product_id": promo.product_id,
                "code": promo.code,
                "status": promo.status,
            }
        return None

def list_all_products() -> List[dict]:
    with session_scope() as session:
        products = session.query(Product).all()
        return [
            {
                "name": p.name,
                "active": p.active,
                "description": p.description,
                "price": p.price,
                "photo_file_id": p.photo_file_id,
            }
            for p in products
        ]

def count_unused_promos(product: str) -> int:
    with session_scope() as session:
        return session.query(PromoCode).filter_by(product_id=product, status="not issued").count()

def get_promo_codes_by_product(product_name: str, status: Optional[str] = None) -> List[dict]:
    with session_scope() as session:
        query = session.query(PromoCode).filter_by(product_id=product_name)
        if status == "unused":
            query = query.filter_by(status="not issued")
        elif status == "used":
            query = query.filter_by(status="issued")
        
        promos = query.all()
        return [
            {
                "id": p.id,
                "product_id": p.product_id,
                "code": p.code,
                "status": p.status,
                "payment_id": p.payment_rel.id if p.payment_rel and p.payment_rel.promo_code == p.code else None
            }
            for p in promos
        ]

def add_promo_codes_bulk(product_name: str, codes: List[str]) -> tuple[int, List[str]]:
    added_count = 0
    duplicates = []
    with session_scope() as session:
        existing_codes = {p.code for p in session.query(PromoCode).filter_by(product_id=product_name).all()}
        
        for code_str in codes:
            if code_str not in existing_codes:
                promo = PromoCode(product_id=product_name, code=code_str, status="not issued")
                session.add(promo)
                added_count += 1
            else:
                duplicates.append(code_str)
        session.commit()
    return added_count, duplicates

def delete_promo_code(promo_code_id: int) -> bool:
    with session_scope() as session:
        promo = session.get(PromoCode, promo_code_id)
        if promo:
            session.delete(promo)
            session.commit()
            return True
        return False

def add_broadcast(text: str, delivered: int = 0):
    with session_scope() as session:
        broadcast = Broadcast(text=text, delivered=delivered)
        session.add(broadcast)
        session.commit()

def list_broadcasts(limit: int = 20, offset: int = 0):
    with session_scope() as session:
        broadcasts = session.query(Broadcast).order_by(Broadcast.sent_at.desc()).limit(limit).offset(offset).all()
        return [
            {
                "id": b.id,
                "text": b.text,
                "sent_at": b.sent_at,
                "delivered": b.delivered,
            }
            for b in broadcasts
        ]

def list_payments(status: Optional[str] = None) -> List[dict]:
    logging.info(f"list_payments called with status: {status}")
    with session_scope() as session:
        query = session.query(Payment)
        if status and status != "all":
            query = query.filter_by(status=status)
        payments = query.order_by(Payment.created_at.desc()).all()
        logging.info(f"Found {len(payments)} payments in database")
        
        result = [
            {
                "id": p.id,
                "user_id": p.user_id,
                "product": p.product,
                "tariff": p.tariff,
                "email": p.email,
                "price": p.price,
                "check_file_id": p.check_file_id,
                "status": p.status,
                "created_at": p.created_at,
                "plan_id": p.plan_id,
                "promo_code": p.promo_code,
            }
            for p in payments
        ]
        logging.info(f"Returning {len(result)} payments")
        return result

def add_plan(product: str, name: str, days: Optional[int], price: int):
    logging.info(f"Attempting to add plan: product={product}, name={name}, days={days}, price={price}")
    with session_scope() as session:
        new_plan = Plan(
            product=product,
            name=name,
            days=days,
            price=price
        )
        session.add(new_plan)
        logging.info(f"Plan object added to session: {new_plan}")
        try:
            session.commit()
            logging.info(f"Plan successfully committed: {new_plan.id}")
        except Exception as e:
            logging.error(f"Error committing new plan: {e}", exc_info=True)
            session.rollback()
            raise

def list_plans(product: str) -> List[dict]:
    with session_scope() as session:
        plans = session.query(Plan).filter_by(product=product).all()
        return [
            {
                "id": p.id,
                "product": p.product,
                "name": p.name,
                "days": p.days,
                "price": p.price,
            }
            for p in plans
        ]

def get_plan(plan_id: int) -> Optional[dict]:
    with session_scope() as session:
        plan = session.get(Plan, plan_id)
        if plan:
            return {
                "id": plan.id,
                "product": plan.product,
                "name": plan.name,
                "days": plan.days,
                "price": plan.price,
            }
        return None

def get_plan_with_product(plan_id: int) -> Optional[dict]:
    with session_scope() as session:
        plan = session.query(Plan).options(joinedload(Plan.product_rel)).filter_by(id=plan_id).first()
        if plan and plan.product_rel:
            return {
                "id": plan.id,
                "product": plan.product_rel.name,
                "name": plan.name,
                "days": plan.days,
                "price": plan.price,
                "product_info": {
                    "name": plan.product_rel.name,
                    "description": plan.product_rel.description,
                    "photo_file_id": plan.product_rel.photo_file_id,
                    "active": plan.product_rel.active,
                }
            }
        return None

def get_user_subscriptions(user_id: int) -> List[dict]:
    with session_scope() as session:
        subscriptions = session.query(Subscription).filter_by(user_id=user_id).all()
        return [
            {
                "id": s.id,
                "user_id": s.user_id,
                "product": s.product,
                "tariff": s.tariff,
                "start_date": s.start_date,
                "end_date": s.end_date,
                "active": s.active,
            }
            for s in subscriptions
        ]

def get_user_payments(user_id: int, limit: int = 20, offset: int = 0) -> List[dict]:
    with session_scope() as session:
        payments = session.query(Payment).filter_by(user_id=user_id).order_by(Payment.created_at.desc()).limit(limit).offset(offset).all()
        return [
            {
                "id": p.id,
                "user_id": p.user_id,
                "product": p.product,
                "tariff": p.tariff,
                "email": p.email,
                "price": p.price,
                "check_file_id": p.check_file_id,
                "status": p.status,
                "created_at": p.created_at,
                "plan_id": p.plan_id,
                "promo_code": p.promo_code,
            }
            for p in payments
        ]

def get_user_withdrawal_requests(user_id: int) -> List[dict]:
    with session_scope() as session:
        requests = session.query(WithdrawalRequest).filter_by(user_id=user_id).order_by(WithdrawalRequest.request_date.desc()).all()
        return [
            {
                "id": r.id,
                "user_id": r.user_id,
                "amount": r.amount,
                "status": r.status,
                "request_date": r.request_date,
                "admin_decision_date": r.admin_decision_date,
                "phone": r.phone,
                "bank": r.bank,
            }
            for r in requests
        ]

def get_settings() -> dict:
    with session_scope() as session:
        settings = session.query(Setting).all()
        return {s.key: s.value for s in settings}

def assign_promo_code_to_payment(payment_id: int, product_name: str) -> Optional[PromoCode]:
    with session_scope() as session:
        promo = session.query(PromoCode).filter_by(product_id=product_name, status="not issued").first()
        if promo:
            payment = session.query(Payment).get(payment_id)
            if payment:
                payment.promo_code = promo.code
                session.commit()
                return promo
        return None

def update_user(user_id: int, **kwargs) -> Optional[dict]:
    with session_scope() as session:
        user = session.query(User).filter_by(id=user_id).first()
        if user:
            for key, value in kwargs.items():
                setattr(user, key, value)
            session.commit()
            return {
                "id": user.id,
                "telegram_id": user.telegram_id,
                "username": user.username,
                "email": user.email,
                "referrer_id": user.referrer_id,
                "referral_balance": user.referral_balance,
                "created_at": user.created_at,
            }
        return None

def add_material(product_name: str, title: str, text: Optional[str] = None, file_id: Optional[str] = None) -> dict:
    with session_scope() as session:
        material = Material(
            product_name=product_name,
            title=title,
            text=text,
            file_id=file_id
        )
        session.add(material)
        session.commit()
        return {"id": material.id, "product_name": material.product_name, "title": material.title, "text": material.text, "file_id": material.file_id}

def get_materials_for_product(product_name: str) -> List[dict]:
    with session_scope() as session:
        materials = session.query(Material).filter_by(product_name=product_name).all()
        return [
            {
                "id": m.id,
                "product_name": m.product_name,
                "title": m.title,
                "text": m.text,
                "file_id": m.file_id,
            }
            for m in materials
        ]

def delete_material(material_id: int) -> bool:
    with session_scope() as session:
        material = session.get(Material, material_id)
        if material:
            session.delete(material)
            session.commit()
            return True
        return False

def get_material(material_id: int) -> Optional[dict]:
    with session_scope() as session:
        material = session.get(Material, material_id)
        if material:
            return {
                "id": material.id,
                "product_name": material.product_name,
                "title": material.title,
                "text": material.text,
                "file_id": material.file_id,
            }
        return None

def delete_plan(plan_id: int) -> bool:
    with session_scope() as session:
        plan = session.get(Plan, plan_id)
        if plan:
            session.delete(plan)
            session.commit()
            return True
        return False

def delete_product(name: str) -> bool:
    with session_scope() as session:
        product = session.query(Product).filter_by(name=name).first()
        if product:
            session.delete(product)
            session.commit()
            return True
        return False

def get_referrals_count(user_id: int) -> int:
    with session_scope() as session:
        return session.query(User).filter_by(referrer_id=user_id).count()

def add_referral_bonus(user_id: int, amount: int):
    with session_scope() as session:
        user = session.get(User, user_id)
        if user:
            user.referral_balance += amount
            session.commit()

def add_withdrawal_request(user_id: int, amount: int, phone: Optional[str] = None, bank: Optional[str] = None) -> dict:
    with session_scope() as session:
        withdrawal_request = WithdrawalRequest(
            user_id=user_id,
            amount=amount,
            status="pending",
            phone=phone,
            bank=bank
        )
        session.add(withdrawal_request)
        session.commit()
        return {
            "id": withdrawal_request.id,
            "user_id": withdrawal_request.user_id,
            "amount": withdrawal_request.amount,
            "status": withdrawal_request.status,
            "request_date": withdrawal_request.request_date,
            "admin_decision_date": withdrawal_request.admin_decision_date,
            "phone": withdrawal_request.phone,
            "bank": withdrawal_request.bank
        }

def get_pending_withdrawal_requests() -> List[dict]:
    with session_scope() as session:
        requests = session.query(WithdrawalRequest).filter_by(status="pending").all()
        return [
            {
                "id": r.id,
                "user_id": r.user_id,
                "amount": r.amount,
                "status": r.status,
                "request_date": r.request_date,
                "admin_decision_date": r.admin_decision_date,
                "phone": r.phone,
                "bank": r.bank,
            }
            for r in requests
        ]

def get_withdrawal_request_by_id(request_id: int) -> Optional[dict]:
    with session_scope() as session:
        request = session.get(WithdrawalRequest, request_id)
        if request:
            return {
                "id": request.id,
                "user_id": request.user_id,
                "amount": request.amount,
                "status": request.status,
                "request_date": request.request_date,
                "admin_decision_date": request.admin_decision_date,
                "phone": request.phone,
                "bank": request.bank,
            }
        return None

def set_withdrawal_request_status(request_id: int, status: str):
    with session_scope() as session:
        request = session.get(WithdrawalRequest, request_id)
        if request:
            request.status = status
            if status == "approved":
                request.admin_decision_date = datetime.utcnow()
            session.commit()
