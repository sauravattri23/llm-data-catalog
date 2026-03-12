"""
=============================================================
  LLM Data Catalog — Fake Database Generator
  Creates 12 realistic e-commerce tables with fake data
  using Faker + SQLAlchemy + PostgreSQL
=============================================================
"""

import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    Boolean, DateTime, Text, ForeignKey, BigInteger, Numeric
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DATABASE_URL = "postgresql://catalog_user:catalog_pass@localhost:5432/ecommerce_db"

fake = Faker("en_IN")   # Indian locale — feels like Flipkart/Swiggy data
Base = declarative_base()

# ─────────────────────────────────────────────
#  TABLE 1 — users
# ─────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    user_id       = Column(Integer, primary_key=True, autoincrement=True)
    full_name     = Column(String(100), nullable=False)
    email         = Column(String(150), unique=True, nullable=False)
    phone_number  = Column(String(15))
    city          = Column(String(50))
    state         = Column(String(50))
    pincode       = Column(String(10))
    is_verified   = Column(Boolean, default=False)
    is_premium    = Column(Boolean, default=False)
    created_at    = Column(DateTime, default=datetime.utcnow)
    updated_at    = Column(DateTime, default=datetime.utcnow)

    orders        = relationship("Order", back_populates="user")
    addresses     = relationship("Address", back_populates="user")
    reviews       = relationship("Review", back_populates="user")


# ─────────────────────────────────────────────
#  TABLE 2 — addresses
# ─────────────────────────────────────────────
class Address(Base):
    __tablename__ = "addresses"

    address_id    = Column(Integer, primary_key=True, autoincrement=True)
    user_id       = Column(Integer, ForeignKey("users.user_id"))
    address_line1 = Column(String(200))
    address_line2 = Column(String(200))
    city          = Column(String(50))
    state         = Column(String(50))
    pincode       = Column(String(10))
    address_type  = Column(String(20))   # home / work / other
    is_default    = Column(Boolean, default=False)
    created_at    = Column(DateTime, default=datetime.utcnow)

    user          = relationship("User", back_populates="addresses")


# ─────────────────────────────────────────────
#  TABLE 3 — categories
# ─────────────────────────────────────────────
class Category(Base):
    __tablename__ = "categories"

    category_id   = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(100), nullable=False)
    parent_id     = Column(Integer, ForeignKey("categories.category_id"), nullable=True)
    description   = Column(Text)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    products      = relationship("Product", back_populates="category")


# ─────────────────────────────────────────────
#  TABLE 4 — products
# ─────────────────────────────────────────────
class Product(Base):
    __tablename__ = "products"

    product_id    = Column(Integer, primary_key=True, autoincrement=True)
    category_id   = Column(Integer, ForeignKey("categories.category_id"))
    product_name  = Column(String(200), nullable=False)
    brand         = Column(String(100))
    description   = Column(Text)
    price         = Column(Numeric(10, 2), nullable=False)
    mrp           = Column(Numeric(10, 2))
    stock_qty     = Column(Integer, default=0)
    sku           = Column(String(50), unique=True)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    category      = relationship("Category", back_populates="products")
    order_items   = relationship("OrderItem", back_populates="product")
    reviews       = relationship("Review", back_populates="product")
    inventory     = relationship("Inventory", back_populates="product")


# ─────────────────────────────────────────────
#  TABLE 5 — orders
# ─────────────────────────────────────────────
class Order(Base):
    __tablename__ = "orders"

    order_id       = Column(Integer, primary_key=True, autoincrement=True)
    user_id        = Column(Integer, ForeignKey("users.user_id"))
    order_status   = Column(String(30))   # pending/confirmed/shipped/delivered/cancelled
    total_amount   = Column(Numeric(10, 2))
    discount_amt   = Column(Numeric(10, 2), default=0)
    tax_amount     = Column(Numeric(10, 2), default=0)
    payment_status = Column(String(20))   # paid/pending/failed/refunded
    shipping_addr  = Column(Text)
    ordered_at     = Column(DateTime, default=datetime.utcnow)
    delivered_at   = Column(DateTime, nullable=True)

    user           = relationship("User", back_populates="orders")
    order_items    = relationship("OrderItem", back_populates="order")
    payments       = relationship("Payment", back_populates="order")
    shipments      = relationship("Shipment", back_populates="order")


# ─────────────────────────────────────────────
#  TABLE 6 — order_items
# ─────────────────────────────────────────────
class OrderItem(Base):
    __tablename__ = "order_items"

    item_id       = Column(Integer, primary_key=True, autoincrement=True)
    order_id      = Column(Integer, ForeignKey("orders.order_id"))
    product_id    = Column(Integer, ForeignKey("products.product_id"))
    quantity      = Column(Integer, nullable=False)
    unit_price    = Column(Numeric(10, 2))
    discount_pct  = Column(Float, default=0)
    total_price   = Column(Numeric(10, 2))
    created_at    = Column(DateTime, default=datetime.utcnow)

    order         = relationship("Order", back_populates="order_items")
    product       = relationship("Product", back_populates="order_items")


# ─────────────────────────────────────────────
#  TABLE 7 — payments
# ─────────────────────────────────────────────
class Payment(Base):
    __tablename__ = "payments"

    payment_id     = Column(Integer, primary_key=True, autoincrement=True)
    order_id       = Column(Integer, ForeignKey("orders.order_id"))
    payment_method = Column(String(30))   # UPI/card/netbanking/COD/wallet
    payment_status = Column(String(20))   # success/failed/pending/refunded
    amount         = Column(Numeric(10, 2))
    transaction_id = Column(String(100), unique=True)
    gateway        = Column(String(50))   # Razorpay/Paytm/PhonePe
    paid_at        = Column(DateTime, nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)

    order          = relationship("Order", back_populates="payments")


# ─────────────────────────────────────────────
#  TABLE 8 — shipments
# ─────────────────────────────────────────────
class Shipment(Base):
    __tablename__ = "shipments"

    shipment_id    = Column(Integer, primary_key=True, autoincrement=True)
    order_id       = Column(Integer, ForeignKey("orders.order_id"))
    carrier        = Column(String(50))   # Delhivery/BlueDart/Ecom Express
    tracking_no    = Column(String(100))
    status         = Column(String(30))   # packed/in_transit/out_for_delivery/delivered
    estimated_date = Column(DateTime)
    delivered_date = Column(DateTime, nullable=True)
    created_at     = Column(DateTime, default=datetime.utcnow)

    order          = relationship("Order", back_populates="shipments")


# ─────────────────────────────────────────────
#  TABLE 9 — reviews
# ─────────────────────────────────────────────
class Review(Base):
    __tablename__ = "reviews"

    review_id     = Column(Integer, primary_key=True, autoincrement=True)
    product_id    = Column(Integer, ForeignKey("products.product_id"))
    user_id       = Column(Integer, ForeignKey("users.user_id"))
    rating        = Column(Integer)    # 1–5
    title         = Column(String(200))
    review_text   = Column(Text)
    is_verified   = Column(Boolean, default=False)
    helpful_votes = Column(Integer, default=0)
    created_at    = Column(DateTime, default=datetime.utcnow)

    product       = relationship("Product", back_populates="reviews")
    user          = relationship("User", back_populates="reviews")


# ─────────────────────────────────────────────
#  TABLE 10 — inventory
# ─────────────────────────────────────────────
class Inventory(Base):
    __tablename__ = "inventory"

    inventory_id  = Column(Integer, primary_key=True, autoincrement=True)
    product_id    = Column(Integer, ForeignKey("products.product_id"))
    warehouse     = Column(String(100))
    qty_available = Column(Integer, default=0)
    qty_reserved  = Column(Integer, default=0)
    qty_damaged   = Column(Integer, default=0)
    reorder_level = Column(Integer, default=10)
    last_restocked= Column(DateTime)
    updated_at    = Column(DateTime, default=datetime.utcnow)

    product       = relationship("Product", back_populates="inventory")


# ─────────────────────────────────────────────
#  TABLE 11 — coupons
# ─────────────────────────────────────────────
class Coupon(Base):
    __tablename__ = "coupons"

    coupon_id     = Column(Integer, primary_key=True, autoincrement=True)
    coupon_code   = Column(String(30), unique=True, nullable=False)
    discount_type = Column(String(20))   # percentage / flat
    discount_value= Column(Float)
    min_order_amt = Column(Numeric(10, 2))
    max_uses      = Column(Integer)
    used_count    = Column(Integer, default=0)
    valid_from    = Column(DateTime)
    valid_until   = Column(DateTime)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime, default=datetime.utcnow)


# ─────────────────────────────────────────────
#  TABLE 12 — user_events (clickstream)
# ─────────────────────────────────────────────
class UserEvent(Base):
    __tablename__ = "user_events"

    event_id      = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id       = Column(Integer, ForeignKey("users.user_id"))
    session_id    = Column(String(100))
    event_type    = Column(String(50))   # page_view/add_to_cart/purchase/search
    page_url      = Column(String(300))
    product_id    = Column(Integer, nullable=True)
    device_type   = Column(String(20))   # mobile/desktop/tablet
    os            = Column(String(30))
    city          = Column(String(50))
    created_at    = Column(DateTime, default=datetime.utcnow)


# ─────────────────────────────────────────────
#  DATA SEEDING FUNCTIONS
# ─────────────────────────────────────────────

def seed_categories(session):
    print("🌱 Seeding categories...")
    parent_cats = ["Electronics", "Fashion", "Home & Kitchen", "Sports", "Books"]
    child_cats  = {
        "Electronics": ["Mobiles", "Laptops", "Headphones", "Cameras"],
        "Fashion":     ["Men", "Women", "Kids", "Accessories"],
        "Home & Kitchen": ["Furniture", "Cookware", "Decor"],
        "Sports":      ["Cricket", "Football", "Fitness"],
        "Books":       ["Fiction", "Non-Fiction", "Academic"],
    }
    cat_map = {}
    for name in parent_cats:
        c = Category(category_name=name, description=fake.sentence())
        session.add(c)
        session.flush()
        cat_map[name] = c.category_id
        for child in child_cats[name]:
            cc = Category(category_name=child, parent_id=c.category_id, description=fake.sentence())
            session.add(cc)
    session.commit()
    print(f"   ✅ {len(parent_cats) + sum(len(v) for v in child_cats.values())} categories created")
    return cat_map


def seed_users(session, count=500):
    print(f"🌱 Seeding {count} users...")
    users = []
    emails = set()
    for _ in range(count):
        email = fake.unique.email()
        while email in emails:
            email = fake.unique.email()
        emails.add(email)
        u = User(
            full_name    = fake.name(),
            email        = email,
            phone_number = fake.phone_number()[:15],
            city         = fake.city(),
            state        = fake.state(),
            pincode      = fake.postcode(),
            is_verified  = random.choice([True, True, False]),
            is_premium   = random.choice([True, False, False, False]),
            created_at   = fake.date_time_between(start_date="-2y", end_date="now"),
        )
        users.append(u)
    session.bulk_save_objects(users)
    session.commit()
    print(f"   ✅ {count} users created")


def seed_addresses(session):
    print("🌱 Seeding addresses...")
    user_ids = [r[0] for r in session.query(User.user_id).all()]
    addresses = []
    for uid in user_ids:
        for _ in range(random.randint(1, 3)):
            a = Address(
                user_id       = uid,
                address_line1 = fake.street_address(),
                address_line2 = fake.building_number(),
                city          = fake.city(),
                state         = fake.state(),
                pincode       = fake.postcode(),
                address_type  = random.choice(["home", "work", "other"]),
                is_default    = random.choice([True, False]),
            )
            addresses.append(a)
    session.bulk_save_objects(addresses)
    session.commit()
    print(f"   ✅ {len(addresses)} addresses created")


def seed_products(session, count=200):
    print(f"🌱 Seeding {count} products...")
    cat_ids = [r[0] for r in session.query(Category.category_id).all()]
    brands  = ["Samsung", "Apple", "Nike", "Adidas", "Sony", "LG", "Boat", "Noise",
               "Puma", "H&M", "Zara", "Prestige", "Philips", "Lenovo", "HP"]
    products = []
    for i in range(count):
        mrp   = round(random.uniform(299, 99999), 2)
        price = round(mrp * random.uniform(0.6, 0.95), 2)
        p = Product(
            category_id  = random.choice(cat_ids),
            product_name = fake.catch_phrase(),
            brand        = random.choice(brands),
            description  = fake.paragraph(nb_sentences=3),
            price        = price,
            mrp          = mrp,
            stock_qty    = random.randint(0, 500),
            sku          = f"SKU{str(i+1).zfill(6)}",
            is_active    = random.choice([True, True, True, False]),
            created_at   = fake.date_time_between(start_date="-1y", end_date="now"),
        )
        products.append(p)
    session.bulk_save_objects(products)
    session.commit()
    print(f"   ✅ {count} products created")


def seed_orders(session, count=1000):
    print(f"🌱 Seeding {count} orders...")
    user_ids    = [r[0] for r in session.query(User.user_id).all()]
    product_ids = [r[0] for r in session.query(Product.product_id).all()]
    statuses    = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    carriers    = ["Delhivery", "BlueDart", "Ecom Express", "DTDC", "XpressBees"]
    gateways    = ["Razorpay", "Paytm", "PhonePe", "Cashfree"]
    pay_methods = ["UPI", "credit_card", "debit_card", "netbanking", "COD", "wallet"]

    for _ in range(count):
        order_date  = fake.date_time_between(start_date="-1y", end_date="now")
        status      = random.choice(statuses)
        total       = round(random.uniform(199, 49999), 2)
        paid        = status not in ["pending", "cancelled"]

        order = Order(
            user_id        = random.choice(user_ids),
            order_status   = status,
            total_amount   = total,
            discount_amt   = round(total * random.uniform(0, 0.3), 2),
            tax_amount     = round(total * 0.18, 2),
            payment_status = "paid" if paid else "pending",
            shipping_addr  = fake.address(),
            ordered_at     = order_date,
            delivered_at   = order_date + timedelta(days=random.randint(2, 7)) if status == "delivered" else None,
        )
        session.add(order)
        session.flush()

        # Order items (1–5 per order)
        for _ in range(random.randint(1, 5)):
            qty        = random.randint(1, 3)
            unit_price = round(random.uniform(99, 9999), 2)
            item = OrderItem(
                order_id   = order.order_id,
                product_id = random.choice(product_ids),
                quantity   = qty,
                unit_price = unit_price,
                discount_pct = random.uniform(0, 30),
                total_price  = round(unit_price * qty, 2),
                created_at   = order_date,
            )
            session.add(item)

        # Payment
        payment = Payment(
            order_id       = order.order_id,
            payment_method = random.choice(pay_methods),
            payment_status = "success" if paid else "pending",
            amount         = total,
            transaction_id = fake.uuid4(),
            gateway        = random.choice(gateways),
            paid_at        = order_date if paid else None,
            created_at     = order_date,
        )
        session.add(payment)

        # Shipment (only for non-pending/cancelled)
        if status not in ["pending", "cancelled"]:
            ship_status = {"confirmed": "packed", "shipped": "in_transit", "delivered": "delivered"}.get(status, "packed")
            shipment = Shipment(
                order_id       = order.order_id,
                carrier        = random.choice(carriers),
                tracking_no    = fake.bothify(text="??########"),
                status         = ship_status,
                estimated_date = order_date + timedelta(days=5),
                delivered_date = order_date + timedelta(days=random.randint(2,7)) if status == "delivered" else None,
                created_at     = order_date,
            )
            session.add(shipment)

    session.commit()
    print(f"   ✅ {count} orders + items + payments + shipments created")


def seed_reviews(session, count=800):
    print(f"🌱 Seeding {count} reviews...")
    user_ids    = [r[0] for r in session.query(User.user_id).all()]
    product_ids = [r[0] for r in session.query(Product.product_id).all()]
    reviews = []
    for _ in range(count):
        r = Review(
            product_id    = random.choice(product_ids),
            user_id       = random.choice(user_ids),
            rating        = random.choices([1,2,3,4,5], weights=[5,5,15,35,40])[0],
            title         = fake.sentence(nb_words=6),
            review_text   = fake.paragraph(nb_sentences=2),
            is_verified   = random.choice([True, True, False]),
            helpful_votes = random.randint(0, 200),
            created_at    = fake.date_time_between(start_date="-1y", end_date="now"),
        )
        reviews.append(r)
    session.bulk_save_objects(reviews)
    session.commit()
    print(f"   ✅ {count} reviews created")


def seed_inventory(session):
    print("🌱 Seeding inventory...")
    product_ids = [r[0] for r in session.query(Product.product_id).all()]
    warehouses  = ["Mumbai WH1", "Delhi WH2", "Bangalore WH3", "Chennai WH4"]
    records     = []
    for pid in product_ids:
        for wh in random.sample(warehouses, random.randint(1, 3)):
            inv = Inventory(
                product_id    = pid,
                warehouse     = wh,
                qty_available = random.randint(0, 300),
                qty_reserved  = random.randint(0, 50),
                qty_damaged   = random.randint(0, 10),
                reorder_level = random.randint(5, 30),
                last_restocked= fake.date_time_between(start_date="-3m", end_date="now"),
            )
            records.append(inv)
    session.bulk_save_objects(records)
    session.commit()
    print(f"   ✅ {len(records)} inventory records created")


def seed_coupons(session, count=50):
    print(f"🌱 Seeding {count} coupons...")
    coupons = []
    for i in range(count):
        valid_from = fake.date_time_between(start_date="-6m", end_date="now")
        c = Coupon(
            coupon_code    = f"DEAL{str(i+1).zfill(4)}",
            discount_type  = random.choice(["percentage", "flat"]),
            discount_value = random.choice([5,10,15,20,25,50,100,200,500]),
            min_order_amt  = random.choice([199, 299, 499, 999, 1999]),
            max_uses       = random.randint(100, 10000),
            used_count     = random.randint(0, 500),
            valid_from     = valid_from,
            valid_until    = valid_from + timedelta(days=random.randint(7, 90)),
            is_active      = random.choice([True, True, False]),
        )
        coupons.append(c)
    session.bulk_save_objects(coupons)
    session.commit()
    print(f"   ✅ {count} coupons created")


def seed_user_events(session, count=5000):
    print(f"🌱 Seeding {count} user events...")
    user_ids    = [r[0] for r in session.query(User.user_id).all()]
    product_ids = [r[0] for r in session.query(Product.product_id).all()]
    event_types = ["page_view", "search", "add_to_cart", "remove_from_cart",
                   "wishlist_add", "purchase", "product_view", "checkout_start"]
    devices     = ["mobile", "desktop", "tablet"]
    os_list     = ["Android", "iOS", "Windows", "macOS"]

    events = []
    for _ in range(count):
        etype = random.choice(event_types)
        e = UserEvent(
            user_id    = random.choice(user_ids),
            session_id = fake.uuid4(),
            event_type = etype,
            page_url   = fake.uri(),
            product_id = random.choice(product_ids) if etype in ["product_view", "add_to_cart", "purchase"] else None,
            device_type= random.choice(devices),
            os         = random.choice(os_list),
            city       = fake.city(),
            created_at = fake.date_time_between(start_date="-3m", end_date="now"),
        )
        events.append(e)
    session.bulk_save_objects(events)
    session.commit()
    print(f"   ✅ {count} user events created")


# ─────────────────────────────────────────────
#  MAIN — Run Everything
# ─────────────────────────────────────────────
def main():
    print("\n" + "="*55)
    print("  🚀 LLM Data Catalog — Fake DB Generator")
    print("="*55 + "\n")

    engine  = create_engine(DATABASE_URL, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("📦 Creating all tables...")
    Base.metadata.create_all(engine)
    print("   ✅ 12 tables created\n")

    print("🌱 Seeding data into all tables...\n")
    seed_categories(session)
    seed_users(session, count=500)
    seed_addresses(session)
    seed_products(session, count=200)
    seed_orders(session, count=1000)
    seed_reviews(session, count=800)
    seed_inventory(session)
    seed_coupons(session, count=50)
    seed_user_events(session, count=5000)

    print("\n" + "="*55)
    print("  ✅ DATABASE SEEDING COMPLETE!")
    print("="*55)
    print("\n📊 Summary:")
    print(f"   Users:        {session.query(User).count():,}")
    print(f"   Products:     {session.query(Product).count():,}")
    print(f"   Orders:       {session.query(Order).count():,}")
    print(f"   Order Items:  {session.query(OrderItem).count():,}")
    print(f"   Payments:     {session.query(Payment).count():,}")
    print(f"   Shipments:    {session.query(Shipment).count():,}")
    print(f"   Reviews:      {session.query(Review).count():,}")
    print(f"   Inventory:    {session.query(Inventory).count():,}")
    print(f"   Coupons:      {session.query(Coupon).count():,}")
    print(f"   User Events:  {session.query(UserEvent).count():,}")
    print(f"   Categories:   {session.query(Category).count():,}")
    print(f"   Addresses:    {session.query(Address).count():,}")
    print("\n🎯 Your fake e-commerce database is ready to catalog!\n")
    session.close()


if __name__ == "__main__":
    main()