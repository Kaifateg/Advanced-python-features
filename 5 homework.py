import datetime
import asyncio
from sqlalchemy import create_engine, text, func, ForeignKey, String
from sqlalchemy.orm import (sessionmaker, DeclarativeBase, Mapped,
                            mapped_column, relationship)
from typing import Annotated, List
from parsing import load_and_transform_data
from pydantic import BaseModel


class Connection:

    def __init__(self, sql_type, user, password, server, port=None, **kwargs):
        self.user = user
        self.password = password
        self.server = server
        self.port = port
        self.sql_type = sql_type
        self.kwargs = kwargs

    @property
    def engine(self):
        if self.sql_type == "MSSQL":
            return (f"mssql+pymssql://{self.user}:{self.password}@"
                    f"{self.server}/{self.kwargs['db_name']}")
        if self.sql_type == "PostgresSQL":
            return (f"postgresql://{self.user}:{self.password}@"
                    f"{self.server}:{str(self.port)}/{self.kwargs['db_name']}")
        else:
            raise ValueError("Unsupported SQL type")


class SessionBuilder:

    def __init__(self, connection: Connection):
        self.engine = create_engine(connection.engine)

    def build(self):
        Session = sessionmaker(bind=self.engine)
        return Session()


class BaseTable(DeclarativeBase):

    __abstract__ = True

    type_annotation_map = {
        "IID": Annotated[int, mapped_column(primary_key=True,
                                            autoincrement=True)],
        "CreatedOn": Annotated[datetime.datetime, mapped_column(
            server_default=text("now()"))],
        "UpdatedAt": Annotated[datetime.datetime, mapped_column(
            server_default=text("now()"), onupdate=func.now())]
    }


class Orders(BaseTable):
    __tablename__ = "Orders"

    ID: Mapped[BaseTable.type_annotation_map['IID']]
    OrderName: Mapped[str]
    Price: Mapped[int]
    CreatedOn: Mapped[BaseTable.type_annotation_map['CreatedOn']]
    UpdatedAt: Mapped[BaseTable.type_annotation_map['UpdatedAt']]
    total_orders_count: Mapped[int] = mapped_column(comment="Кол-во заказов всего")
    turnover_fbo: Mapped[int] = mapped_column(comment="Оборот FBO")
    turnover_fbs: Mapped[int] = mapped_column(comment="Оборот FBS")
    missed_revenue: Mapped[int] = mapped_column(comment="Упущенная выгода")
    feedback_count: Mapped[int] = mapped_column(comment="Отзывов")
    goods: Mapped[List["Goods"]] = relationship(back_populates="order")


class Suppliers(BaseTable):
    __tablename__ = "Suppliers"

    ID: Mapped[BaseTable.type_annotation_map['IID']]
    SupplierName: Mapped[str]
    CreatedOn: Mapped[BaseTable.type_annotation_map['CreatedOn']]
    UpdatedAt: Mapped[BaseTable.type_annotation_map['UpdatedAt']]
    goods: Mapped[List["Goods"]] = relationship(back_populates="supplier")


class SupplierODT(BaseModel):
    id: int
    supplier_name: str
    created_on: datetime.datetime
    updated_at: datetime.datetime


class Goods(BaseTable):
    __tablename__ = "Goods"

    ID: Mapped[BaseTable.type_annotation_map["IID"]]
    GoodsName: Mapped[str]
    Price: Mapped[int]
    CreatedOn: Mapped[BaseTable.type_annotation_map["CreatedOn"]]
    UpdatedAt: Mapped[BaseTable.type_annotation_map["UpdatedAt"]]
    supplier_id: Mapped[int] = mapped_column(ForeignKey("Suppliers.ID"))
    order_id: Mapped[int] = mapped_column(ForeignKey("Orders.ID"))
    sku: Mapped[str] = mapped_column(String(50), comment="SKU артикула")
    brand: Mapped[str] = mapped_column(comment="Бренд")
    main_category: Mapped[str] = mapped_column(comment="Основная категория")
    days_on_sale: Mapped[int] = mapped_column(comment="Кол-во дней когда артикул был в продаже")
    days_with_purchases: Mapped[int] = mapped_column(comment="Кол-во дней, когда артикул покупали")
    last_stock_balance: Mapped[int] = mapped_column(comment="Последние остатки на складах")
    missed_revenue_percent: Mapped[float] = mapped_column(comment="Упущенная выгода в процентах")
    search_queries: Mapped[int] = mapped_column(comment="Поисковых запросов")
    supplier: Mapped[Suppliers] = relationship(back_populates="goods")
    order: Mapped[Orders] = relationship(back_populates="goods")


def populate_db_from_loader(session, data):
    print(
        f"\nНачало сохранения {len(data)} записей в БД с использованием всех доступных столбцов...")

    suppliers_cache = {}

    for item_dict in data:
        try:
            seller_name = item_dict['Продавец']
            item_name = item_dict['Название']
            price = int(item_dict['Цена'])
            sku_val = str(item_dict['SKU'])
            brand_val = item_dict['Бренд']
            category_val = item_dict['Основная категория']
            days_sale = int(
                item_dict['Кол-во дней когда артикул был в продаже'])
            days_purchase = int(
                item_dict['Кол-во дней, когда артикул покупали'])
            orders_count = int(item_dict['Кол-во заказов'])
            turnover_fbo_val = int(item_dict['Оборот FBO'])
            turnover_fbs_val = int(item_dict['Оборот FBS'])
            missed_rev_val = int(item_dict['Упущенная выгода'])
            last_stock = int(item_dict['Последние остатки на складах'])
            missed_rev_pct = float(item_dict['Упущенная выгода в процентах'])
            feedback_count_val = int(item_dict['Отзывов'])
            search_queries_val = int(item_dict['Поисковых запросов'])

        except KeyError as e:
            print(
                f"Пропущена запись из-за отсутствия критически важного столбца: {e}")
            continue
        except ValueError as e:
            print(f"Пропущена запись из-за ошибки формата данных: {e}")
            continue

        if seller_name not in suppliers_cache:
            supplier = Suppliers(SupplierName=seller_name)
            session.add(supplier)
            session.flush()
            suppliers_cache[seller_name] = supplier
        else:
            supplier = suppliers_cache[seller_name]

        order_name = f"Order SKU: {sku_val}"
        order = Orders(
            OrderName=order_name,
            Price=price,
            total_orders_count=orders_count,
            turnover_fbo=turnover_fbo_val,
            turnover_fbs=turnover_fbs_val,
            missed_revenue=missed_rev_val,
            feedback_count=feedback_count_val
        )
        session.add(order)
        session.flush()

        good = Goods(
            GoodsName=item_name,
            Price=price,
            supplier_id=supplier.ID,
            order_id=order.ID,
            sku=sku_val,
            brand=brand_val,
            main_category=category_val,
            days_on_sale=days_sale,
            days_with_purchases=days_purchase,
            last_stock_balance=last_stock,
            missed_revenue_percent=missed_rev_pct,
            search_queries=search_queries_val
        )
        session.add(good)

    session.commit()
    print("Сохранение в БД завершено.")


def display_data(session, table_class):
    print(f"\n--- Table: '{table_class.__tablename__}' ---")
    for item in session.query(table_class).all():
        print(item.__dict__)
    print("-" * 20)


def display_suppliers_as_odt(session):
    print(f"\n--- Таблица: 'Suppliers' (через ODT) ---")
    suppliers = session.query(Suppliers).all()
    supplier_odts = [SupplierODT.model_validate(s) for s in suppliers]

    for dto in supplier_odts:
        print(f"ID: {dto.id}, Название: {dto.supplier_name}")
    print("-" * 20)


async def main_process():
    categories_to_fetch = list(range(1, 20))
    skip_value_start = 0
    transformed_data = await load_and_transform_data(categories_to_fetch,
                                                     skip_value_start)
    conn_params = Connection(
        server="localhost",
        port=5432,
        user="postgres",
        password="password",
        db_name="synergy",
        sql_type="PostgresSQL"
    )
    try:
        session_builder = SessionBuilder(conn_params)
        engine = session_builder.engine

        BaseTable.metadata.create_all(engine)

        db_session = session_builder.build()
        if transformed_data:
            populate_db_from_loader(db_session, transformed_data)

        display_data(db_session, Orders)
        display_suppliers_as_odt(db_session)

        db_session.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main_process())