import datetime
import random
from faker import Faker
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from typing import Annotated


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

    def __init__(self, **kwargs):
        columns = self.__table__.c.keys()
        for key, value in kwargs.items():
            if key in columns: setattr(self, key, value)


class Orders(BaseTable):
    __tablename__ = "Orders"

    ID: Mapped[BaseTable.type_annotation_map['IID']]
    OrderName: Mapped[str]
    Price: Mapped[int]
    CreatedOn: Mapped[BaseTable.type_annotation_map['CreatedOn']]
    UpdatedAt: Mapped[BaseTable.type_annotation_map['UpdatedAt']]


class Suppliers(BaseTable):
    __tablename__ = "Suppliers"

    ID: Mapped[BaseTable.type_annotation_map['IID']]
    SupplierName: Mapped[str]
    CreatedOn: Mapped[BaseTable.type_annotation_map['CreatedOn']]
    UpdatedAt: Mapped[BaseTable.type_annotation_map['UpdatedAt']]


class Goods(BaseTable):
    __tablename__ = "Goods"

    ID: Mapped[BaseTable.type_annotation_map['IID']]
    GoodsName: Mapped[str]
    Price: Mapped[int]
    CreatedOn: Mapped[BaseTable.type_annotation_map['CreatedOn']]
    UpdatedAt: Mapped[BaseTable.type_annotation_map['UpdatedAt']]


def seed_database(session, num_entries=20):
    fake = Faker('ru_RU')
    print(f"Add {num_entries} random strings...")
    for _ in range(num_entries):
        session.add(Suppliers(SupplierName=fake.company()))
        session.add(Goods(GoodsName=fake.bs(), Price=random.randint(100, 5000)))
        session.add(Orders(OrderName=f"Order #{random.randint(1000, 9999)}",
                           Price=random.randint(50, 10000)))
    session.commit()
    print("Data added successfully.")


def display_data(session, table_class):
    print(f"\n--- Table: '{table_class.__tablename__}' ---")
    for item in session.query(table_class).all():
        print(item.__dict__)
    print("-" * 20)


try:
    conn_params = Connection(
            server="localhost",
            port=5432,
            user="postgres",
            password="password",
            db_name="synergy",
            sql_type="PostgresSQL"
    )
    session_builder = SessionBuilder(conn_params)
    engine = session_builder.engine

    BaseTable.metadata.create_all(engine)

    db_session = session_builder.build()
    seed_database(db_session, num_entries=10)
    display_data(db_session, Suppliers)
    display_data(db_session, Goods)
    display_data(db_session, Orders)
    db_session.close()
except ValueError as e:
    print(f"Value error: {e}")
except Exception as e:
    print(f"Error: {e}")
