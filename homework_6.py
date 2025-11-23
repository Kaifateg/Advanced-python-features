from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, \
    mapped_column, Session
from typing import Annotated, List
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
            return (
                f"mssql+pymssql://{self.user}:{self.password}@{self.server}/{self.kwargs['db_name']}")
        if self.sql_type == "PostgresSQL":
            return (
                f"postgresql://{self.user}:{self.password}@{self.server}:{str(self.port)}/{self.kwargs['db_name']}")
        else:
            raise ValueError("Unsupported SQL type")


conn_params = Connection(
    server="localhost",
    port=5432,
    user="postgres",
    password="password",
    db_name="synergy",
    sql_type="PostgresSQL"
).engine
engine = create_engine(conn_params)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class BaseTable(DeclarativeBase):
    type_annotation_map = {
        "IID": Annotated[
            int, mapped_column(primary_key=True, autoincrement=True)]
    }


class Sallers(BaseTable):
    __tablename__ = "Sallers"
    id: Mapped[BaseTable.type_annotation_map['IID']]
    saller_name: Mapped[str]


class SallerBase(BaseModel):
    saller_name: str


class SallerResponse(SallerBase):
    id: int

    class Config:
        from_attributes = True


app = FastAPI()


@app.get("/sallers", response_model=List[SallerResponse])
def get_sellers(db: Session = Depends(get_db)):
    result = db.query(Sallers).all()
    return result


@app.get("/sallers/{id}", response_model=SallerResponse)
def get_seller(id: int, db: Session = Depends(get_db)):
    result = db.query(Sallers).filter(Sallers.id == id).first()
    if result is None:
        raise HTTPException(status_code=404, detail="Seller not found")
    return result


@app.put("/sallers/{id}/update", response_model=SallerResponse)
def update_seller(id: int, updated_data: SallerBase, db: Session = Depends(get_db)):
    seller = db.query(Sallers).filter(Sallers.id == id).first()
    if seller is None:
        raise HTTPException(status_code=404, detail="Seller not found")

    seller.saller_name = updated_data.saller_name
    db.commit()
    db.refresh(seller)
    return seller


def create_test_data(db: Session):
    if db.query(Sallers).count() == 0:
        print("Таблица Sallers пуста. Добавляем тестовые данные.")
        seller1 = Sallers(saller_name="Wildberries Official")
        seller2 = Sallers(saller_name="Ozon Express")
        db.add_all([seller1, seller2])
        db.commit()
    else:
        print("Тестовые данные уже существуют.")


@app.on_event("startup")
def on_startup():
    BaseTable.metadata.create_all(bind=engine)
    print("Database tables ensured.")

    db = SessionLocal()
    try:
        create_test_data(db)
    finally:
        db.close()
