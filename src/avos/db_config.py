from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from avos.models.base import Base


def get_session(db_url: str = "sqlite:///app.db"):
    engine = create_engine(db_url, echo=False, future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()
