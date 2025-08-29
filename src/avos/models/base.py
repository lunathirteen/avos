from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


# All ORM classes inherit from this Base
class Base(MappedAsDataclass, DeclarativeBase):
    pass
