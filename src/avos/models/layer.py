from sqlalchemy import Column, String, Float, Integer, DateTime
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
from avos.models.experiment import Base

class Layer(Base):
    __tablename__ = "layers"

    layer_id = Column(String, primary_key=True)
    layer_salt = Column(String, nullable=False)
    total_slots = Column(Integer, default=100)
    total_traffic_percentage = Column(Float, default=100.0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship: One Layer -> Many Experiments
    experiments = relationship("Experiment", back_populates="layer")
