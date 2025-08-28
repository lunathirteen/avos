from sqlalchemy import Column, String, Float, DateTime, Integer, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
from enum import Enum
import json

Base = declarative_base()


class ExperimentStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"


class Experiment(Base):
    """Configuration for a single experiment.

    :param experiment_id: Unique experiment identifier
    :param name: Human-readable experiment name
    :param variants: List of variants (e.g., ['control', 'treatment'])
    :param traffic_allocation: Percent of traffic for each variant (must sum to 100)
    :param traffic_percentage: Share of layer traffic this experiment should receive
    :param start_date: Optional start date for the experiment
    :param end_date: Optional end date for the experiment
    :param target_audience: Optional targeting criteria
    """

    __tablename__ = "experiments"

    experiment_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    variants = Column(String, nullable=False)  # JSON string
    traffic_allocation = Column(String, nullable=False)  # JSON string
    traffic_percentage = Column(Float, default=100.0)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    target_audience = Column(String, nullable=False)  # JSON string
    status = Column(SQLEnum(ExperimentStatus), default=ExperimentStatus.DRAFT)
    priority = Column(Integer, default=0)
    layer_id = Column(String, ForeignKey("layers.layer_id"), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    layer = relationship("Layer", back_populates="experiments")

    def __init__(
        self,
        experiment_id,
        layer_id,
        name,
        variants,
        traffic_allocation,
        traffic_percentage=100.0,
        start_date=None,
        end_date=None,
        target_audience={},
        status=ExperimentStatus.DRAFT,
        priority=0,
    ):
        self.experiment_id = experiment_id
        self.layer_id = layer_id
        self.name = name
        self.variants = json.dumps(variants)
        self.traffic_allocation = json.dumps(traffic_allocation)
        self.traffic_percentage = traffic_percentage
        self.start_date = start_date
        self.end_date = end_date
        self.target_audience = json.dumps(target_audience)
        self.status = status
        self.priority = priority

        # Validation logic
        ta = json.loads(self.traffic_allocation)
        if any(p <= 0 for p in ta.values()):
            raise ValueError("All traffic_allocation values must be greater than 0")
        if abs(sum(ta.values()) - 100.0) > 0.01:
            raise ValueError("Sum of traffic_allocation must be 100%")
        if self.traffic_percentage <= 0 or self.traffic_percentage > 100:
            raise ValueError("traffic_percentage must be in (0, 100]")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")

    def is_active(self, current_time=None):
        if self.status != ExperimentStatus.ACTIVE:
            return False
        current_time = current_time or datetime.now()
        if self.start_date and current_time < self.start_date:
            return False
        if self.end_date and current_time > self.end_date:
            return False
        return True

    def variant_list(self):
        return json.loads(self.variants)

    def traffic_dict(self):
        return json.loads(self.traffic_allocation)

    def target_audience_dict(self):
        return json.loads(self.target_audience)
