from __future__ import annotations
import json
from datetime import datetime
from enum import Enum
from typing import List, Dict, Any

from sqlalchemy import Column, String, Float, DateTime, Integer, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from avos.models.base import Base


class ExperimentStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"


class Experiment(Base):
    __tablename__ = "experiments"

    experiment_id: Mapped[str] = mapped_column(String, primary_key=True)
    layer_id: Mapped[str] = mapped_column(
        String, ForeignKey("layers.layer_id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    variants: Mapped[str] = mapped_column(String, nullable=False)           # JSON
    traffic_allocation: Mapped[str] = mapped_column(String, nullable=False) # JSON
    start_date: Mapped[datetime | None] = mapped_column(DateTime)
    end_date: Mapped[datetime | None] = mapped_column(DateTime)

    traffic_percentage: Mapped[float] = mapped_column(Float, default=100.0)
    status: Mapped[ExperimentStatus] = mapped_column(
        SQLEnum(ExperimentStatus), default=ExperimentStatus.DRAFT
    )
    priority: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default_factory=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default_factory=datetime.now, onupdate=datetime.now
    )
    # Relationship back to Layer
    layer: Mapped["Layer"] = relationship("Layer", back_populates="experiments", default=None)


    # ---------------- helper accessors ----------------
    def variant_list(self) -> List[str]:
        return json.loads(self.variants)

    def traffic_dict(self) -> Dict[str, float]:
        return json.loads(self.traffic_allocation)

    def is_active(self, now: datetime | None = None) -> bool:
        if self.status != ExperimentStatus.ACTIVE:
            return False
        now = now or datetime.now()
        if self.start_date and now < self.start_date:
            return False
        if self.end_date and now > self.end_date:
            return False
        return True
