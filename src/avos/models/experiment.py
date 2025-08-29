from __future__ import annotations
import json
from datetime import datetime, UTC
from enum import Enum
from typing import List, Dict

from sqlalchemy import String, Float, DateTime, Integer, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from avos.models.base import Base


class ExperimentStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"


class Experiment(Base):
    __tablename__ = "experiments"

    # Non-default fields first (dataclass ordering rule)
    experiment_id: Mapped[str] = mapped_column(String, primary_key=True)
    layer_id: Mapped[str] = mapped_column(String, ForeignKey("layers.layer_id"), nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    variants: Mapped[str] = mapped_column(String, nullable=False)
    traffic_allocation: Mapped[str] = mapped_column(String, nullable=False)

    # Optional fields
    start_date: Mapped[datetime | None] = mapped_column(DateTime)
    end_date: Mapped[datetime | None] = mapped_column(DateTime)

    # Fields with defaults
    traffic_percentage: Mapped[float] = mapped_column(Float, default=100.0)
    status: Mapped[ExperimentStatus] = mapped_column(SQLEnum(ExperimentStatus), default=ExperimentStatus.DRAFT)
    priority: Mapped[int] = mapped_column(Integer, default=0)

    # Consistent UTC timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default_factory=lambda: datetime.now(UTC), onupdate=datetime.now(UTC)
    )

    # Relationships last
    layer: Mapped["Layer"] = relationship("Layer", back_populates="experiments", init=False)

    # Simplified constructor - only handle JSON serialization
    def __init__(
        self,
        *,
        variants: List[str],
        traffic_allocation: Dict[str, float],
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        **kw,
    ):
        kw["variants"] = json.dumps(variants)
        kw["traffic_allocation"] = json.dumps(traffic_allocation)

        # Explicitly set timestamps if not provided
        kw["created_at"] = created_at or datetime.now(UTC)
        kw["updated_at"] = updated_at or datetime.now(UTC)

        super().__init__(**kw)

    # Helper methods
    def get_variant_list(self) -> List[str]:
        return json.loads(self.variants)

    def get_traffic_dict(self) -> Dict[str, float]:
        return json.loads(self.traffic_allocation)

    def is_active(self, now: datetime | None = None) -> bool:
        if self.status != ExperimentStatus.ACTIVE:
            return False
        now = now or datetime.now(UTC)
        if self.start_date and now < self.start_date:
            return False
        if self.end_date and now > self.end_date:
            return False
        return True
