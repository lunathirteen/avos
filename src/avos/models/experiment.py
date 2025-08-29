from __future__ import annotations
import json
from datetime import datetime
from enum import Enum
from typing import List, Dict

from sqlalchemy import String, Float, DateTime, Integer, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from avos.models.base import Base
from avos.utils.datetime_utils import to_utc, utc_now, UTC


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
    start_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Fields with defaults
    traffic_percentage: Mapped[float] = mapped_column(Float, default=100.0)
    status: Mapped[ExperimentStatus] = mapped_column(SQLEnum(ExperimentStatus), default=ExperimentStatus.DRAFT)
    priority: Mapped[int] = mapped_column(Integer, default=0)

    # Consistent UTC timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=utc_now, onupdate=utc_now)

    # Relationships last
    layer: Mapped["Layer"] = relationship("Layer", back_populates="experiments", init=False)

    # Simplified constructor - only handle JSON serialization
    def __init__(
        self,
        *,
        variants: List[str],
        traffic_allocation: Dict[str, float],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        **kw,
    ):
        # Convert all datetimes to UTC before storing
        kw["variants"] = json.dumps(variants)
        kw["traffic_allocation"] = json.dumps(traffic_allocation)
        kw["start_date"] = to_utc(start_date)
        kw["end_date"] = to_utc(end_date)
        kw["created_at"] = to_utc(created_at) or utc_now()
        kw["updated_at"] = to_utc(updated_at) or utc_now()

        super().__init__(**kw)

    # Helper methods
    def get_variant_list(self) -> List[str]:
        return json.loads(self.variants)

    def get_traffic_dict(self) -> Dict[str, float]:
        return json.loads(self.traffic_allocation)

    def is_active(self, now: datetime | None = None) -> bool:
        """Check if experiment is active at the given time (UTC)."""
        if self.status != ExperimentStatus.ACTIVE:
            return False

        # Ensure we're working with UTC datetime
        now = to_utc(now) or utc_now()

        # All stored datetimes are UTC timezone-aware, so comparison is safe
        if self.start_date and now < to_utc(self.start_date):
            return False
        if self.end_date and now > to_utc(self.end_date):
            return False

        return True
