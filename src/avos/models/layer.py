from __future__ import annotations
from datetime import datetime
from typing import List, TYPE_CHECKING

from sqlalchemy import String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column

from avos.constants import BUCKET_SPACE
from avos.models.base import Base
from avos.utils.datetime_utils import utc_now

if TYPE_CHECKING:
    from avos.models.experiment import Experiment


class Layer(Base):
    __tablename__ = "layers"

    layer_id: Mapped[str] = mapped_column(String, primary_key=True)
    layer_salt: Mapped[str] = mapped_column(String, nullable=False)

    total_slots: Mapped[int] = mapped_column(Integer, default=BUCKET_SPACE)
    total_traffic_percentage: Mapped[float] = mapped_column(Float, default=1.0)

    # UTC timezone-aware timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=utc_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default_factory=utc_now, onupdate=utc_now)

    slots: Mapped[List["LayerSlot"]] = relationship(
        "LayerSlot", back_populates="layer", cascade="all, delete-orphan", default_factory=list
    )
    experiments: Mapped[List["Experiment"]] = relationship(
        "Experiment", back_populates="layer", cascade="all, delete-orphan", default_factory=list
    )


class LayerSlot(Base):
    __tablename__ = "layer_slots"

    layer_id: Mapped[str] = mapped_column(String, ForeignKey("layers.layer_id"), primary_key=True)
    slot_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_id: Mapped[str | None] = mapped_column(String, ForeignKey("experiments.experiment_id"))
    reserved_experiment_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("experiments.experiment_id"), nullable=True, default=None
    )

    layer: Mapped["Layer"] = relationship(
        "Layer",
        back_populates="slots",
        uselist=False,
        init=False,
        repr=False,
    )
    experiment: Mapped["Experiment"] = relationship(
        "Experiment",
        foreign_keys=[experiment_id],
        init=False,
        repr=False,
    )
