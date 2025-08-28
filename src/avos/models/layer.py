from __future__ import annotations
from datetime import datetime, UTC
from typing import List, Optional

from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column

from avos.models.base import Base



class Layer(Base):
    __tablename__ = "layers"

    layer_id:   Mapped[str] = mapped_column(String, primary_key=True)
    layer_salt: Mapped[str] = mapped_column(String, nullable=False)

    total_slots:              Mapped[int]   = mapped_column(Integer, default=100)
    total_traffic_percentage: Mapped[float] = mapped_column(Float,   default=100.0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default_factory=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default_factory=lambda: datetime.now(UTC),
        onupdate=datetime.now
    )

    slots: Mapped[List["LayerSlot"]] = relationship(
        "LayerSlot",
        back_populates="layer",
        cascade="all, delete-orphan",
        default_factory=list
    )
    experiments: Mapped[List["Experiment"]] = relationship(
        "Experiment",
        back_populates="layer",
        cascade="all, delete-orphan",
        default_factory=list
    )

class LayerSlot(Base):
    __tablename__ = "layer_slots"

    layer_id: Mapped[str] = mapped_column(
        String, ForeignKey("layers.layer_id"), primary_key=True
    )
    slot_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    experiment_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("experiments.experiment_id")
    )

    # scalar back-refs – leave them out of the constructor
    layer: Mapped["Layer"] = relationship(
        "Layer",
        back_populates="slots",
        uselist=False,
        init=False,          #  ← key line
        repr=False,
    )
    experiment: Mapped["Experiment"] = relationship(
        "Experiment",
        init=False,
        repr=False,
    )
