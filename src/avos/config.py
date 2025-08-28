from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

from .splitter import HashBasedSplitter
from avos.models.experiment import ExperimentConfig
import hashlib


@dataclass
class Layer:
    """Слой экспериментов с фиксированным количеством бакетов"""

    layer_id: str
    layer_salt: str
    total_slots: int = 100
    total_traffic_percentage: float = 100.0
    slots: List[Optional[str]] = field(default_factory=list)
    experiments: Optional[Dict[str, ExperimentConfig]] = field(default_factory=dict)

    def __post_init__(self):
        if not self.layer_id:
            raise ValueError("layer_id cannot be empty")
        if not self.layer_salt:
            raise ValueError("layer_salt cannot be empty")
        if self.total_slots == 0:
            raise ValueError("total_slots cannot be 0")
        if self.total_traffic_percentage <= 0 or self.total_traffic_percentage > 100:
            raise ValueError("layer total_traffic_percentage must be in (0, 100]")
        if not self.slots:  # Only initialize if empty
            self.slots = [None] * self.total_slots
        if self.total_slots != len(self.slots):
            raise ValueError("The number of slots must match total_slots")

    def get_free_slots_count(self) -> int:
        """Возвращает количество свободных бакетов"""
        return sum(1 for slot in self.slots if slot is None)

    def add_experiment(self, experiment: ExperimentConfig) -> bool:
        """
        Добавляет эксперимент, выделяя ему нужное количество свободных бакетов.

        Returns:
            True если эксперимент успешно добавлен, False если недостаточно места
        """
        if experiment.experiment_id in self.experiments:
            raise ValueError(f"Experiment {experiment.experiment_id} already exists in layer")

        current_traffic = sum(exp.traffic_percentage for exp in self.experiments.values())
        if current_traffic + experiment.traffic_percentage > 100.0 + 1e-9:
            return False  # Недостаточно места

        slots_needed = int((experiment.traffic_percentage / 100) * self.total_slots)
        if slots_needed > self.get_free_slots_count():
            return False  # Недостаточно свободных бакетов

        # Находим свободные слоты
        free_slots = [i for i, slot in enumerate(self.slots) if slot is None]
        assigned_slots = free_slots[:slots_needed]

        # Назначаем слоты эксперименту
        for slot_id in assigned_slots:
            self.slots[slot_id] = experiment.experiment_id

        # Сохраняем конфигурацию эксперимента
        self.experiments[experiment.experiment_id] = experiment

        print(f"✅ Experiment {experiment.experiment_id} assigned slots: {assigned_slots}")
        return True

    def get_user_assignment(self, unit_id: str) -> Dict[str, Any]:
        """
        Определяет назначение пользователя в эксперименты.

        Returns:
            Словарь с информацией о назначении
        """

        slot_id = self._assign_slot(unit_id)

        # Получаем эксперимент из бакета
        experiment_id = self.slots[slot_id]

        if experiment_id is None:
            return {
                "unit_id": unit_id,
                "experiment_id": None,
                "variant": None,
                "status": "not_assigned",
                "slot_id": slot_id,
            }

        # Назначаем вариант внутри эксперимента
        experiment = self.experiments[experiment_id]

        hash_splitter = HashBasedSplitter(experiment_id=experiment.experiment_id)
        variant = hash_splitter.assign_variant(unit_id, experiment.variant_list(), experiment.traffic_dict().values())

        return {
            "unit_id": unit_id,
            "experiment_id": experiment_id,
            "variant": variant,
            "status": "assigned",
            "bucket_id": slot_id,
            "experiment_name": experiment.name,
        }

    def get_layer_info(self) -> Dict[str, Any]:
        """Возвращает информацию о состоянии слоя"""
        free_slots = self.get_free_slots_count()

        # Подсчитываем бакеты по экспериментам
        experiment_slots = {}
        for experiment_id in self.experiments:
            experiment_slots[experiment_id] = sum(1 for slot in self.slots if slot == experiment_id)

        return {
            "layer_id": self.layer_id,
            "total_slots": self.total_slots,
            "free_slots": free_slots,
            "used_slots": self.total_slots - free_slots,
            "utilization_percentage": ((self.total_slots - free_slots) / self.total_slots) * 100,
            "active_experiments": len(self.experiments),
            "experiment_slots": experiment_slots,
        }

    def _assign_slot(self, user_id: str | int) -> int:
        """Assign unit to slot."""
        hash_input = f"{user_id}{self.layer_salt}".encode("utf-8")
        hash_digest = hashlib.md5(hash_input).hexdigest()
        hash_int = int(hash_digest, 16)

        return int(hash_int % self.total_slots)

    def remove_experiment(self, experiment_id: str) -> bool:
        """
        Завершает эксперимент, освобождая его бакеты.

        Returns:
            True если эксперимент найден и завершен, False если не найден
        """
        if experiment_id not in self.experiments:
            return False

        # Освобождаем все бакеты этого эксперимента
        freed_slots = []
        for i, slot in enumerate(self.slots):
            if slot == experiment_id:
                self.slots[i] = None
                freed_slots.append(i)

        # Удаляем из реестра
        del self.experiments[experiment_id]

        print(f"🔄 Experiment {experiment_id} finished, freed slots: {freed_slots}")
        return True
