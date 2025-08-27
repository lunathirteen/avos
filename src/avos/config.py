from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

from .splitter import HashBasedSplitter
import hashlib


@dataclass
class ExperimentConfig:
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

    experiment_id: str
    name: str
    variants: List[str]
    traffic_allocation: Dict[str, float]
    traffic_percentage: float = 100.0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    target_audience: Optional[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        # Validate traffic allocation
        if any(allocation_perc <= 0 for allocation_perc in self.traffic_allocation.values()):
            raise ValueError("All traffic_allocation values must be greater than 0")

        total_traffic = sum(self.traffic_allocation.values())
        if abs(total_traffic - 100.0) > 0.01:
            raise ValueError(f"Sum of traffic_allocation must be 100%, but is {total_traffic:.2f}%")

        # Validate dates
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")

        # Validate traffic percentage
        if self.traffic_percentage <= 0 or self.traffic_percentage > 100:
            raise ValueError("traffic_percentage must be in (0, 100]")

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

        current_traffic = sum(exp.traffic_percentage for exp in self.experiments)
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
                "slot_id": slot_id
            }

        # Назначаем вариант внутри эксперимента
        experiment = self.experiments[experiment_id]

        hash_splitter = HashBasedSplitter(experiment_id=experiment.experiment_id)
        variant = hash_splitter.assign_variant(unit_id, experiment.variants, experiment.traffic_allocation.values())

        return {
            "unit_id": unit_id,
            "experiment_id": experiment_id,
            "variant": variant,
            "status": "assigned",
            "bucket_id": slot_id,
            "experiment_name": experiment.name
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
            "experiment_slots": experiment_slots
        }

    def _assign_slot(self, user_id: str|int) -> float:
        """Assign unit to slot."""
        hash_input = f"{user_id}{self.layer_salt}".encode("utf-8")
        hash_digest = hashlib.md5(hash_input).hexdigest()
        hash_int = int(hash_digest, 16)

        return int((hash_int % 10000) / 100.0)

    # def finish_experiment(self, experiment_id: str) -> bool:
    #     """
    #     Завершает эксперимент, освобождая его бакеты.

    #     Returns:
    #         True если эксперимент найден и завершен, False если не найден
    #     """
    #     if experiment_id not in self.experiments:
    #         return False

    #     # Освобождаем все бакеты этого эксперимента
    #     freed_buckets = []
    #     for i, bucket in enumerate(self.buckets):
    #         if bucket == experiment_id:
    #             self.buckets[i] = None
    #             freed_buckets.append(i)

    #     # Удаляем из реестра
    #     del self.experiments[experiment_id]

    #     print(f"🔄 Experiment {experiment_id} finished, freed buckets: {freed_buckets}")
    #     return True



    # def _assign_variant_within_experiment(self, unit_id: str, experiment: ExperimentConfig) -> str:
    #     """Назначает вариант внутри эксперимента"""
    #     # Используем experiment-specific hash для назначения варианта
    #     hash_input = f"{unit_id}{self.layer_salt}{experiment.experiment_id}"
    #     hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16) % 100

    #     cumulative_percentage = 0
    #     for variant, percentage in experiment.traffic_allocation.items():
    #         cumulative_percentage += percentage
    #         if hash_value < cumulative_percentage:
    #             return variant

    #     # Fallback
    #     return list(experiment.traffic_allocation.keys())[-1]





    # def get_bucket_visualization(self) -> str:
    #     """Визуализация распределения бакетов (для отладки)"""
    #     viz = []
    #     current_exp = None
    #     start_idx = 0

    #     for i, bucket in enumerate(self.buckets + [None]):  # +[None] для обработки последней группы
    #         if bucket != current_exp:
    #             if current_exp is not None:
    #                 length = i - start_idx
    #                 exp_name = current_exp if current_exp else "FREE"
    #                 viz.append(f"[{start_idx:2d}-{i-1:2d}] {exp_name:12} ({length:2d} buckets)")
    #             current_exp = bucket
    #             start_idx = i

    #     return "\n".join(viz)
