from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import hashlib


@dataclass
class Slot:
    slot_id: str
    traffic_percentage: float
    variants: List[str]
    weights: List[float]

    def __post_init__(self):
        if not self.slot_id:
            raise ValueError("slot_id cannot be empty")
        if self.traffic_percentage <= 0 or self.traffic_percentage > 100:
            raise ValueError("slot traffic_percentage must be in (0, 100]")
        if not self.variants or not self.weights:
            raise ValueError("variants and weights must be non-empty")
        if len(self.variants) != len(self.weights):
            raise ValueError("variants and weights lengths must match")
        if any(w <= 0 for w in self.weights):
            raise ValueError("variant weights must be > 0")
        total = sum(self.weights)
        if abs(total - 100.0) > 0.01:
            raise ValueError("slot weights must sum to 100")


@dataclass
class Layer:
    layer_id: str
    layer_salt: str
    total_traffic_percentage: float = 100.0
    slots: List[Slot] = field(default_factory=list)

    def __post_init__(self):
        if not self.layer_id:
            raise ValueError("layer_id cannot be empty")
        if not self.layer_salt:
            raise ValueError("layer_salt cannot be empty")
        if self.total_traffic_percentage <= 0 or self.total_traffic_percentage > 100:
            raise ValueError("layer total_traffic_percentage must be in (0, 100]")
        total_slots_pct = sum(s.traffic_percentage for s in self.slots)
        if total_slots_pct > 100.0 + 1e-9:
            raise ValueError("sum of slots traffic_percentage must be <= 100")

    def _hash_to_percent(self, seed: str) -> float:
        """Convert seed to percentage 0-100."""
        h = hashlib.md5(seed.encode("utf-8")).hexdigest()
        val = int(h, 16) % 100
        return float(val)

    def _get_slot_for_user(self, user_id: str) -> Optional[Slot]:
        """Determine which slot user belongs to based on hash."""
        if not self.slots:
            return None

        seed = f"{user_id}{self.layer_salt}"
        hash_percent = self._hash_to_percent(seed)

        cumulative = 0.0
        for slot in self.slots:
            cumulative += slot.traffic_percentage
            if hash_percent < cumulative:
                return slot

        return None  # User is in unallocated traffic

    def add_slot(self, slot: Slot) -> bool:
        """
        Добавляет слот в слой.
        Возвращает True если удалось добавить, False если нет места.
        """
        # Проверяем, что слот с таким ID еще не существует
        if self.get_slot_by_id(slot.slot_id) is not None:
            raise ValueError(f"Slot with id '{slot.slot_id}' already exists")

        # Проверяем, что есть место для слота
        current_traffic = sum(s.traffic_percentage for s in self.slots)
        if current_traffic + slot.traffic_percentage > 100.0 + 1e-9:
            return False  # Недостаточно места

        # Добавляем слот
        self.slots.append(slot)
        return True

    def remove_slot(self, slot_id: str) -> bool:
        """Remove slot by ID. Returns True if removed, False if not found."""
        initial_length = len(self.slots)
        self.slots = [slot for slot in self.slots if slot.slot_id != slot_id]
        return len(self.slots) < initial_length

    def get_slot_by_id(self, slot_id: str) -> Optional[Slot]:
        """Возвращает слот по ID или None если не найден."""
        for slot in self.slots:
            if slot.slot_id == slot_id:
                return slot
        return None

    def get_allocated_traffic(self) -> float:
        """Возвращает процент трафика, занятый всеми слотами."""
        return sum(slot.traffic_percentage for slot in self.slots)

    def get_unallocated_traffic(self) -> float:
        """Возвращает процент неразмеченного трафика в слое."""
        return 100.0 - self.get_allocated_traffic()

    def has_space_for(self, traffic_percentage: float) -> bool:
        """Проверяет, есть ли место для слота с заданным процентом трафика."""
        return self.get_unallocated_traffic() >= traffic_percentage

    def get_slots_summary(self) -> List[dict]:
        """Возвращает сводку по всем слотам в слое."""
        return [
            {
                'slot_id': slot.slot_id,
                'traffic_percentage': slot.traffic_percentage,
                'variants_count': len(slot.variants),
                'variants': slot.variants
            }
            for slot in self.slots
        ]

    def clear_all_slots(self):
        """Удаляет все слоты из слоя."""
        self.slots.clear()
