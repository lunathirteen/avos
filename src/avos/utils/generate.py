"""
Module generates fake data
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np


class FakeDataGenerator(ABC):
    def __init__(self, n_events: int = 1000):
        self._n_events = n_events

    @abstractmethod
    def get_data(self):
        pass


class FakeUserRegistration(FakeDataGenerator):
    def _generate_time(self) -> List[datetime]:
        r_times = []
        for _ in range(self._n_events):
            td_hours = np.random.randint(-72, -1)
            td_ms = np.random.randint(-100000, 100000)
            r_times.append(
                datetime.now() + timedelta(hours=td_hours, milliseconds=td_ms)
            )

        return r_times

    def _generate_userid(self) -> List[int]:
        return np.random.randint(1000, 100000, size=self._n_events)

    def _generate_gender(self) -> List[int]:
        return np.random.binomial(1, 0.5, size=self._n_events)

    def get_data(self) -> Dict[str, Any]:
        data = {
            "userid": self._generate_userid(),
            "gender": self._generate_gender(),
            "creationdate": self._generate_time(),
        }
        return data
