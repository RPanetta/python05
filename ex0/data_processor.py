from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any)-> bool:

    @abstractmethod
    def ingest(self, data:  Any)-> None:

    def outputt(self)-> tuple[int, str]:


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def validate(self, data: Any)-> bool:

    def ingest(self, data: int | float | list[int | float])-> None:


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def validate(self, data: Any)-> bool:

    def ingest(self, data: str | list[str])-> None:

class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    def validate(self, data: Any)-> bool:

    def ingest(self, data: dict[str, str] | list[dict[str, str]])-> None:


def main() -> None:
    print("=== Code Nexus- Data Processor ===")
    print("\nTesting Numeric Processor...")


if __name__ == "__main__":
    main()
    