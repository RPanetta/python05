from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[str] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        if isinstance(data, int | float):
            return True
        elif isinstance(data, list[int | float]):
            return True
        elif isinstance(data, str | list[str]):
            return True
        elif isinstance(data, dict[str, str] | list[dict[str, str]]):
            return True
        else:
            return False


    #@abstractmethod
    #def ingest(self, data:  Any)-> None:
    #    try:
    #    except:
    #        print("Error")

    #def output(self)-> tuple[int, str]


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, int):
            return True
        elif isinstance(data, float):
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, int | float):
                    return False
            return True
        else:
            return False

	def ingest(self, data: int | float | list[int | float])-> None:
     try:
         
        except ValueError as e:
        print(f"Got exception: {e}")


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        else:
            return False

    #def ingest(self, data: str | list[str])-> None:


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    return False
                if not isinstance(value, str):
                    return False
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
                for key, value in item.items():
                    if not isinstance(key, str):
                        return False
                    if not isinstance(value, str):
                        return False
            return True
        else:
            return False

    #def ingest(self, data: dict[str, str] | list[dict[str, str]])-> None:


def main() -> None:
    print("=== Code Nexus- Data Processor ===")
    print("\nTesting Numeric Processor...")

    number = NumericProcessor()
    number.storage
    print("Trying to validate input '42':", number.validate(42))
    print("Trying to validate input 'Hello':", number.validate("Hello"))
    print("Test invalid ingestion of string 'foo' without prior validation:")
    


if __name__ == "__main__":
    main()
