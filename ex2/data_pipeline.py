from abc import ABC, abstractmethod
import typing
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.next_rank: int = 0
        self.total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise ValueError("Processor has no available data")
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError(f"Improper numeric data: {data}")
        if isinstance(data, list):
            for item in data:
                self.storage.append((self.next_rank, str(item)))
                self.next_rank += 1
                self.total_processed += 1
        else:
            self.storage.append((self.next_rank, str(data)))
            self.next_rank += 1
            self.total_processed += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError(f"Improper text data: {data}")
        if isinstance(data, list):
            for item in data:
                self.storage.append((self.next_rank, item))
                self.next_rank += 1
                self.total_processed += 1
        else:
            self.storage.append((self.next_rank, data))
            self.next_rank += 1
            self.total_processed += 1


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
        if isinstance(data, list):
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

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError(f"Improper log data: {data}")

        if isinstance(data, dict):
            log_level = data.get("log_level", "")
            log_message = data.get("log_message", "")
            result = log_level + ": " + log_message
            self.storage.append((self.next_rank, result))
            self.next_rank += 1
            self.total_processed += 1

        else:
            for item in data:
                log_level = item.get("log_level", "")
                log_message = item.get("log_message", "")
                result = log_level + ": " + log_message
                self.storage.append((self.next_rank, result))
                self.next_rank += 1
                self.total_processed += 1


class Protocol:
    def __init__(self):
        pass


class ExportPlugin(Protocol):
    def __init__(self):
        super().__init__()
    
    def process_output(self, data: list[tuple[int, str]]) -> None:


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled = False

            for processor in self.processors:
                if processor.validate(element):
                    try:
                        processor.ingest(element)
                        handled = True
                        break
                    except Exception as error:
                        print(error)

            if not handled:
                print(f"DataStream error - "
                      f"Can't process element in stream: {element}")
    
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:


class Protocol:
    def __init__(self):
        pass


class ExportPlugin(Protocol):
    def __init__(self):
        super().__init__()
    
    def process_output(self, data: list[tuple[int, str]]) -> None:


def main() -> None:


if __name__=="__main__":
    main()