from abc import ABC, abstractmethod
import typing
from typing import Any, Protocol


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        lines: list[str] = []
        for _, value in data:
            lines.append(value)

        print("CSV Output:")
        print(",".join(lines))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        items: list[str] = []
        for rank, value in data:
            items.append(f'"item_{rank}": "{value}"')

        print("JSON Output:")
        print("{" + ", ".join(items) + "}")


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

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for processor in self.processors:
            print(f"{processor.__class__.__name__}: "
                  f"total {processor.total_processed} items processed, "
                  f"remaining {len(processor.storage)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:

            collected: list[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    collected.append(processor.output())
                except ValueError:
                    break
            if collected:
                plugin.process_output(collected)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")

    stream = DataStream()

    print("\nInitialize Data Stream...")
    stream.print_processors_stats()

    number = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    stream.register_processor(number)
    stream.register_processor(text)
    stream.register_processor(log)

    print("\nRegistering Processors")

    batch1 = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print(f"\nSend first batch of data on stream: {batch1}")

    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    stream.print_processors_stats()

    batch2 = [
        21,
        [
            "I love AI",
            "LLMs are wonderful",
            "Stay healthy"
        ],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print(f"\nSend another batch of data: {batch2}")

    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
