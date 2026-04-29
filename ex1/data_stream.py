from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = list()
        self._processed: int = 0
        self._name: str

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def get_name(self) -> str:
        return self._name

    def output(self) -> tuple[int, str]:
        index: int
        value: str
        if len(self._data) > 0:
            index = self._processed
            value = self._data.pop(0)
            self._processed += 1
        else:
            raise IndexError("No stored data")
        return (index, value)

    def get_processed_count(self) -> int:
        return self._processed

    def get_data_count(self) -> int:
        return len(self._data)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super(NumericProcessor, self).__init__()
        self._name = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(elem, (int, float)) for elem in data)
        else:
            return isinstance(data, (int, float))

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for elem in data:
                self._data.append(str(elem))
        else:
            self._data.append(str(data))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super(TextProcessor, self).__init__()
        self._name = "Text Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(elem, str) for elem in data)
        else:
            return isinstance(data, str)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for elem in data:
                self._data.append(elem)
        else:
            self._data.append(data)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super(LogProcessor, self).__init__()
        self._name = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return (
                all(isinstance(key, str) for el in data for key in el.keys())
                and all(isinstance(v, str) for el in data for v in el.values())
            )
        elif isinstance(data, dict):
            return (
                all(isinstance(key, str) for key in data.keys())
                and all(isinstance(v, str) for v in data.values())
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for elem in data:
                elem_str: str = "{}: {}".format(
                    elem.get('log_level'),
                    elem.get('log_message')
                )
                self._data.append(elem_str)
        else:
            data_str: str = "{}: {}".format(
                data.get('log_level'),
                data.get('log_message')
            )
            self._data.append(data_str)


class DataStream:
    def __init__(self) -> None:
        self._processor_list: list[DataProcessor] = list()

    def register_processor(self, proc: DataProcessor) -> None:
        self._processor_list.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        ingested: bool
        for data in stream:
            ingested = False
            for proc in self._processor_list:
                if proc.validate(data):
                    proc.ingest(data)
                    ingested = True
                    break
            if not ingested:
                print(
                    "DataStream error -",
                    "Can't process element in stream:",
                    data
                )

    def print_processors_stats(self) -> None:
        processed: int = 0
        remaining: int = 0
        if len(self._processor_list) > 0:
            for proc in self._processor_list:
                processed = proc.get_processed_count()
                remaining = proc.get_data_count()
                print(
                    f"{proc.get_name()}:",
                    f"total {processed + remaining} items processed,",
                    f"remaining {remaining} on processor"
                )
        else:
            print("No processor found, no data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream: DataStream = DataStream()
    num_proc: NumericProcessor = NumericProcessor()
    text_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    data: list[Any] = [
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
            },
        ],
        42,
        ["Hi", "five"]
    ]

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()

    print("Registering Numeric Processor")
    data_stream.register_processor(num_proc)
    print("Send first batch of data on stream:", data)
    data_stream.process_stream(data)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()

    print("Registering other data processors")
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)
    print("Send the same batch again")
    data_stream.process_stream(data)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print()

    print(
        "Consume some elements from the data processors:",
        "Numeric 3,",
        "Text 2,",
        "Log 1"
    )
    for i in range(3):
        num_proc.output()
    for i in range(2):
        text_proc.output()
    log_proc.output()
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
