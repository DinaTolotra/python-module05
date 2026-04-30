from abc import ABC, abstractmethod
from typing import Any, Protocol
from sys import stderr


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        output_list: list[tuple[int, str]]
        remaining: int
        for proc in self._processor_list:
            output_list = list()
            remaining = proc.get_data_count()
            remaining -= proc.get_processed_count()
            for i in range(min(nb, remaining)):
                output_list.append(proc.output())
            plugin.process_output(output_list)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        record_list: list[str] = list()
        record: str
        for item in data:
            record = '"item_{}": "{}"'.format(item[0], item[1])
            record_list.append(record)
        print("JSON Output:")
        print("{", ", ".join(record_list), "}")


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        str_list: list[str] = list()
        for item in data:
            str_list.append(item[1])
        print("CSV Output:")
        print(",".join(str_list))


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("\nInitialize Data Stream...\n")
    num_proc: NumericProcessor = NumericProcessor()
    text_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    data_stream: DataStream = DataStream()
    json_exporter: JSONExportPlugin = JSONExportPlugin()
    csv_exporter: CSVExportPlugin = CSVExportPlugin()
    data_1: list = [
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
    data_2: list = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": " ERROR",
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

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("")

    print("\nRegistering Processors\n")
    data_stream.register_processor(num_proc)
    data_stream.register_processor(text_proc)
    data_stream.register_processor(log_proc)

    print("Send first batch of data on stream:", data_1)
    data_stream.process_stream(data_1)

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("")

    print("Send 3 processed data from each processor to a CSV plugin:")
    data_stream.output_pipeline(3, csv_exporter)
    print("")

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("")

    print("Send another batch of data:", data_2)
    data_stream.process_stream(data_2)

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("")

    print("Send 5 processed data from each processor to a JSON plugin:")
    data_stream.output_pipeline(5, json_exporter)
    print("")

    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("")
