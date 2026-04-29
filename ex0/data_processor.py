from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = list()
        self._generated: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        return False

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        index: int
        value: str
        if len(self._data) > 0:
            index = self._generated
            value = self._data.pop(0)
            self._generated += 1
        else:
            raise IndexError("No stored data")
        return (index, value)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super(NumericProcessor, self).__init__()

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


if __name__ == "__main__":
    num_proc: NumericProcessor = NumericProcessor()
    text_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    num_list: list[int | float] = [1, 2, 3, 4, 5]
    text_list: list[str] = ["Hello", "Nexus", "World"]
    log_list: list[dict[str, str]] = [
            {"log_level": "NOTICE", "log_message": "Connection to server"},
            {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
    ]
    elem: tuple[int, str]

    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    print(" Trying to validate input '42':", num_proc.validate(42))
    print(" Trying to validate input 'Hello':", num_proc.validate("Hello"))
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except ValueError as error:
        print(" Got exception:", error)
    print(" Processing data:", num_list)
    if num_proc.validate(num_list):
        num_proc.ingest(num_list)
    print(f"Extracting {len(num_list)} values...")
    for i in range(len(num_list)):
        elem = num_proc.output()
        print(f" Numeric value {elem[0]}: {elem[1]}")

    print("\nTesting Text Processor...")
    print(" Trying to validate input '42':", text_proc.validate(42))
    print(" Processing data:", text_list)
    if text_proc.validate(text_list):
        text_proc.ingest(text_list)
    print(f"Extracting {len(text_list)} values...")
    for i in range(len(text_list)):
        elem = text_proc.output()
        print(f" Text value {elem[0]}: {elem[1]}")

    print("\nTesting Log Processor...")
    print(" Trying to validate input 'Hello':", log_proc.validate("Hello"))
    print(" Processing data:", log_list)
    if log_proc.validate(log_list):
        log_proc.ingest(log_list)
    print(f"Extracting {len(log_list)} values...")
    for i in range(len(log_list)):
        elem = log_proc.output()
        print(f" Log entry {elem[0]}: {elem[1]}")
