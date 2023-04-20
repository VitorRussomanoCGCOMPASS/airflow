from abc import ABC, abstractclassmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union
import yaml
from typing import Any, List, Optional, Text, Union
from tempfile import _TemporaryFileWrapper
import json
import uuid



class Input(ABC):
    FILE_EXTENSION: str | tuple

    @property
    def FILENAME(self) -> str:

        FILE_EXTENSION = self.FILE_EXTENSION
        
        if isinstance(FILE_EXTENSION, tuple):
            FILE_EXTENSION = FILE_EXTENSION[0]

        return "data_" + str(uuid.uuid4()) + FILE_EXTENSION

    @abstractclassmethod
    def save_to_file(self, file: _TemporaryFileWrapper) -> None:
        ...

    @abstractclassmethod
    def read_from_file(self, file: _TemporaryFileWrapper) -> None:
        ...

    @classmethod
    def convert_type(cls) -> None:
        pass


@dataclass
class HTML(Input):
    FILE_EXTENSION = ".html"
    html_string: Text

    def save_to_file(self, file: _TemporaryFileWrapper) -> None:
        file.write(self.html_string)

    def read_from_file(self, file) -> str:
        html_file = open(file.name, "r", encoding="utf-8")
        return html_file.read()


@dataclass
class JSON(Input):
    FILE_EXTENSION = ".json"
    values: Any

    def save_to_file(self, file: _TemporaryFileWrapper) -> None:
        json.dump(self.values, file)

    def read_from_file(self, file):
        return json.load(file)


@dataclass
class YAML(Input):
    FILE_EXTENSION = (".yml", ".yaml")
    values: Any

    # FIXME : SELF.VALUES WONT WORK HERE.
    def save_to_file(self, file: _TemporaryFileWrapper) -> None:
        yaml.dump(self.values, file)

    def read_from_file(self, file):
        return yaml.safe_load(file.name)


@dataclass
class ExpectationFile(YAML):
    pass


@dataclass
class SQLSource(JSON):
    stringify_dict: bool

    @abstractclassmethod
    def convert_type(self, value, **kwargs) -> None:
        """Convert a value from DBAPI to output-friendly formats."""

    def convert_types(self, row) -> None:
        return self.convert_type(row, stringify_dict=self.stringify_dict)

    def save_to_file(self, file: _TemporaryFileWrapper) -> None:
        json.dumps(self.values, default=self.convert_types)


import time
import datetime
from decimal import Decimal


@dataclass
class MSSQLSource(SQLSource):
    @classmethod
    def convert_type(cls, value, **kwargs) -> float | str | Any:
        """
        Takes a value from MSSQL, and converts it to a value that's safe for JSON

        :param value: MSSQL Column value

        Datetime, Date and Time are converted to ISO formatted strings.
        """

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()

        return value


@dataclass
class PostgresSource(SQLSource):
    @classmethod
    def convert_type(cls, value, stringify_dict=True) -> float | str | Any:
        """
        Takes a value from Postgres, and converts it to a value that's safe for JSON

        Timezone aware Datetime are converted to UTC seconds.
        Unaware Datetime, Date and Time are converted to ISO formatted strings.

        Decimals are converted to floats.
        :param value: Postgres column value.
        :param stringify_dict: Specify whether to convert dict to string.
        """

        if isinstance(value, datetime.datetime):
            iso_format_value = value.isoformat()
            if value.tzinfo is None:
                return iso_format_value
            return parser.parse(iso_format_value).float_timestamp  # type: ignore

        if isinstance(value, datetime.date):
            return value.isoformat()

        if isinstance(value, datetime.time):
            formatted_time = time.strptime(str(value), "%H:%M:%S")
            time_delta = datetime.timedelta(
                hours=formatted_time.tm_hour,
                minutes=formatted_time.tm_min,
                seconds=formatted_time.tm_sec,
            )
            return str(time_delta)

        if stringify_dict and isinstance(value, dict):
            return json.dumps(value)

        if isinstance(value, Decimal):
            return float(value)

        return value
