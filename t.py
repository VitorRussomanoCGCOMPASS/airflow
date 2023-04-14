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
    FILE_ENDING: str

    @property
    def FILENAME(self) -> str:
        return "data_" + str(uuid.uuid4()) + self.FILE_ENDING

    @abstractclassmethod
    def save_to_file(self, file: _TemporaryFileWrapper):
        ...

    @classmethod
    def convert_type(cls):
        pass


@dataclass
class HTML(Input):
    FILE_ENDING = ".html"
    html_string: Text

    def save_to_file(self, file: _TemporaryFileWrapper):
        file.write(self.html_string)


@dataclass
class JSON(Input):
    FILE_ENDING = ".json"
    values: Any

    def save_to_file(self, file: _TemporaryFileWrapper):
        json.dump(self.values, file)


@dataclass
class YAML(Input):
    FILE_ENDING = ".yml"
    values: Any

    # FIXME : SELF.VALUES WONT WORK HERE.
    def save_to_file(self, file: _TemporaryFileWrapper):
        yaml.dump(self.values, file)


@dataclass
class ExpectationFile(YAML):
    pass


@dataclass
class SQLSource(Input):
    FILE_ENDING = ".json"
    stringify_dict: bool
    values: Any

    @abstractclassmethod
    def convert_type(self, value, **kwargs):
        """Convert a value from DBAPI to output-friendly formats."""

    def convert_types(self, row):
        return self.convert_type(row, stringify_dict=self.stringify_dict)

    def save_to_file(self, file: _TemporaryFileWrapper):
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
