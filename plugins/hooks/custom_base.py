from airflow.hooks.base import BaseHook
from typing import Iterable

from abc import abstractmethod, ABC
from enum import Enum


class Method(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class CustomBaseHook(BaseHook, ABC):
    def __init__(self, method: str, **kwargs):
        self._val_methods(method, self.allowed_methods)
        self.method = method
        super().__init__(**kwargs)

    @staticmethod
    def _val_methods(method: str, allowed_methods) -> None:

        if isinstance(allowed_methods, Method):
            allowed_methods = [allowed_methods]

        if isinstance(allowed_methods, Iterable):
            allowed_methods = [x.value for x in allowed_methods]

        if method not in allowed_methods:
            raise ValueError(f"Invalid method '{method}' provided.")

    @property
    @abstractmethod
    def allowed_methods(self):
        return self.allowed_methods


