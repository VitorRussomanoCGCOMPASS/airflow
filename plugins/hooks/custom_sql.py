from airflow.compat.functools import cached_property
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from typing import NoReturn
from airflow.exceptions import AirflowException, AirflowFailException
from sqlalchemy import create_engine

# FIXME: THIS IS IN THE WRONG PLACE.


class NewOp(BaseOperator):
    """
    This is a base class for generic SQL Operator to get a DB Hook
    The provided method is .get_db_hook(). The default behavior will try to
    retrieve the DB hook based on connection type.
    You can customize the behavior by overriding the .get_db_hook() method.
    :param conn_id: reference to a specific database
    """

    conn_name_attr: str

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str | None = None,
        hook_params: dict | None = None,
        retry_on_failure: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.hook_params = {} if hook_params is None else hook_params
        self.retry_on_failure = retry_on_failure

    @cached_property
    def _conn(self) -> Connection:
        self.log.debug("Get connection for %s", self.conn_id)

        if not self.conn_id:
            raise Exception("Invalid conn_id")

        conn = BaseHook.get_connection(self.conn_id)
        conn.schema = self.database
        return conn

    @cached_property
    def _hook(self) -> DbApiHook:
        conn = self._conn()
        hook: DbApiHook = conn.get_hook(hook_params=self.hook_params)

        if self.database:
            hook.schema = self.database

        return hook

    def get_db_hook(self) -> DbApiHook:
        """
        Get the database hook for the connection.
        :return: the database hook object.
        """

        return self._hook

    def _raise_exception(self, exception_string: str) -> NoReturn:
        if self.retry_on_failure:
            raise AirflowException(exception_string)
        raise AirflowFailException(exception_string)

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.
        :return: the extracted uri.
        """
        conn = self._conn()
        return conn.get_uri()

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(self.get_uri(), **engine_kwargs)
