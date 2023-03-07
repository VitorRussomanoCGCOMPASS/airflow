import abc
import datetime
import json
import time
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable, Sequence

from hooks.anbima import AnbimaHook
from hooks.britech import BritechHook
from requests import Response

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import (
    fetch_all_handler,
    return_single_query_results,
)

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.context import Context
from airflow.utils.operator_helpers import determine_kwargs


class CustomBaseSQLOperator(BaseSQLOperator):

    """
    Saves data from a specific SQL query into a file in Blob Storage.
    Converting based on the SQL server type to a Json friendly output.


    :param sql: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension. (templated)
    :param conn_id: reference to a specific sql connection
    :param database: reference to a specific database
    :param params: (optional) the params to render the SQL query with.
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param split_statements: (optional) if split single SQL string into statements. By default, defers
        to the default value in the ``run`` method of the configured hook.
    :param return_last: (optional) return the result of only last statement (default: True).
    :param stringify_dict: Whether to dump Dictionary type objects
        (such as JSON columns) as a string.

    """

    template_fields: Sequence[str] = (
        "conn_id",
        "sql",
        "parameters",
    )

    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": ".sql", "parameters": "json"}

    def __init__(
        self,
        *,
        sql: str,
        conn_id: str | None = None,
        database: str | None = None,
        parameters: dict | None = None,
        results_to_dict: bool = False,
        handler: Callable[[Any], Any] = fetch_all_handler,
        split_statements: bool | None = None,
        return_last: bool = True,
        stringify_dict: bool = False,
        max_file_size_bytes: int = 1000000,
        autocommit: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.parameters = parameters or {}
        self.handler = handler
        self.split_statements = split_statements
        self.return_last = return_last
        self.stringify_dict = stringify_dict
        self.max_file_size_bytes = max_file_size_bytes
        self.results_to_dict = results_to_dict
        self.autocommit = autocommit

    def execute(self, context: Context):
        self.log.info("Executing:  %s", self.sql)

        hook = self.get_db_hook()

        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}

        output = hook.run(
            sql=self.sql,
            parameters=self.params,
            return_last=self.return_last,
            handler=self.handler if self.do_xcom_push else None,
            **extra_kwargs,
        )

        if self.split_statements is not None:
            if return_single_query_results(
                self.sql, self.return_last, self.split_statements
            ):
                processed_output = self._process_output([output], hook.descriptions)[-1]

        processed_output = self._process_output(output, hook.descriptions)

        json_safe_output = json.dumps(processed_output, default=self.convert_types)

        return json_safe_output

    @abc.abstractmethod
    def process_output(
        self, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """Processes results from SQL along with descriptions"""

    def _process_output(
        self, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:

        if self.results_to_dict:
            results = self._results_to_dict(results, descriptions)

        results = self.process_output(results, descriptions)

        return results

    def _results_to_dict(
        self, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:

        if isinstance(descriptions, list):
            if descriptions[-1] is not None:
                column_names = [getattr(i, "name", None) for i in descriptions[-1]]
                results = [dict(zip(column_names, i)) for i in results]

        return results

    @abc.abstractmethod
    def convert_type(self, value, **kwargs):
        """Convert a value from DBAPI to output-friendly formats."""

    def convert_types(self, row):
        """Convert values from DBAPI to output-friendly formats."""
        return self.convert_type(row, stringify_dict=self.stringify_dict)

    def render_template_fields(self, context, jinja_env=None) -> None:
        """Add the rendered 'params' to the context dictionary before running the templating"""
        # Like the original method, get the env if not provided
        if not jinja_env:
            jinja_env = self.get_template_env()

        # Run the render template on params and add it to the context
        if self.parameters:
            context["params"] = self.render_template(
                self.parameters, context, jinja_env, set()
            )

        # Call the original method
        super().render_template_fields(context=context, jinja_env=jinja_env)


class WasbToSqlOperator(BaseSQLOperator):
    """
    Loads Data from Blob into a SQL Database
    Need to provide a parser function that takes a filename as an input and returns an iterable of rows

    :param blob_name:
    :param container_name:
    :param wasb_conn_id:
    :param file_path:
    :param table:
    :param parser:
    :param column_list:
    :param commit_every:
    :param schema:
    :param conn_id:

    """

    template_fields: Sequence[str] = (
        "conn_id",
        "schema",
        "table",
        "column_list",
        "file_path",
        "blob_name",
        "container_name",
    )
    template_ext: Sequence[str] = ()

    def __init__(
        self,
        *,
        blob_name: str,
        container_name: str,
        file_path: str,
        table: str,
        parser: Callable[[str], Iterable[Iterable]],
        column_list: list[str] | None = None,
        commit_every: int = 1000,
        schema: str | None = None,
        conn_id: str,
        wasb_conn_id: str = "wasb_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.blob_name = blob_name
        self.container_name = container_name
        self.table = table
        self.parser = parser
        self.column_list = column_list
        self.commit_every = commit_every
        self.schema = schema
        self.conn_id = conn_id
        self.wasb_conn_id = wasb_conn_id
        self.file_path = file_path

    def execute(self, context: Context):
        self.log.info("Loading %s to SQL table %s...", self.blob_name, self.table)

        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        with NamedTemporaryFile() as temp:

            self.log.info("Downloading data from blob: %s", self.blob_name)
            wasb_hook.get_file(
                file_path=temp.name,
                container_name=self.container_name,
                blob_name=self.blob_name,
            )

            temp.flush()
            temp.seek(0)

            self.db_hook.insert_rows(
                table=self.table,
                schema=self.schema,
                target_fields=self.column_list,
                rows=self.parser(temp.name),
                commit_every=self.commit_every,
            )

    @cached_property
    def db_hook(self):
        self.log.debug("Get connection for %s: ", self.conn_id)
        hook = self.get_db_hook()
        if not callable(getattr(hook, "insert_rows", None)):
            raise AirflowException(
                "This hook is not supported. The hook class must have an `insert_rows` method."
            )
        return hook


class BaseAPIOperator(BaseOperator):
    """
    Copy data from an API to Wasb in JSON format.

    :param data: The data to pass (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.    :param extra_options:
    :param log_response: Log the response (default: False)
    :param request_params: params in the URL for a GET request (templated)
    :param endpoint: The relative part of the full url.

    """

    template_fields = (
        "data",
        "headers",
        "request_params",
    )

    def __init__(
        self,
        *,
        data: str | dict | None = None,
        headers: dict | None = None,
        response_check: Callable[..., bool] | None = None,
        extra_options: dict | None = None,
        log_response: bool = False,
        request_params: dict | None = None,
        endpoint: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers or {}
        self.request_params = request_params or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data
        self.json = json

    def execute(self, context: Context):

        response = self.call_response(context)

        return response

    @abc.abstractmethod
    def _call_response(
        self,
        endpoint: str,
        data: str | dict | None = None,
        headers: dict | None = None,
        request_params: dict | None = None,
        extra_options: dict | None = None,
    ) -> Response:
        "Call an specific API endpoint and generate the response"

    def call_response(self, context: Context):
        "Call an specific API endpoint and generate the response"

        self.log.info("Calling HTTP Method")
        response = self._call_response(
            endpoint=self.endpoint,
            data=self.data,
            headers=self.headers,
            request_params=self.request_params,
            extra_options=self.extra_options,
        )

        if self.log_response:
            self.log.info(response.text)

        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")

        output = self._process_response(response)

        return output

    def _process_response(self, response: Response):
        """"""
        return response


class BritechOperator(BaseAPIOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def _call_response(
        cls, endpoint, data, headers, request_params, extra_options
    ) -> Response:
        hook = BritechHook(method="GET")

        response = hook.run(
            endpoint=endpoint,
            data=data,
            headers=headers,
            request_params=request_params,
            extra_options=extra_options,
            json=None,
        )
        return response

    def _process_response(self, response: Response):
        return response.json()


class AnbimaOperator(BaseAPIOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def _call_response(
        cls, endpoint, data, headers, request_params, extra_options
    ) -> Response:
        hook = AnbimaHook()

        response = hook.run(
            endpoint=endpoint,
            data=data,
            headers=headers,
            request_params=request_params,
            extra_options=extra_options,
        )

        return response

    def _process_response(self, response: Response):
        return response.json()


class GeneralSQLExecuteQueryOperator(CustomBaseSQLOperator):
    def process_output(
        self, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        if self.results_to_dict:
            results = self._results_to_dict(results, descriptions)

        results = self.process_output(results, descriptions)

        return results

    def convert_type(self, value, **kwargs) -> Any:
        return value


class MSSQLOperator(CustomBaseSQLOperator):
    def __init__(
        self, *, conn_id="mssql_default", database: str | None = None, **kwargs
    ) -> None:
        super().__init__(database=database, **kwargs, conn_id=conn_id)

    @classmethod
    def convert_type(cls, value, **kwargs) -> float | str | Any:
        """
        Takes a value from MSSQL, and converts it to a value that's safe for JSON

        Datetime, Date and Time are converted to ISO formatted strings.
        """
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()
        return value


class PostgresOperator(CustomBaseSQLOperator):
    def __init__(
        self, *, conn_id="postgres_default", database: str | None = None, **kwargs
    ) -> None:
        super().__init__(database=database, **kwargs, conn_id=conn_id)

    @classmethod
    def convert_type(cls, value, stringify_dict=True) -> float | str | Any:
        """
        Takes a value from Postgres, and converts it to a value that's safe for JSON

        Timezone aware Datetime are converted to UTC seconds.
        Unaware Datetime, Date and Time are converted to ISO formatted strings.

        Decimals are converted to floats.
        :param value: Postgres column value.
        :param schema_type: BigQuery data type.
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

    @classmethod
    def process_output(
        cls, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """Processes results from SQL along with descriptions"""
        return results
