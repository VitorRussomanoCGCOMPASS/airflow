import abc
import datetime
import json
import os
import pathlib
import time
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable, Mapping, Sequence

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


class BaseSQLToWasbOperator(BaseSQLOperator):
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
    :param container_name: Name of the container. (templated)
    :param blob_name: Name of the blob. (templated)
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
    :param load_options: Optional keyword arguments that 'WasbHook.load_file()' takes.
    :param wasb_overwrite_object: Whether the blob to be uploaded should overwrite the current data.
    When wasb_overwrite_object is True, it will overwrite the existing data.
    If set to False, the operation might fail with
    ResourceExistsError in case a blob object already exists. Defaults to True
    :param wasb_conn_id: Reference to the wasb connection. Defaults to wasb_default
    :param stringify_dict: Whether to dump Dictionary type objects
        (such as JSON columns) as a string.
    :param max_file_size_bytes: To set the approx. maximum size bytes on a file.

    """

    template_fields: Sequence[str] = (
        "conn_id",
        "sql",
        "params",
        "blob_name",
        "container_name",
    )

    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": ".sql", "params": "json"}

    def __init__(
        self,
        *,
        sql: str,
        conn_id: str | None = None,
        database: str | None = None,
        params: dict | None = None,
        dict_cursor: bool = False,
        handler: Callable[[Any], Any] = fetch_all_handler,
        split_statements: bool | None = None,
        return_last: bool = True,
        blob_name: str,
        container_name: str,
        create_container: bool = False,
        load_options: dict | None = None,
        wasb_overwrite_object: bool = True,
        wasb_conn_id: str = "wasb_default",
        stringify_dict: bool = False,
        max_file_size_bytes: int = 1000000,
        **kwargs,
    ) -> None:
        super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.params = params or {}
        self.handler = handler
        self.split_statements = split_statements
        self.return_last = return_last
        self.blob_name = blob_name
        self.container_name = container_name
        self.wasb_conn_id = wasb_conn_id
        self.create_container = create_container
        self.load_options = load_options or {"overwrite": wasb_overwrite_object}
        self.stringify_dict = stringify_dict
        self.max_file_size_bytes = max_file_size_bytes
        self.dict_cursor = dict_cursor

    def execute(self, context: Context) -> None:
        self.log.info("Executing:  %s", self.sql)

        hook = self.get_db_hook()
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}

        output = hook.run(
            sql=self.sql,
            parameters=self.params,
            handler=self.handler,
            return_last=self.return_last,
            **extra_kwargs,
        )

        if self.split_statements is not None:
            if return_single_query_results(
                self.sql, self.return_last, self.split_statements
            ):
                processed_output = self._process_output([output], hook.descriptions)[-1]

        processed_output = self._process_output(output, hook.descriptions)

        with NamedTemporaryFile(mode="w", suffix=".json") as tmp:

            self.log.info("Writing data to temp file %s", tmp.name)

            json.dump(processed_output, tmp, default=self.convert_types)
            tmp.flush()

            file_size = os.stat(tmp.name).st_size

            if file_size >= self.max_file_size_bytes:
                raise AirflowException(
                    "Allowed file size is %s (bytes). Given file size is %s. ",
                    self.max_file_size_bytes,
                    file_size,
                )

            self.log.info(
                "Uploading to wasb://%s as %s",
                self.container_name,
                self.blob_name,
            )

            blob_name = self.blob_name + pathlib.Path(tmp.name).suffix

            wasb_hook.load_file(
                file_path=tmp.name,
                container_name=self.container_name,
                blob_name=blob_name,
                create_container=self.create_container,
                **self.load_options,
            )
    def _process_output(
        self, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """If self.dict_cursor turns into dict"""
        if self.dict_cursor:
            column_names = list(map(lambda x: x.name,  descriptions[0]))
            results = [dict(zip(column_names, row)) for row in results]
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
        if self.params:
            context["params"] = self.render_template(
                self.params, context, jinja_env, set()
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


class BaseAPIToWasbOperator(BaseOperator):
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
    :param wasb_conn_id: Reference to the wasb connection. Defaults to 'wasb_default'
    :param container_name: Name of the container. (templated)
    :param blob_name: Name of the blob. (templated)
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
    :param load_options: Optional keyword arguments that 'WasbHook.load_file()' takes.
    :param wasb_overwrite_object: Whether the blob to be uploaded should overwrite the current data.
    When wasb_overwrite_object is True, it will overwrite the existing data.
    If set to False, the operation might fail with ResourceExistsError in case a blob object already exists. Defaults to True
    :param max_file_size_bytes: To set the approx. maximum size bytes on a file.

    """

    template_fields = (
        "data",
        "headers",
        "request_params",
        "blob_name",
        "container_name",
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
        wasb_conn_id: str = "wasb_default",
        blob_name: str,
        container_name: str,
        create_container: bool = False,
        load_options: dict | None = None,
        wasb_overwrite_object: bool = True,
        max_file_size_bytes: int = 1000000,
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
        self.blob_name = blob_name
        self.container_name = container_name
        self.wasb_conn_id = wasb_conn_id
        self.create_container = create_container
        self.load_options = load_options or {"overwrite": wasb_overwrite_object}
        self.max_file_size_bytes = max_file_size_bytes

    def execute(self, context: Context):

        response = self.call_response(context)

        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        with NamedTemporaryFile(mode="w", suffix=".json") as tmp:

            self.log.info("Writing data to temp file %s", tmp.name)

            json.dump(response.json(), tmp)

            tmp.flush()

            file_size = os.stat(tmp.name).st_size

            if file_size >= self.max_file_size_bytes:
                raise AirflowException(
                    "File size %s excedeed allowed size in bytes (%s) ",
                    file_size,
                    self.max_file_size_bytes,
                )

            self.log.info(
                "Uploading to wasb://%s as %s",
                self.container_name,
                self.blob_name,
            )

            blob_name = self.blob_name + pathlib.Path(tmp.name).suffix

            wasb_hook.load_file(
                file_path=tmp.name,
                container_name=self.container_name,
                blob_name=blob_name,
                create_container=self.create_container,
                **self.load_options,
            )

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

    def call_response(self, context: Context) -> Response:
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
        return response


class BritechToWasbOperator(BaseAPIToWasbOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def _call_response(cls, endpoint, data, headers, request_params, extra_options):
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


class AnbimaToWasbOperator(BaseAPIToWasbOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def _call_response(cls, endpoint, data, headers, request_params, extra_options):
        hook = AnbimaHook()

        response = hook.run(
            endpoint=endpoint,
            data=data,
            headers=headers,
            request_params=request_params,
            extra_options=extra_options,
        )

        return response


class MSSQLToWasbOperator(BaseSQLToWasbOperator):
    def __init__(self, *, conn_id="mssql_default", database, **kwargs):
        super().__init__(database=database, **kwargs, conn_id=conn_id)

    @classmethod
    def convert_type(cls, value, **kwargs):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for JSON

        Datetime, Date and Time are converted to ISO formatted strings.
        """
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (datetime.date, datetime.time)):
            return value.isoformat()
        return value



class PostgresToWasbOperator(BaseSQLToWasbOperator):
    def __init__(self, *, conn_id="postgres_default", database, **kwargs):
        super().__init__(database=database, **kwargs, conn_id=conn_id)

    @classmethod
    def convert_type(cls, value, stringify_dict=True):
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


