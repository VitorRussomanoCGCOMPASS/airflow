import abc
import datetime
import json
import time
from decimal import Decimal
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable, Sequence


from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import (
    fetch_all_handler,
    return_single_query_results,
)
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.context import Context


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

        if not self.do_xcom_push:
            return None

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

        results = self.process_output(results, descriptions)

        if self.results_to_dict:
            results = self._results_to_dict(results, descriptions)

        return results

    @abc.abstractmethod
    def _results_to_dict(
        self, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        ...

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


class GeneralSQLExecuteQueryOperator(CustomBaseSQLOperator):
    @classmethod
    def process_output(
        cls, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """Processes results from SQL along with descriptions"""
        return results

    @classmethod
    def convert_type(cls, value, **kwargs) -> Any:
        return value


class MSSQLOperator(CustomBaseSQLOperator):
    def __init__(
        self, *, conn_id="mssql-default", database: str | None = None, **kwargs
    ) -> None:

        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(conn_id=conn_id, **kwargs)

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

    @classmethod
    def process_output(
        cls, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """Processes results from SQL along with descriptions"""
        output = []

        for row in results:
            output.append(tuple(row))
        return output

    #FIXME : PLS
    @classmethod
    def _results_to_dict(
        cls, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:

        dict_results = []
        print(results)

        if isinstance(descriptions, list):
            print(descriptions)
            if descriptions[-1] is not None:
                column_names = [
                    column[0] if column is not None else None for column in descriptions[-1]
                ]
                for row in results:
                    print(row)
                    print(column_names)
                    dict_results.append(dict(zip(column_names, row)))

        return dict_results



class PostgresOperator(CustomBaseSQLOperator):
    def __init__(
        self, *, conn_id="postgres-default", database: str | None = None, **kwargs
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

    @classmethod
    def _results_to_dict(
        cls, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:


        if isinstance(descriptions, list):
            if descriptions[-1] is not None:
                column_names = [getattr(i, "name", None) for i in descriptions[-1]]
                results = [dict(zip(column_names, i)) for i in results]

        return results

