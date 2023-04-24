import abc
from typing import Any, Callable, Iterable, Mapping, NoReturn, Sequence, Type

from FileObjects import MSSQLSource, PostgresSource, SQLSource

from airflow.exceptions import (AirflowException, AirflowFailException,
                                AirflowSkipException)
from airflow.providers.common.sql.hooks.sql import (
    fetch_all_handler, return_single_query_results)
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
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
    WRAPPER: Type[SQLSource]

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

        # json_safe_output = json.dumps(processed_output, default=self.convert_types)

        return self.WRAPPER(processed_output, self.stringify_dict)

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


# FIXME : IF THERE IS NO OUTPUT. IT STILL TRIES TO LOOP
class MSSQLOperator(CustomBaseSQLOperator):

    WRAPPER = MSSQLSource

    def __init__(
        self, *, conn_id="mssql-default", database: str | None = None, **kwargs
    ) -> None:

        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(conn_id=conn_id, **kwargs)

    @classmethod
    def process_output(
        cls, results: list[Any] | Any, descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:
        """Processes results from SQL along with descriptions"""
        if results:
            output = []

            for row in results:
                output.append(tuple(row))

            return output
        return results

    @classmethod
    def _results_to_dict(
        cls, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ) -> list[Any]:

        dict_results = []

        if isinstance(descriptions, list):
            if descriptions[-1] is not None:
                column_names = [
                    column[0] if column is not None else None
                    for column in descriptions[-1]
                ]
                for row in results:
                    dict_results.append(dict(zip(column_names, row)))

        return dict_results


class PostgresOperator(CustomBaseSQLOperator):

    WRAPPER = PostgresSource

    def __init__(
        self, *, conn_id="postgres-default", database: str | None = None, **kwargs
    ) -> None:
        super().__init__(database=database, **kwargs, conn_id=conn_id)

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


import json
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class StoredProcedure:
    sp: str
    type: Literal["hard"] | Literal["soft"]
    schema: str = ".dbo"
    params: list[dict] | dict | None = None
    description: str | None = None

    built_stored_procedure: str = field(init=False)
    result: bool | None = field(init=False)

    def __post_init__(self):
        self.result = None
        self.built_stored_procedure = self._parse_stored_procedure(
            self.sp, self.params, self.schema
        )

    @staticmethod
    def _parse_stored_procedure(
        sp: str, params: list[dict] | dict | None, schema: str
    ) -> str:
        # TODO : WE PARSE THE STORED PROCEUDRES STATEMNT
        return "a"


@dataclass
class Checkpoint:
    checkpoint: str
    validations: list[StoredProcedure] | StoredProcedure | str
    description: str | None = None

    def __repr__(self) -> str:
        return f"{self.checkpoint} "


class CheckV2(BaseSQLOperator):
    # checkpoint can be either a path or the obj.
    template_fields: Sequence[str] = "checkpoint"
    template_ext: Sequence[str] = ".json"
    template_fields_renderers = {"checkpoint": ".json"}

    def __init__(
        self,
        *,
        checkpoint: Checkpoint | str,
        conn_id: str | None = None,
        database: str | None = None,
        skip_on_failure: bool = False,
        **kwargs,
    ) -> None:

        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(database=database, **kwargs, conn_id=conn_id)
        self.skip_on_failure = skip_on_failure
        self.checkpoint = checkpoint

    # One checkpoint has multiple validations
    # How can we generate the results and get the type of the validation
    def execute(self, **context):

        if isinstance(self.checkpoint, str):
            checkpoint = json.loads(self.checkpoint)
            self.checkpoint = Checkpoint(**checkpoint)

        self.log.info("Executing Checkpoint : %s ", self.checkpoint.checkpoint)

        # WE ACCESS THE PARSED STORED PROCEDURE
        # WE EXECUTE THEM
        # WE STORE THE RESULTS

        # ACTUALLY, WE CAN jUST STORE THE RESULTS IN THE OBjECT.

        if isinstance(self.checkpoint.validations, StoredProcedure):
            pass


class SQLCheckOperator(BaseSQLOperator):
    """
    Performs checks against a db. The ``SQLCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.
    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.
    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed. (templated)
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    :param parameters: (optional) the parameters to render the SQL query with.
    :param skip_on_failure: (optional) if true it skips downstream tasks instead of raising an exception.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#fff7e6"

    def __init__(
        self,
        *,
        sql: str,
        conn_id: str | None = None,
        database: str | None = None,
        parameters: Iterable | Mapping | None = None,
        skip_on_failure: bool = False,
        **kwargs,
    ) -> None:

        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(database=database, **kwargs, conn_id=conn_id)
        self.sql = sql
        self.parameters = parameters
        self.skip_on_failure = skip_on_failure

    def execute(self, context: Context) -> None:
        self.log.info("Executing SQL check: %s", self.sql)
        records = self.get_db_hook().get_first(self.sql, self.parameters)

        self.log.info("Record: %s", records)

        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")
        elif not all(bool(r) for r in records):
            self._raise_exception(
                f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}"
            )

        self.log.info("Success.")

    def _raise_exception(self, exception_string: str) -> NoReturn:

        if self.skip_on_failure:
            raise AirflowSkipException(exception_string)

        if self.retry_on_failure:
            raise AirflowException(exception_string)
        raise AirflowFailException(exception_string)
