from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sensors.base import BaseSensorOperator


class SqlSensor(BaseSensorOperator):
    """
    Runs a sql statement repeatedly until a criteria is met. It will keep trying until
    success or failure criteria are met, or if the first cell is not in (0, '0', '', None).
    Optional success and failure callables are called with the first cell returned as the argument.
    If success callable is defined the sensor will keep retrying until the criteria is met.
    If failure callable is defined and the criteria is met the sensor will raise AirflowException.
    Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also
    be passed to the sensor in which case it will fail if no rows have been returned
    :param conn_id: The connection to run the sensor against
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :param parameters: The parameters to render the SQL query with (optional).
    :param success: Success criteria for the sensor is a Callable that takes first_cell
        as the only argument, and returns a boolean (optional).
    :param failure: Failure criteria for the sensor is a Callable that takes first_cell
        as the only argument and return a boolean (optional).
    :param fail_on_empty: Explicitly fail on no rows returned.
    :param hook_params: Extra config params to be passed to the underlying hook.
            Should match the desired hook constructor params.
    """

    template_fields: Sequence[str] = ("sql", "parameters")
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": ".sql", "parameters": "json"}

    ui_color = "#7c7287"

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        sql,
        success=None,
        failure=None,
        fail_on_empty=False,
        hook_params=None,
        parameters: dict | None = None,
        **kwargs,
    ):
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters or {}
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        self.hook_params = hook_params
        self.parameters = parameters
        super().__init__(**kwargs)

    def _get_hook(self) -> DbApiHook:
        if not self.conn_id:
            raise Exception("<(param)> conn_id is required. But was not provided.")

        conn = BaseHook.get_connection(self.conn_id)
        hook = conn.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        return hook

    def poke(self, context: Any):
        hook = self._get_hook()

        self.log.info("Poking: %s (with parameters %s)", self.sql, self.parameters)
        records = hook.get_records(self.sql)

        if not records:
            if self.fail_on_empty:
                raise AirflowException(
                    "No rows returned, raising as per fail_on_empty flag"
                )
            else:
                return False

        first_cell = records[0][0]

        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(
                        f"Failure criteria met. self.failure({first_cell}) returned True"
                    )
            else:
                raise AirflowException(
                    f"self.failure is present, but not callable -> {self.failure}"
                )
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException(
                    f"self.success is present, but not callable -> {self.success}"
                )
        return bool(first_cell)

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


class SoftSQLCheckSensor(BaseSensorOperator):
    """
    Sensor that performs a check against a database by running an SQL query that returns a single row.
    Each value on that first row is evaluated using Python `bool` casting. If any of the values return 'False'
    the check is failed, and the sensor sleeps before trying again according to the poke_interval.

    Note that Python bool casting evals the following as `False`:
    - `False`
    - `0`
    - Empty string (`""`)
    - Empty list (`[]`)
    - Empty dictionary or set (`{}`)

    Optionally, we can set `fail_parameters`, so that we execute the same query given by `sql`, but with parameters that
    will hard fail the sensor, instead of making it sleep. This way it is possible to create ranges where it should
    instantly fail, or just wait for the next `poke_interval`.

    :param conn_id: The connection ID to use to connect to the database. If not provided, it will be inferred from
        the `hook_params`.
    :type conn_id: str, optional
    :param sql: The SQL query to execute against the database.
    :type sql: str
    :param hook_params: Extra parameters to pass to the underlying hook.
    :type hook_params: dict, optional
    :param mode: Whether the sensor should `reschedule` or `fail` when the check fails.
    :type mode: str, optional
    :param exponential_backoff: Whether the sensor should use exponential backoff when retrying the check.
    :type exponential_backoff: bool, optional
    :param soft_fail: Whether the sensor should soft fail or hard fail when the check fails.
    :type soft_fail: bool, optional 
    :param parameters: A dictionary of parameters to render the SQL query with.
    :type parameters: dict, optional
    :param fail_parameters: A dictionary of parameters to render the SQL query with, to check for a hard failure.
    :type fail_parameters: dict, optional
    :param poke_interval: Time in seconds that the job should wait in between each try.
    :type poke_interval: int or float

    :raises AirflowException: If the failure query returns zero rows.
    """
    template_fields: Sequence[str] = (
        "sql",
        "parameters",
        "fail_parameters",
        "fail_sql",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {
        "sql": ".sql",
        "parameters": "json",
        "fail_parameterse": ".sql",
        "fail_sql": ".sql",
    }

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        sql,
        hook_params=None,
        mode: str = "reschedule",
        exponential_backoff: bool = True,
        soft_fail: bool = False,
        parameters: dict | None = None,
        fail_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters or {}
        self.hook_params = hook_params
        self.parameters = parameters
        self.fail_parameters = fail_parameters or {}
        super().__init__(
            mode=mode,
            soft_fail=soft_fail,
            exponential_backoff=exponential_backoff,
            **kwargs,
        )

        self.fail_sql = self.construct_fail_sql(sql) if fail_parameters else None

    def _get_hook(self) -> DbApiHook:
        if not self.conn_id:
            raise Exception("<(param)> conn_id is required. But was not provided.")

        conn = BaseHook.get_connection(self.conn_id)
        hook = conn.get_hook(hook_params=self.hook_params)
        if not isinstance(hook, DbApiHook):
            raise AirflowException(
                f"The connection type is not supported by {self.__class__.__name__}. "
                f"The associated hook should be a subclass of `DbApiHook`. Got {hook.__class__.__name__}"
            )
        return hook

    @classmethod
    def construct_fail_sql(cls, sql):
        fail_sql = sql.replace("params.", "params.failure.")
        return fail_sql

    def poke(self, context: Any):
        hook = self._get_hook()

        if self.fail_sql:
            self.log.info("Running failure sql statement : %s ", self.fail_sql)
            records = hook.get_first(self.fail_sql)

            if not records:
                raise AirflowException(
                    f"The failure query returned zero rows: {self.fail_sql}"
                )
            self.log.info("Failure sql statement succeded")

        self.log.info("Poking: %s", self.sql)

        records = hook.get_first(self.sql)

        if not records:
            self.log.warning("The following query returned zero rows %s:", self.sql)
            return False

        elif not all(bool(r) for r in records):
            self.log.warning(f"\nQuery:\n{self.sql}\nResults:\n{records!s}")
            return False

        self.log.info("Success.")

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

        if self.fail_parameters:
            value = self.render_template(
                self.fail_parameters, context, jinja_env, set()
            )
            if "params" not in context:
                raise Exception("Could not find params in context")
            context["params"].update({"failure": value})
        # Run the render template on params  for failure sql and add it to the context

        # Call the original method
        super().render_template_fields(context=context, jinja_env=jinja_env)
