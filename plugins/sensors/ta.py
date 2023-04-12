import json
from typing import Sequence, Iterable

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class CheckpointSerializable(object):
    def toJson(self):
        return json.dumps(self.__dict__)

    def __repr__(self):
        return self.toJson()


class Checkpoint(CheckpointSerializable):
    def __init__(
        self,
        soft_expectations: Iterable[dict] | dict,
        hard_expectations: Iterable[dict] | dict | None = None,
        name: str | None = None,
        data_asset_name: str | Iterable[str] | None = None,
        description: str | None = None,
    ):
        self.name = name
        self.data_asset_name = data_asset_name
        self.soft_expectations = soft_expectations
        self.hard_expectations = hard_expectations
        self.description = description


class Sensor(BaseSensorOperator):
    template_fields: Sequence[str] = (
        "sql",
        "parameters",
    )

    template_ext: Sequence[str] = ("sql",)

    template_fields_renderers = {"sql": ".sql", "parameters": ".json"}

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        sql,
        parameters: dict | None = None,
        exponential_backoff: bool = True,
        mode: str = "reschedule",
        hook_params=None,
        **kwargs,
    ):

        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters or {}
        self.hook_params = hook_params

        super().__init__(
            mode=mode,
            exponential_backoff=exponential_backoff,
            **kwargs,
        )

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

    def poke(self, context: Context):

        pass

    def render_template_fields(self, context, jinja_env=None) -> None:
        if not jinja_env:
            jinja_env = self.get_template_env()

        if self.parameters:
            context["params"] = self.render_template(
                self.parameters, context, jinja_env, set()
            )

            try:
                checkpoint = Checkpoint(**self.parameters)
            except TypeError as exc:
                raise exc
