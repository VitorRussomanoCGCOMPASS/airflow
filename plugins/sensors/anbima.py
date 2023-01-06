from typing import Callable, Union

from hooks.anbima import AnbimaHook

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.operator_helpers import determine_kwargs


class AnbimaSensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers")

    def __init__(
        self,
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data

    def poke(self, context):
        hook = AnbimaHook()
        self.log.info("Poking: %s", self.endpoint)

        try:
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
            )
            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                return self.response_check(response, **kwargs)
        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False

            raise exc

        return True
