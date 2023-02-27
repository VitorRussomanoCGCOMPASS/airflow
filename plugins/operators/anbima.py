from typing import Callable, Union

from hooks.anbima import AnbimaHook

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.operator_helpers import determine_kwargs


class AnbimaOperator(BaseOperator):
    template_fields = ("endpoint", "data", "headers", "request_params")

    def __init__(
        self,
        request_params: Union[dict, None] = None,
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        extra_options: Union[dict, None] = None,
        log_response: bool = False,
        **kwargs,
    ):
        super(AnbimaOperator, self).__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data
        self.request_params = request_params or {}
    
    def execute(self, context):
        hook = AnbimaHook()
        self.log.info("Calling HTTP method")

        response = hook.run(
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

        return response.json()
    