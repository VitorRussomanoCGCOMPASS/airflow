from typing import Callable, Union
import json

from hooks.britech import BritechHook

from airflow.exceptions import AirflowException
from airflow.utils.operator_helpers import determine_kwargs


from airflow.models import BaseOperator
import os


class BritechOperator(BaseOperator):
    template_fields = ("endpoint", "data", "headers", "output_path", "request_params")

    def __init__(
        self,
        output_path: Union[str, None] = None,
        request_params: Union[dict, None] = None,
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        extra_options: Union[dict, None] = None,
        log_response: bool = False,
        json: Union[dict, None] = None,
        method: str = "GET",
        **kwargs,
    ):
        super(BritechOperator, self).__init__(**kwargs)
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.request_params = request_params or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data
        self.output_path = output_path
        self.json = json

    def execute(self, context):
        hook = BritechHook(method=self.method)
        self.log.info("Calling HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            data=self.data,
            headers=self.headers,
            request_params=self.request_params,
            extra_options=self.extra_options,
            json=self.json,
        )

        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")

        if self.method == "GET" and self.output_path:
            self.log.info(f"Writing to {self.output_path}")
            output_dir = os.path.dirname(self.output_path)
            os.makedirs(output_dir, exist_ok=True)

            with open(self.output_path, "w") as file_:
                json.dump(response.json(), fp=file_)

        return response.json()


