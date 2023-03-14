from hooks.anbima import AnbimaHook
from hooks.britech import BritechHook
from airflow.models.baseoperator import BaseOperator
from typing import Callable
from airflow.utils.context import Context
from requests import Response
from airflow.utils.operator_helpers import determine_kwargs
from airflow.exceptions import AirflowException
import abc


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
        json: dict| None = None,
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

    def _process_response(self, response: Response) -> Response:
        """"""
        return response


class BritechOperator(BaseAPIOperator):
    def __init__(self, **kwargs) -> None:
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
    def __init__(self, **kwargs) -> None:
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
