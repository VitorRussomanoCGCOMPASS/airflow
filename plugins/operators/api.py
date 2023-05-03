from typing import Any, Callable
import json
from hooks.anbima import AnbimaHook
from hooks.britech import BritechHook
from FileObjects import JSON
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import determine_kwargs


class BritechOperator(BaseOperator):
    """
    Get data from an Britech API endpoint in JSON format.

    :param data: The data to pass (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.    :param extra_options:
    :param log_response: Log the response (default: False)
    :param request_params: params in the URL for a GET request (templated)
    :param endpoint: The relative part of the full url.
    :param json: payload to push


    """

    template_fields = (
        "data",
        "headers",
        "request_params",
    )

    def __init__(
        self,
        *,
        britech_conn_id="britech-api",
        data: str | dict | None = None,
        headers: dict | None = None,
        response_check: Callable[..., bool] | None = None,
        extra_options: dict | None = None,
        log_response: bool = False,
        request_params: dict | None = None,
        endpoint: str,
        json: dict | None = None,
        echo_params: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.britech_conn_id = britech_conn_id
        self.endpoint = endpoint
        self.headers = headers or {}
        self.request_params = request_params or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data
        self.json = json
        self.echo_params = echo_params

    def execute(self, context: Context) -> tuple:
        "Call an specific API endpoint and generate the response"

        hook = BritechHook(method="GET", conn_id=self.britech_conn_id)

        self.log.info("Calling HTTP Method")
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

        if self.echo_params:
            return response.json(), self.request_params
        return response.json()


class AnbimaOperator(BaseOperator):
    """
    Get data from the Anbima API endpoint in JSON format.

    :param data: The data to pass (templated)
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    d:param extra_options:
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
        anbima_conn_id="anbima-api",
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
        self.anbima_conn_id = anbima_conn_id
        self.endpoint = endpoint
        self.headers = headers or {}
        self.request_params = request_params or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data

    def execute(self, context: Context) -> str:
        "Call an specific API endpoint and generate the response"

        hook = AnbimaHook(conn_id=self.anbima_conn_id)

        self.log.info("Calling HTTP Method")
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

        response.encoding= response.apparent_encoding
        print(response.encoding)
        return response.text.encode('utf-8').decode('utf-8')
    
    
    
    
