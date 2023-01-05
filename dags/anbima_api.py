import json

import requests
from typing import Any
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

ANBIMA_CONNECTION = Connection(
    conn_id="anbima_api",
    conn_type="http",
    description=None,
    login="mL8an5sznCN3",
    password=None,
    host="api.anbima.com.br",
    port=443,
    schema="https",
)

# COMPLETE : Create a Operator (Instead of pushing XCOM from the Sensor, so we are not fully commited to using XCOM and can change the behaviour in the Operator in the future  )


class AnbimaHook(BaseHook):
    def __init__(self):  # , conn_id = 'anbima_api'
        super().__init__()
        self._session = None
        self.method = "GET"
        self.config = ANBIMA_CONNECTION
        # self._conn_id = conn_id

    def _get_conn(self):
        if self._session is None:

            schema = self.config.schema
            host = self.config.host
            port = self.config.port

            self._base_url = f"{schema}://{host}:{port}"
            self._session = requests.Session()

        return self._session, self._base_url

    def get_token(self):
        """
        Requests for Anbima access token - expires in 60 min.

        Returns
        -------
        token : str
            access token
        """
        payload = {"grant_type": "client_credentials"}
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic bUw4YW41c3puQ04zOkc1aHgwVUczMjZBaw==",
        }
        session, base_url = self._get_conn()
        response = session.request(
            "POST",
            base_url + "/oauth/access-token",
            json=payload,
            headers=headers,
        )

        return json.loads(response.text)

    def run(
        self,
        endpoint: str | None = None,
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """

        Parameters
        ----------
        endpoint : str
        the endpoint to be called i.e. resource/v1/query?
        data : dict
            payload to be uploaded
        headers : dict
            additional headers to be passed through as a dictionary
        params : dict
            query params
        extra_options : dict
            additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes

        Returns
        -------
        requests.Response
        """
        token = self.get_token()
        client_id = self.config.login  # self._conn_id

        if headers is None:
            headers = {
                "Content-Type": "application/json",
                "client_id": client_id,
                "access_token": token["access_token"],
            }

        if data is None:
            data = {"grant_type": "client_credentials"}

        extra_options = extra_options or {}

        session, base_url = self._get_conn()
        session.headers.update(headers)

        # COMPLETE : Should create better the request. Also, not use extra_options here
        # COMPLETE: params should be {'data':extra_options}

        # Construct request with base_url
        req = requests.Request(self.method, base_url, params=data, headers=headers)

        prepped_request = session.prepare_request(req)

        url = self.url_from_endpoint(endpoint, base_url)

        # add params to the processed url
        if params:
            prepped_request.prepare_url(url, params=params)

        self.log.info("Sending '%s' to url: %s", self.method, prepped_request.url)
        return self.run_and_check(session, prepped_request, extra_options)

    def check_response(self, response):
        """
        Checks the status code and raise an AirflowException exception on non 2XX or 3XX
        status codes

        :param response: A requests response object
        :type response: requests.response
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def url_from_endpoint(self, endpoint: str | None, base_url: str | None) -> str:
        if (
            base_url
            and not base_url.endswith("/")
            and endpoint
            and not endpoint.startswith("/")
        ):
            url = base_url + "/" + endpoint
        else:
            url = (base_url or "") + (endpoint or "")
        return url

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: dict[Any, Any],
    ) -> Any:
        """
        Grabs extra options like timeout and actually runs the request,
        checking for the result
        :param session: the session to be used to execute the request
        :param prepped_request: the prepared request generated in run()
        :param extra_options: additional options to be used when executing the request
            i.e. ``{'check_response': False}`` to avoid checking raising exceptions on non 2XX
            or 3XX status codes
        """
        extra_options = extra_options or {}

        settings = session.merge_environment_settings(
            prepped_request.url,
            proxies=extra_options.get("proxies", {}),
            stream=extra_options.get("stream", False),
            verify=extra_options.get("verify"),
            cert=extra_options.get("cert"),
        )

        # Send the request.
        send_kwargs: dict[str, Any] = {
            "timeout": extra_options.get("timeout"),
            "allow_redirects": extra_options.get("allow_redirects", True),
        }
        send_kwargs.update(settings)

        try:
            response = session.send(prepped_request, **send_kwargs)

            if extra_options.get("check_response", True):
                self.check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning("%s Tenacity will retry to execute the operation", ex)
            raise ex

    def test_connection(self):
        """Test HTTP Connection"""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)


from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import determine_kwargs


class AnbimaSensor(BaseSensorOperator):
    template_fields = ("endpoint", "request_params", "headers")

    def __init__(
        self,
        endpoint: str,
        data: Any = None,
        request_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        response_check=None,
        extra_options: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data or {}

    def poke(self, context: Context):
        hook = AnbimaHook()
        self.log.info("Poking: %s", self.endpoint)
        # COMPLETE :  NEED TO USE THE DATE IN THE ENDPOINT.

        try:
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                params=self.request_params,
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


from airflow.models import BaseOperator
from typing import Callable

from pendulum import datetime
import os


class AnbimaOperator(BaseOperator):
    template_fields = ("endpoint", "data", "headers", "_output_path")

    def __init__(
        self,
        endpoint: str,
        output_path: str,
        data: Any = None,
        request_parms: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        response_check: Callable[..., bool] | None = None,
        extra_options: dict[str, Any] | None = None,
        log_response: bool = False,
        **kwargs,
    ):
        super(AnbimaOperator, self).__init__(**kwargs)
        self.endpoint = endpoint
        self.request_params = request_parms or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.log_response = log_response
        self.data = data
        self.output_path = output_path

    def execute(self, context):
        hook = AnbimaHook()
        self.log.info("Calling HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            data=self.data,
            params=self.request_params,
            headers=self.headers,
            extra_options=self.extra_options,
        )

        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")

        self.log.info(f"Writing to {self.output_path}")
        output_dir = os.path.dirname(self.output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self.output_path, "w") as file_:
            json.dump(response, fp=file_)


from airflow import DAG
from airflow.operators.empty import EmptyOperator


default_args = {"retries": 1, "retry_delay": datetime.timedelta(minutes=5)}

with DAG(
    dag_id="04_sensor",
    default_args=default_args,
    description=None,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    wait = AnbimaSensor(
        task_id="wait",
        request_params={"data": "{{ds}}"},
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        mode="reschedule",
        timeout=60,
    )

    fetch = AnbimaOperator(
        task_id="fetch",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        request_parms={"data": "{{ds}}"},
        output_path="/data/custom_operator/{{ds}}.json",
    )

    pull = EmptyOperator(task_id="pull")
