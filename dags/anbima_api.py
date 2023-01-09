import json
from airflow.exceptions import AirflowException

import requests
from typing import Any, Union
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
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
    ) -> requests.Response:
        """_summary_

        Parameters
        ----------
        endpoint : Union[None, str], optional
            the endpoint to be called i.e. resource/v1/query?
        data : Union[str, dict, None], optional
            payload to be uploaded
        headers : Union[str, dict, None], optional
            additional headers to be passed through as a dictionary
            query params
        extra_options : Union[dict, None], optional
            additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes

        Returns
        -------
        requests.Response
        """

        token = self.get_token()
        client_id = self.config.login  # self._conn_id

        headers = headers or {}
        headers.update(
            {
                "Content-Type": "application/json",
                "client_id": client_id,
                "access_token": token["access_token"],
            }
        )

        if data is None:
            data = {"grant_type": "client_credentials"}

        session, base_url = self._get_conn()
        session.headers.update(headers)

        # Construct request with base_url

        url = self.url_from_endpoint(endpoint, base_url)

        req = requests.Request(self.method, url, params=data, headers=headers)

        prepped_request = session.prepare_request(req)

        self.log.info("Sending '%s' to url: %s", self.method, prepped_request.url)
        return self.run_and_check(session, prepped_request, extra_options)

    def url_from_endpoint(self, endpoint: Union[str, None], base_url: str) -> str:
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

    def run_and_check(
        self,
        session: requests.Session,
        prepped_request: requests.PreparedRequest,
        extra_options: Union[dict, None] = None,
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


from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.operator_helpers import determine_kwargs
from typing import Callable


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
    


from airflow.models import BaseOperator
import os


class AnbimaOperator(BaseOperator):
    template_fields = ("endpoint", "data", "headers", "output_path")

    def __init__(
        self,
        output_path: str,
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
        self.output_path = output_path

    def execute(self, context):
        hook = AnbimaHook()
        self.log.info("Calling HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            data=self.data,
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
            json.dump(response.text, fp=file_)


from airflow import DAG

from pendulum import duration
from pendulum import datetime
from airflow.operators.empty import EmptyOperator



default_args = {"retries": 1, "retry_delay": duration(minutes=5)}

with DAG(
    dag_id="anbima_resultados_ima",
    default_args=default_args,
    description=None,
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
):

    wait = AnbimaSensor(
        task_id="wait_for_data",
        headers={"data": "{{prev_ds}}"} ,
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        mode="reschedule",
        timeout=60 * 60,
        data=None,
        response_check=None,
        extra_options=None,
    )


    fetch = AnbimaOperator(
        task_id="fetch_data",
        endpoint="/feed/precos-indices/v1/indices-mais/resultados-ima",
        headers={"data": "{{prev_ds}}"} ,
        output_path="C:/Users/Vitor Russomano/airflow/data/custom_operator/{{prev_ds}}.json",
    )

    pull = EmptyOperator(task_id="pull")

    wait.set_downstream(fetch)
    fetch.set_downstream(pull)




