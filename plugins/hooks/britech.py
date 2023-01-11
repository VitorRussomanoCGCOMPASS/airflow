from typing import Any, Union

import requests
from connections.britech import BRITECH_CONNECTION
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class BritechHook(BaseHook):
    def __init__(self, context=None, method="GET", conn_id="britech_api"):
        super().__init__(context)
        self._session = None
        self.method = method
        self._conn_id = conn_id

    def _get_conn(self):
        if self._session is None:
            schema = self.config.schema
            host = self.config.host

            self._base_url = f"{schema}://{host}"
            self._session = requests.Session()
        return self._session, self._base_url

    def run(
        self,
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params: Union[dict, None] = None,
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
        extra_options : Union[dict, None], optional
            additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        request_params : Union[dict, None], optional
            query string params

        Returns
        -------
        requests.Response
        """
        config = self.get_connection(self._conn_id)
        self.config = config

        client_id = config.login  # self._conn_id
        client_pass = config.password

        AUTH = HTTPBasicAuth(client_id, client_pass)

        headers = headers or {}

        session, base_url = self._get_conn()
        session.headers.update(headers)

        # Construct request with base_url

        url = self.url_from_endpoint(endpoint, base_url)

        req = requests.Request(
            self.method,
            url,
            data=data,
            headers=headers,
            params=request_params,
            auth=AUTH,
        )

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
