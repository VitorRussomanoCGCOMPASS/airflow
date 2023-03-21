from airflow.sensors.base import BaseSensorOperator
from typing import Union, Callable
from hooks.britech import BritechHook
from airflow.utils.operator_helpers import determine_kwargs
from airflow.exceptions import AirflowException
from itertools import chain


# FIXME: CHECK IF FAILS SILENTLY ON 404


class BritechIndicesSensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers", "request_params")

    def __init__(
        self,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        britech_conn_id: str = "britech-api",
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.endpoint = "/MarketData/BuscaCotacaoIndicePeriodo"
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data
        self.britech_conn_id = britech_conn_id

    def poke(self, context) -> bool:
        """
        Pokes endpoint BuscaCotacaoIndicePeriodo.
        Checking if the indice is available within the date provided.
        Requires ids in the request_params along with the date.


        Returns
        -------
        bool
            True if the indice is available for the day. False otherwise.

        Raises
        ------
        Exception
            If the ids are not in the specified format. Required that the ids must be split by comma.
            e.g. '1 , 2 , 3'

        exc
            404 error

        """
        hook = BritechHook(conn_id=self.britech_conn_id)
        self.log.info("Poking: %s", self.endpoint)

        # TODO: IF ids in xcom format
        ids = self.request_params.get("idIndice")

        if isinstance(ids, list):
            ids = list(chain(*ids))[-1]

        if ids is None:
            raise Exception("Ids should not be None.")

        ids = "".join(ids.split())
        if ids == ";":
            raise Exception(
                "The indices must be specified by a string of ids separated by comma."
            )

        try:
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
                request_params=self.request_params,
            )
            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                return self.response_check(response, **kwargs)

            ids = ids.split(",")

            if len(response.json()) < len(ids):
                return False

        except AirflowException as exc:
            if str(exc).startswith("404"):
                raise exc

        return True


class BritechFundsSensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers", "request_params")

    """


    Provided with 'cnpj' and 'dataReferencia' in request_params. We use the ConsultaStatusCarteira
    in BRITECH API to check the fund status.
    Else if provided with 'id' and 'dataReferencia' in request_params. We call the database to
    get the cnpjs and then call for the britech endpoint to check for the fund status.
    
    """

    def __init__(
        self,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        britech_conn_id: str = "britech-api",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = "/Fromtis/ConsultaStatusCarteiras"
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data
        self.britech_conn_id = britech_conn_id

    def poke(self, context):
        """
        Pokes the ConsultaStatusCarteira endpoint.
        Must have cnpj or id along with date.

        Returns
        -------
        bool:

        Raises
        ------
        Exception
            Incorrect format of ids or insufficient parameters in request_params.

        exc
            404 error
        """

        hook = BritechHook(conn_id=self.britech_conn_id)
        self.log.info("Poking: %s", self.endpoint)

        # TODO : GET IN THE DATABASE
        # TODO : CHECK IF LEN OF IDS AND CNPjS ARE SAME
        # COMPLETE : CALL FOR BRITECH ENDPOINT

        if "ids" in self.request_params:
            ids = self.request_params.pop("id")

            if ids is None:
                raise Exception(
                    "The funds must be specified by a list of ids separated by comma."
                )

            ids = "".join(ids.split())
            if ids == ";":
                raise Exception(
                    "The funds must be specified by a list of ids separated by comma."
                )
            cnpjs = [1, 2, 3, 4]
            self.request_params["cnpj"] = cnpjs

        if "cnpj" in self.request_params:
            cnpjs = self.request_params["cnpj"]

            if isinstance(cnpjs, str):
                cnpjs = cnpjs.split(",")

            for cnpj in cnpjs:
                request_params = self.request_params
                request_params.update({"cnpj": cnpj})

                try:
                    response = hook.run(
                        endpoint=self.endpoint,
                        data=self.data,
                        headers=self.headers,
                        extra_options=self.extra_options,
                        request_params=request_params,
                    )
                    if self.response_check:
                        kwargs = determine_kwargs(
                            self.response_check, [response], context
                        )
                        return self.response_check(response, **kwargs)

                    data = response.json()
                    status = data.get("Status")
                    error = None if data.get("Erro") not in [1, 3] else data.get("Erro")

                    if error:
                        self.log.info(
                            "Fund with cnpj: %s has return error code : %s", cnpj, error
                        )
                        return False

                    if status != "Aberto":
                        self.log.info(
                            "Fund with cnpj: %s has status : %s", cnpj, status
                        )
                        return False

                except AirflowException as exc:
                    if str(exc).startswith("404"):
                        raise exc

            return True


class BritechEmptySensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers", "request_params")

    def __init__(
        self,
        endpoint: Union[str, None] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        britech_conn_id: str = "britech-api",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data
        self.britech_conn_id = britech_conn_id

    def poke(self, context):
        hook = BritechHook(conn_id=self.britech_conn_id)
        self.log.info("Poking: %s", self.endpoint)

        try:
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
                request_params=self.request_params,
            )
            if self.response_check:
                kwargs = determine_kwargs(self.response_check, [response], context)
                return self.response_check(response, **kwargs)

            data = response.json()
            if not data:
                return False

        except AirflowException as exc:
            if str(exc).startswith("404"):
                raise exc

        return True
