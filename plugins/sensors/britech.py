from airflow.sensors.base import BaseSensorOperator
from typing import Union, Callable
from hooks.britech import BritechHook
from airflow.utils.operator_helpers import determine_kwargs
from airflow.exceptions import AirflowException


class BritechIndicesSensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers", "request_params")

    def __init__(
        self,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params: Union[dict, None] = None,
        response_check: Union[Callable[..., bool], None] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.endpoint = "/MarketData/BuscaCotacaoIndicePeriodo"
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data

    def poke(self, context) -> bool:
        hook = BritechHook()
        self.log.info("Poking: %s", self.endpoint)

        ids = self.request_params.get("idIndices")

        if ids is None:
            raise Exception(
                "The indices must be specified by a string of ids separated by comma."
            )

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

            if  len(response.json()) < len(ids):
                return False

        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False

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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = "/Fromtis/ConsultaStatusCarteiras"
        self.request_params = request_params or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.response_check = response_check
        self.data = data

    def poke(self, context):
        hook = BritechHook()
        self.log.info("Poking: %s", self.endpoint)

        # TODO : GET IN THE DATABASE
        # TODO : CHECK IF LEN OF IDS AND CNPjS ARE SAME
        # TODO : CALL FOR BRITECH ENDPOINT

        ids = self.request_params.pop("id")

        if ids is None:
            raise Exception(
                "The funds must be specified by a list of ids separated by comma."
            )

        ids = "".join(ids.split())
        if ids == ";":
            raise Exception(
                "The indices must be specified by a list of ids separated by comma."
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
        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False

            raise exc

        return True


""" 


        if 'idIndice' in self.request_params:
            id = self.request_params.get('idIndice')
            if id is not None:
                id = "".join(id.split(','))
                id = id.split(',')
                self.request_params.update({'idIndice':id})

 """
