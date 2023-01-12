from airflow.sensors.base import BaseSensorOperator
from typing import Union, Callable
from hooks.britech import BritechHook
from airflow.utils.operator_helpers import determine_kwargs
from airflow.exceptions import AirflowException


    
class BritechFundsSensor(BaseSensorOperator):
    template_fields = ("endpoint", "headers", "request_params")

    """
    Provided {'cnpj' , 'dataReferencia'} we can use the ConsultaStatusCarteiras
    Else if provided 'id' and 'dataReferencia' we must first get the cnpjs. For that we can use BuscaFundos.
    WE MUST MAKE SURE THAT LEN OF IDS AND CNPjS MATCH
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
