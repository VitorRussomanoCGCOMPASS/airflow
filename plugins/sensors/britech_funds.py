from typing import Callable, Union

from hooks.britech import BritechHook

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.operator_helpers import determine_kwargs


class StatusCarteiraSensor(BaseSensorOperator):
    template_fields = ('request_params')
    
    def __init__(
        self,
        endpoint: Union[None, str] = None,
        data: Union[str, dict, None] = None,
        headers: Union[dict, None] = None,
        extra_options: Union[dict, None] = None,
        request_params : Union[dict,None] = None,
        **kwargs,
    ):
    
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.headers = headers or {}
        self.extra_options = extra_options or {}
        self.data = data
        self.request_params=  request_params or {} 

    def poke(self, context):
        hook = BritechHook()
        self.log.info("Poking: %s", self.endpoint)
        
        if self.endpoint is None:
            self.endpoint = '/Fromtis/ConsultaStatusCarteiras'
    
        try:
            response = hook.run(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options,
                request_params = self.request_params
            )
            
            if response.json().get('status') is not 'Fechado':
                return False

        except AirflowException as exc:
            if str(exc).startswith("404"):
                return False
            raise exc

        return True
