from flask_api.models.vna import VNA
from marshmallow import EXCLUDE

from include.schemas.base_schema import CustomSchema

from marshmallow import pre_load
from marshmallow.utils import pluck






   
class VNASchema(CustomSchema):
    class Meta:
        model = VNA 
        unknown = EXCLUDE
        load_instance= True
    
    # COMPLETE : WE CAN REWRITE THIS!
    @pre_load(pass_many=True)
    def pre_loaders(self, data, many, **kwargs):
        """
        Pre process the data. Applies data_referencia for all the titles.
        # This pre process is complicated. Each pair of data_referencia and titulos is
        # actually many VNA objects. So it needs to reiterate of each dictionary inside titulos and add
        # the correct data_referencia that sits outside.
    
        Returns
        -------
        """
        
        data_referencia = pluck(data, "data_referencia").pop()
        titulos = data[0].get('titulos')

        for titulo in titulos:
            titulo.update({'data_referencia':data_referencia})

        return titulos

