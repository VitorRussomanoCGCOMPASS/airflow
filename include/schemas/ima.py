from include.schemas.base_schema import CustomSchema
from flask_api.models.ima import ComponentsIMA, IMA
from marshmallow import EXCLUDE, pre_load
from marshmallow.fields import Float
from marshmallow_sqlalchemy.fields import Nested
    




# COMPLETE : FIX THIS.

class ComponentsIMASchema(CustomSchema):
    class Meta:
        model = ComponentsIMA
        unknown = EXCLUDE
        dateformat= "%Y-%m-%d"
        load_instance= True
        include_relationships  = False
        include_fk = True
    


class IMASchema(CustomSchema):
    class Meta:
        model = IMA
        unknown = EXCLUDE
        load_instance= True
        load_relationships = False
        include_relationships =False
        include_fk =False
    
    @pre_load
    def pre_loader(self,data,many,**kwargs):
        self.context['indice'] = data['indice']
        self.context['data_referencia'] =data['data_referencia']

        indice = data['indice']
        data_referencia =data['data_referencia']
        components= data['componentes'] 

        for component in components:
            component.update({"indice":indice,"data_referencia":data_referencia})

        return data

    yield_col = Float(data_key="yield")
    components = Nested(ComponentsIMASchema,data_key='componentes',many=True)
