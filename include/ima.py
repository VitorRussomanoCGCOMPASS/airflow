from include.base_schema import CustomSchema
from flask_api.models.ima import ComponentsIMA, IMA
from marshmallow import EXCLUDE, pre_load, fields



from flask_api.dbconnection import EngineManager
manager = EngineManager()
engine = manager.get_engine('localdev')

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

# COMPLETE : FIX THIS.

class ComponentsIMASchema(CustomSchema):
    class Meta:
        model = ComponentsIMA
        unknown = EXCLUDE
        dateformat= "%Y-%m-%d"
        load_instance= True
        include_relationships  = False
        include_fk = True
        sqla_session = session
        

class IMASchema(CustomSchema):
    class Meta:
        model = IMA
        unknown = EXCLUDE
        load_instance= True
        load_relationships = False
        include_relationships =False
        include_fk =False
        sqla_session=session
    
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

    yield_col = fields.Float(data_key="yield")
    components = fields.Nested(ComponentsIMASchema,data_key='componentes',many=True)





import json
with open('C:/Users/Vitor Russomano/airflow/data/anbima/ima_2023-01-12.json','r') as _file:
    data = json.load(_file)


a = IMASchema().load(data,many=True)