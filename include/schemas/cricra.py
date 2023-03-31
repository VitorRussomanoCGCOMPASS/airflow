from flask_api.models.anbima import TempCriCra
from marshmallow import EXCLUDE, fields, pre_load


from include.schemas.base_schema import CustomSchema


class CriCraSchema(CustomSchema):
    class Meta:
        model = TempCriCra
        unknown = EXCLUDE
        dateformat = "%Y-%m-%d"
        load_instance = True

    data_finalizado = fields.Date("%Y-%m-%dT%H:%M:%S.%f")

    @pre_load
    def fill_series(self, data, many, **kwargs):
        
        if data["serie"] == "":
            data["serie"] = 1
        
        return data

