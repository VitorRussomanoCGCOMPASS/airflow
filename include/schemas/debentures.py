from flask_api.models.anbima import TempDebentures
from marshmallow import EXCLUDE, fields, pre_load

from include.schemas.base_schema import CustomSchema


# COMPLETE: LOAD INSTANCE TRUE

class DebenturesSchema(CustomSchema):
    class Meta:
        model = TempDebentures
        unknown = EXCLUDE
        dateformat = "%Y-%m-%d"
        load_instance=True

    data_finalizado = fields.Date("%Y-%m-%dT%H:%M:%S.%f")

    @pre_load
    def pre_loader(self, data, many, **kwargs):
        """
        Pre processes the data. Exchanges '--' for None in percent_reune.
        """
        if data["percent_reune"] == "--":
            data["percent_reune"] = None
    
        if isinstance(data['percent_reune'] , str):
            data["percent_reune"] = float(data["percent_reune"].replace("%", "e-2"))
        return data

    