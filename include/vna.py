from flask_api.models.vna import VNA
from marshmallow import EXCLUDE

from include.base_schema import CustomSchema


class VNASchema(CustomSchema):
    class Meta:
        model = VNA 
        unknown = EXCLUDE
        load_instance= True
 