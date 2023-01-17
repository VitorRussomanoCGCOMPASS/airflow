from flask_api.models.cricra import CriCra
from marshmallow import EXCLUDE


from include.base_schema import CustomSchema


class CriCraSchema(CustomSchema):
    class Meta:
        model = CriCra
        unknown = EXCLUDE
        load_instance= True
 