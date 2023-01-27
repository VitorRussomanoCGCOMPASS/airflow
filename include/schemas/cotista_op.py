from flask_api.models.cotista_op import CotistaOp
from marshmallow import EXCLUDE

from include.schemas.base_schema import CustomSchema


class CotistaOpSchema(CustomSchema):
    class Meta:
        model = CotistaOp
        unknown = EXCLUDE
        dateformat = "%Y-%m-%dT%H:%M:%S"
        load_instance = True

