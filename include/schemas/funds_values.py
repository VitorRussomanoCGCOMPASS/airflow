from flask_api.models.funds import FundsValues, Funds
from marshmallow import EXCLUDE, fields
from include.schemas.base_schema import CustomSchema
from marshmallow_sqlalchemy.fields import Nested
from marshmallow import validate


class FundsSchema(CustomSchema):
    class Meta:
        model = Funds
        unknown = EXCLUDE
        load_instance = False


class FundsValuesSchemas(CustomSchema):
    class Meta:
        model = FundsValues
        unknown = EXCLUDE
        dateformat = "%Y-%m-%dT%H:%M:%S"
        load_instance = False
        load_relationships = False

    funds_id = fields.Integer(data_key="IdCarteira")
    date = fields.Date(data_key="Data")
    




""" 

CotaFechamento = fields.Float(
        validate=validate.NoneOf(
            [
                0,
            ],
        )
    )
    PLFechamento = fields.Float(
        validate=validate.NoneOf(
            [
                0,
            ],
        )
    )


 """