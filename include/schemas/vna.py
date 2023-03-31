from flask_api.models.anbima import TempVNA
from marshmallow import EXCLUDE, pre_load
from marshmallow.utils import pluck

from include.schemas.base_schema import CustomSchema


class VNASchema(CustomSchema):
    class Meta:
        model = TempVNA
        unknown = EXCLUDE
        load_instance = True

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
        titulos = data[0].get("titulos")

        for titulo in titulos:
            titulo.update({"data_referencia": data_referencia})

        return titulos
