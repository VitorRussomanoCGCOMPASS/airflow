from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
import typing
from marshmallow import types, RAISE, ValidationError
import logging

class CustomSchema(SQLAlchemyAutoSchema):
    def load(
        self,
        data: (
            typing.Mapping[str, typing.Any]
            | typing.Iterable[typing.Mapping[str, typing.Any]]
        ),
        *,
        many: bool | None = None,
        partial: bool | types.StrSequenceOrSet | None = None,
        unknown: str | None = None,
    ):

        if unknown and unknown != "exclude":
            return super().load(data, many=many, partial=partial, unknown=unknown)
        if self.unknown == "exclude" or unknown == "exclude":
            try:
                return super().load(data, many=many, partial=partial, unknown=RAISE)
                #:param unknown: Whether to exclude, include, or raise an error for unknown
                # fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.
                # If `None`, the value for `self.unknown` is used.except ValidationError as err:
            except ValidationError as err:
                item_list = [
                    key
                    for (key, value) in err.messages_dict.items()
                    if value == [self.error_messages.get("unknown")]
                ]
                if item_list:
                    logging.warning(
                        "%s is set for %s. But it received: %s",
                        self,
                        self.unknown,
                        item_list,
                    )

        return super().load(data, many=many, partial=partial, unknown=unknown)

    def loads(
        self,
        json_data: str,
        *,
        many: bool | None = None,
        partial: bool | types.StrSequenceOrSet | None = None,
        unknown: str | None = None,
        **kwargs,
    ):

        data = self.opts.render_module.loads(json_data, **kwargs)

        if unknown and unknown != "exclude":
            return super().load(data, many=many, partial=partial, unknown=unknown)
        if self.unknown == "exclude" or unknown == "exclude":
            try:
                return super().load(data, many=many, partial=partial, unknown=RAISE)
                #:param unknown: Whether to exclude, include, or raise an error for unknown
                # fields in the data. Use `EXCLUDE`, `INCLUDE` or `RAISE`.
                # If `None`, the value for `self.unknown` is used.except ValidationError as err:
            except ValidationError as err:
                item_list = [
                    key
                    for (key, value) in err.messages_dict.items()
                    if value == [self.error_messages.get("unknown")]
                ]
                if item_list:
                    logging.warning(
                        "%s is set for %s. But it received: %s",
                        self,
                        self.unknown,
                        item_list,
                    )

        return super().load(data, many=many, partial=partial, unknown=unknown)
