import os
from tempfile import NamedTemporaryFile
from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
from FileObjects import JSON, Input
from typing import NoReturn, Callable


class BackendException(Exception):
    pass


class CustomXComBackend(BaseXCom):

    # the prefix is optional and used to make it easier to recognize
    # which reference strings in the Airflow metadata database
    # refer to an XCom that has been stored in Azure Blob Storage
    PREFIX = "xcom_wasb://"
    CONTAINER_NAME = "rgbrprdblob"
    MAX_FILE_SIZE_BYTES = 1000000

    @staticmethod
    def serialize_value(
        value,
        key=None,
        task_id=None,
        dag_id=None,
        run_id=None,
        map_index=None,
        **kwargs,
    ) -> None:

        hook = WasbHook(wasb_conn_id="wasb-default")
        print("checkppoint 1 ")
        print(type(value))

        with NamedTemporaryFile(mode="w") as tmp:
            # Defaults to JSON

            enforced_json = False
            # Defaults to JSON
            if not isinstance(value, Input):
                value = JSON(values=value)
                enforced_json = True
                logging.info("Defaulted to JSON backend handler")

            # Explicit point of failure
            try:
                value.save_to_file(tmp)
            except Exception as exc:
                CustomXComBackend._raise_exception(
                    original_exception=exc, enforced_json=enforced_json
                )

            filename = value.FILENAME
            tmp.flush()

            file_size = os.stat(tmp.name).st_size

            if file_size >= CustomXComBackend.MAX_FILE_SIZE_BYTES:
                raise BackendException(
                    "Allowed file size is %s (bytes). Given file size is %s. ",
                    CustomXComBackend.MAX_FILE_SIZE_BYTES,
                    file_size,
                )

            blob_key = f"{run_id}/{task_id}/{filename}"

            # load the local  file into Azure Blob Storage
            hook.load_file(
                file_path=tmp.name,
                container_name=CustomXComBackend.CONTAINER_NAME,
                blob_name=blob_key,
            )

        # define the string that will be saved to the Airflow metadata
        # database to refer to this XCom
        reference_string = CustomXComBackend.PREFIX + blob_key

        # use JSON serialization to write the reference string to the
        # Airflow metadata database (like a regular XCom)
        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result) -> str | Any:
        # retrieve the relevant reference string from the metadata database
        hook = WasbHook(wasb_conn_id="wasb-default")
        reference_string = BaseXCom.deserialize_value(result=result)

        blob_key = reference_string.replace(CustomXComBackend.PREFIX, "")

        with NamedTemporaryFile() as temp:

            hook.get_file(
                file_path=temp.name,
                container_name=CustomXComBackend.CONTAINER_NAME,
                blob_name=blob_key,
                offset=0,
                length=100000,
            )

            temp.flush()
            temp.seek(0)

            file_extension = os.path.splitext(reference_string)[1]

            for subclass in Input.__subclasses__():
                if (
                    file_extension == subclass.FILE_EXTENSION
                    or file_extension in subclass.FILE_EXTENSION
                ):
                    print("subclass %s", subclass)

                    read_from_file: Callable | None = getattr(
                        subclass, "read_from_file", None
                    )

                    if read_from_file:
                        output = read_from_file(temp)
                        return output

            raise BackendException(
                "File extension is not supported by current backend."
            )

    # Overriding so we don't have a bunch of calls due to UI.
    def orm_deserialize_value(self) -> Any:
        """
        Deserialize method which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom orm model. This is used when viewing XCom listing
        in the webserver, for example.
        """
        reference_string = BaseXCom._deserialize_value(self, True)
        return reference_string

    @staticmethod
    def _raise_exception(original_exception, enforced_json: bool) -> NoReturn:
        if enforced_json:
            raise BackendException(
                "No Backend supported classes were provided. Enforcing JSON did not work"
            )
        raise original_exception
