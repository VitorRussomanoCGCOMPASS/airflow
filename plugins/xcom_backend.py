import json
import os
import uuid
from tempfile import NamedTemporaryFile
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class HTMLXcom:
    def __init__(self, html_string: str, **kwargs) -> None:
        self.html_string = html_string


class CustomXComBackendJSON(BaseXCom):
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
    ):

        hook = WasbHook(wasb_conn_id="wasb-default")

        with NamedTemporaryFile(mode="w") as tmp:
            # FIXME : THIS IS TERRIBLE. BUT IDK H0W IS AIRFLOW IMPORTING SO THAT THERE IS NO MISMATHC.
            if str(type(value)) == "<class 'xcom_backend.HTMLXcom'>":
                tmp.write(value.html_string)
                filename = "data_" + str(uuid.uuid4()) + ".html"
            else:
                json.dump(value, tmp)
                filename = "data_" + str(uuid.uuid4()) + ".json"

            # write the value to a local temporary file
            tmp.flush()

            file_size = os.stat(tmp.name).st_size

            if file_size >= CustomXComBackendJSON.MAX_FILE_SIZE_BYTES:
                raise AirflowException(
                    "Allowed file size is %s (bytes). Given file size is %s. ",
                    CustomXComBackendJSON.MAX_FILE_SIZE_BYTES,
                    file_size,
                )

            blob_key = f"{run_id}/{task_id}/{filename}"

            # load the local  file into Azure Blob Storage
            hook.load_file(
                file_path=tmp.name,
                container_name=CustomXComBackendJSON.CONTAINER_NAME,
                blob_name=blob_key,
            )

        # define the string that will be saved to the Airflow metadata
        # database to refer to this XCom
        reference_string = CustomXComBackendJSON.PREFIX + blob_key

        # use JSON serialization to write the reference string to the
        # Airflow metadata database (like a regular XCom)
        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result) -> str | Any:
        # retrieve the relevant reference string from the metadata database
        hook = WasbHook(wasb_conn_id="wasb-default")
        reference_string = BaseXCom.deserialize_value(result=result)

        blob_key = reference_string.replace(CustomXComBackendJSON.PREFIX, "")

        with NamedTemporaryFile() as temp:

            hook.get_file(
                file_path=temp.name,
                container_name=CustomXComBackendJSON.CONTAINER_NAME,
                blob_name=blob_key,
                offset=0,
                length=100000,
            )

            temp.flush()
            temp.seek(0)

            if reference_string.endswith(".html"):
                HtmlFile = open(temp.name, "r", encoding="utf-8")
                output = HtmlFile.read()
            else:
                output = json.load(temp)
                if isinstance(output, str):
                    # Try removing double encoded json
                    try:
                        output = json.loads(output)
                    except json.decoder.JSONDecodeError:
                        pass

        return output

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
