import json
import uuid
from tempfile import NamedTemporaryFile
from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import os
from airflow.exceptions import AirflowException

# TODO : SUFFIX /  PREFFIX DYNAMIC

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
    # the connection to Wasb is created by using the WasbHook with
        # the conn id configured in Step 3
        hook = WasbHook(wasb_conn_id="wasb_default")
        # make sure the file_id is unique, either by using combinations of
        # the task_id, run_id and map_index parameters or by using a uuid
        filename = "data_" + str(uuid.uuid4()) + ".json"
        # define the full blob key where the file should be stored
        blob_key = f"{run_id}/{task_id}/{filename}"

        with NamedTemporaryFile(mode="w", suffix=".json") as tmp:
            json.dump(value, tmp)
            tmp.flush()
            # write the value to a local temporary JSON file

            file_size = os.stat(tmp.name).st_size

            if file_size >= CustomXComBackendJSON.MAX_FILE_SIZE_BYTES:
                raise AirflowException(
                    "Allowed file size is %s (bytes). Given file size is %s. ",
                    CustomXComBackendJSON.MAX_FILE_SIZE_BYTES,
                    file_size,
                )

            # load the local JSON file into Azure Blob Storage
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
    def deserialize_value(result):
        # retrieve the relevant reference string from the metadata database
        reference_string = BaseXCom.deserialize_value(result=result)
        # create the Wasb connection using the WasbHook and recreate the key
        hook = WasbHook(wasb_conn_id="wasb_default")
        blob_key = reference_string.replace(CustomXComBackendJSON.PREFIX, "")
        # download the JSON file found at the location described by the



        # reference string to my_xcom.json

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

            output = json.load(temp)
            
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
