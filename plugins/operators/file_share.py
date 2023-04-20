from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook
from airflow.utils.context import Context
from typing import Any
import os
from t import HTML, YAML, JSON


class FileShareOperator(BaseOperator):
    def __init__(
        self,
        *,
        azure_fileshare_conn_id: str = "azure-fileshare-default",
        share_name: str,
        directory_name: str,
        file_name: str,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.azure_fileshare_conn_id = azure_fileshare_conn_id
        self.share_name = share_name
        self.directory_name = directory_name
        self.file_name = file_name

    def execute(self, context: Context) -> HTMLXcom | Any:
        hook = AzureFileShareHook(azure_fileshare_conn_id=self.azure_fileshare_conn_id)
        conn = hook.get_conn()

        if conn.exists(self.share_name, self.directory_name, self.file_name):
            result = conn.get_file_to_text(
                self.share_name, self.directory_name, self.file_name
            )

            file_extension = os.path.splitext(result.name)[1]

            # We can also use subclasses, but this is fine for now.
            if file_extension == ".html":
                return HTML(html_string=result.content)
            if file_extension == ".yaml" or ".yml":
                return YAML(values=result.content)
            if file_extension == ".json":
                return JSON(values=result.content)

            raise Exception("File extension not supported")

        raise FileNotFoundError("No file named %s", self.file_name)
