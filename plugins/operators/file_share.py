from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook
from airflow.utils.context import Context


class FileShareOperator(BaseOperator):
    def __init__(
        self,
        *,
        conn_id: str = "azure_fileshare_default",
        share_name: str,
        directory_name: str,
        file_name: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.share_name = share_name
        self.directory_name = directory_name
        self.file_name = file_name

    def execute(self, context: Context):
        hook = AzureFileShareHook(azure_fileshare_conn_id=self.conn_id)
        conn = hook.get_conn()

        if conn.exists(self.share_name, self.directory_name, self.file_name):
            result = conn.get_file_to_text(
                self.share_name, self.directory_name, self.file_name
            )

            return result.content

        raise FileNotFoundError("No file named %s", self.file_name)
