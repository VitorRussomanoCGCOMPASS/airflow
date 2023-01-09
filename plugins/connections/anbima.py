from airflow.models.connection import Connection
from dotenv import load_dotenv
import os

load_dotenv()

ANBIMA_CLIENT_ID = os.getenv("ANBIMA_CLIENT_ID")

# FIXME:  THE LOAD_DOTENV DOES NOT WORK.
# TODO : EASIER TO jUST MOVE THE CONNECTION TO THE AIRFLOW DATABASE.



ANBIMA_CONNECTION = Connection(
    conn_id="anbima_api",
    conn_type="http",
    description=None,
    login="mL8an5sznCN3",
    password=None,
    host="api.anbima.com.br",
    port=443,
    schema="https",
)
