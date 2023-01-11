from airflow.models.connection import Connection
from dotenv import load_dotenv
import os

load_dotenv()
# we use env variables just as fall back to work outside the webserver,

ANBIMA_CLIENT_ID = os.getenv("ANBIMA_CLIENT_ID")

# COMPLETE:  THE LOAD_DOTENV DOES NOT WORK.
# COMPLETE: EASIER TO jUST MOVE THE CONNECTION TO THE AIRFLOW DATABASE.



ANBIMA_CONNECTION = Connection(
    conn_id="anbima_api",
    conn_type="http",
    description=None,
    login= ANBIMA_CLIENT_ID,
    password=None,
    host="api.anbima.com.br",
    port=443,
    schema="https",
)
