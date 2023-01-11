from airflow.models.connection import Connection
from dotenv import load_dotenv
import os

load_dotenv()
BRITECH_ID = os.getenv('BRITECH_API_ID')
BRITECH_PASS = os.getenv('BRITECH_API_PASS')


# COMPLETE : MOVE IT TO THE SYSTEM.

BRITECH_CONNECTION = Connection(
    conn_id = 'britech_api',
    conn_type='http',
    description=None,
    login=BRITECH_ID,
    password=BRITECH_PASS,
    host='saas.britech.com.br/compass_ws/api',
    schema='https'

)