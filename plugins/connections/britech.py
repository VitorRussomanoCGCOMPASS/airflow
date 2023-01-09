from airflow.models.connection import Connection

# TODO : MOVE IT TO THE SYSTEM.

BRITECH_CONNECTION = Connection(
    conn_id = 'britech_api',
    conn_type='http',
    description=None,
    login='webservice',
    password='Compass@1',
    host='saas.britech.com.br/compass_ws/api',
    schema='https'

)