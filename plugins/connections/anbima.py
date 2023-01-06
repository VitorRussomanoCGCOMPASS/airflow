from airflow.models.connection import Connection

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