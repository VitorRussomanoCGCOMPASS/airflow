from airflow.utils.decorators import apply_defaults
from sqlalchemy.orm import sessionmaker, Session
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from typing import Union
from sqlalchemy import create_engine

def get_session(conn_id: str) -> Session:
    """

    Parameters
    ----------
    conn_id : str
        _description_

    Returns
    ------- 
    Session

    """    
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    return sessionmaker(bind=engine)()


class SQLAlchemyOperator(PythonOperator):
    """
    PythonOperator with SQLAlchemy session management - creates session for the Python callable
    and commit/rollback it afterwards.

    Set `conn_id` with you DB connection.

    Pass `session` parameter to the python callable.
    """

    @apply_defaults
    def __init__(self, conn_id: str , *args, **kwargs):
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)

    def execute_callable(self):
        session = get_session(self.conn_id)
        try:
            result = self.python_callable(
                *self.op_args, session=session, **self.op_kwargs
            )
        except Exception as exc:
            session.rollback()
            raise exc
        
        session.commit()
        return result

