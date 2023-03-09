from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from typing import Union
from sqlalchemy.orm import scoped_session, sessionmaker



def get_session(conn_id: Union[str, None] = None, database: Union[str, None] = None):
    """

    Parameters
    ----------
    conn_id : str
        _description_

    Returns
    -------
    Session

    """
    hook = PostgresHook(postgres_conn_id=conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    return scoped_session(sessionmaker(engine))


class SQLAlchemyOperator(PythonOperator):
    """
    PythonOperator with SQLAlchemy session management - creates session for the Python callable
    and commit/rollback it afterwards.

    Set `conn_id` with you DB connection.

    Pass `session` parameter to the python callable.
    """

    @apply_defaults
    def __init__(
        self,
        conn_id: Union[str, None] = None,
        database: Union[str, None] = None,
        *args,
        **kwargs
    ) -> None:

        self.conn_id = conn_id
        self.database = database
        super().__init__(*args, **kwargs)

    def execute_callable(self):

        session = get_session(self.conn_id, self.database)

        try:
            result = self.python_callable(
                *self.op_args, session=session, **self.op_kwargs
            )
        except Exception as exc:
            session.rollback()
            raise exc

        session.commit()
        return result


class SQLAlchemyOperatorLocal(PythonOperator):
    """
    PythonOperator with SQLAlchemy session management - creates session for the Python callable
    and commit/rollback it afterwards.

    Set `conn_id` with you DB connection.

    Pass `session` parameter to the python callable.
    """

    @apply_defaults
    def __init__(
        self,
        conn_id: Union[str, None] = None,
        database: Union[str, None] = None,
        file_path: Union[str, None] = None,
        *args,
        **kwargs
    ) -> None:

        self.conn_id = conn_id
        self.database = database
        self.file_path = file_path
        super().__init__(*args, **kwargs)

    def execute_callable(self):

        session = get_session(self.conn_id, self.database)

        try:
            result = self.python_callable(
                *self.op_args,
                session=session,
                file_path=self.file_path,
                **self.op_kwargs
            )
        except Exception as exc:
            session.rollback()
            raise exc

        session.commit()
        return result
