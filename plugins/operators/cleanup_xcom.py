import logging
from datetime import datetime

from sqlalchemy import func

from airflow.exceptions import AirflowException
from airflow.models import XCom
from airflow.models.baseoperator import BaseOperator
from airflow.utils.session import provide_session


class XComOperator(BaseOperator):
    """
    Deletes every XCOM older than 7 days in the metadata database.

    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @provide_session
    def execute(self, context, session=None):
        if not session:
            session = context["session"]

        ts = context["ts"]
        ts_datetime = datetime.fromisoformat(ts).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        rows_deleted = (
            session.query(XCom)
            .filter(func.extract("days", ts_datetime - func.DATE(XCom.timestamp)) >= 7)
            .delete(synchronize_session=False)
        )

        if not rows_deleted:
            raise AirflowException("Could not delete any XCOM")

        session.commit()
        logging.info("Rows affected: %s" % (rows_deleted))
