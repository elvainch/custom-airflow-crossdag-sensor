from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
from typing import List, Optional, Union
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.db import provide_session
from sqlalchemy.orm.session import Session
from time import sleep
from airflow.exceptions import AirflowSensorTimeout, AirflowSkipException


""" 
This sensor is for checking latest status of dag. If its success then only it will return true or mark 
as completed so next task will get execute.
In case dag is failed it will raise an airflow exception as failed.
params:
dag_name: pass the dag_id for your dag
status_to_check: pass 'success' if you want sensor to return true for this.
for e.g. 
dag_status_sensor = DagStatusSensor(dag_name='test_dag',status_to_check='success',task_id='dag_status_sensor',poke_interval=30,timeout=1800,dag=dag)
"""

class DagStatusSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,dag_name,status_to_check, run_to_check=-1,*args,**kwargs):
        self.dag_name = dag_name
        self.status_to_check = status_to_check
        self.run_to_check = run_to_check

        super(DagStatusSensor,self).__init__(*args,**kwargs)

    def poke (self,context):

        task_instance = context['task_instance']
        start = timezone.make_aware(datetime.now().replace(hour=0,minute=0,second=0, microsecond=0))
        end = timezone.make_aware(datetime.now().replace(hour=23,minute=59,second=59, microsecond=0))
        daggies = MyDagRun.find(dag_id=self.dag_name, execution_start_date=start, execution_end_date=end)

        if(daggies):
            length = len(daggies)
            if(length>0):
                if daggies[self.run_to_check].state == self.status_to_check:
                    task_instance.xcom_push("status", "successful")
                    return True
                elif daggies[self.run_to_check].state == "failed":
                    task_instance.xcom_push("status", "failure")
                    return False
                else:#if state is running or else. After timeout will raise exception
                    task_instance.xcom_push("status", "failure")
                    return False
            else:#if no runs of sensed dag, after timeout will raise exception and fail
                task_instance.xcom_push("status", "failure")
                return False
        else:
            task_instance.xcom_push("status", "failure")
            return False

    def execute(self, context):
        started_at = timezone.utcnow()
        while not self.poke(context):
            if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                if self.soft_fail and not context['ti'].is_eligible_to_retry():
                    raise AirflowSkipException('Snap. Time is OUT.')
                else:
                    raise AirflowSensorTimeout('Snap. Time is OUT.')
            sleep(self.poke_interval)
        self.log.info("Success criteria met. Exiting.")


class MyDagRun(DagRun):

    @staticmethod
    @provide_session
    def find(
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        state: Optional[str] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: bool = False,
        session: Session = None,
        execution_start_date: Optional[datetime] = None,
        execution_end_date: Optional[datetime] = None,
    ) -> List[DagRun]:

        DR = MyDagRun

        qry = session.query(DR)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(DR.dag_id.in_(dag_ids))
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(DR.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(DR.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(DR.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))

        return qry.order_by(DR.execution_date).all()

