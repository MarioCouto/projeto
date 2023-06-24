from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0)
}

with DAG(
    'projeto_acidentes_aereos',
    default_args=default_args,
    description='Projeto 01 - Acidentes Aéreos',
    schedule_interval=timedelta(days=1)
) as dag:
    
    Inicio = EmptyOperator(task_id= 'Inicio')
    Fim = EmptyOperator(task_id= 'Fim')

    wf_projeto = BashOperator(
        task_id='wf_projeto',
        bash_command='sh /Applications/Apache/apache-hop/hop-run.sh --project="Projeto Fia" --file "/Applications/Apache/apache-hop/config/projects/projeto/workflow/wf_acidentes_aereos.hwf"',
        dag=dag
    )


Inicio >> wf_projeto >> Fim