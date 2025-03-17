from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    'movie',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS = []
    BASE_DIR = "~/data/movies/dailyboxoffice"

    def branch_fun(ds_nodash):
        import os
        if os.path.exists(f"{BASE_DIR}/dt={ds_nodash}"):
            return rm_dir.task_id
        else:
            return "get.start","echo.task"    

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )
    
    def fn_merge_data(ds_nodash):
        print(ds_nodash)
        
    
        
    merge_data = PythonVirtualenvOperator(
        task_id='merge.data',
        python_callable=fn_merge_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
    )

    def common_get_data(ds_nodash,url_param):
        print(ds_nodash,url_param)
    
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn":"Y"}}
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn":"N"}}
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"repNationCd":"K"}}
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=[REQUIREMENTS],
        op_kwargs={"url_param":{"repNationCd":"F"}}
    )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=[REQUIREMENTS],
        op_kwargs={"url_param":{}}
    )

    rm_dir = BashOperator(task_id='rm.dir',
                          bash_command='rm -rf $BASE_DIR/dt={{ ds_nodash }}',
                          env={'BASE_DIR': BASE_DIR})

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    get_start = EmptyOperator(task_id='get.start', trigger_rule=TriggerRule.ALL_DONE)
    get_end = EmptyOperator(task_id='get.end')
    

    start >> branch_op

    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [multi_y, multi_n, nation_k, nation_f, no_param] >> get_end

    get_end >> merge_data >> end