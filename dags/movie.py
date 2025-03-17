from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)
from airflow.utils.trigger_rule import TriggerRule
import os


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
    end_date=datetime(2024, 1, 2),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS =["git+https://github.com/Jacob-53/movie.git@0.4.1"]
    BASE_DIR = "/home/jacob/data/movies/dailyboxoffice"

    def branch_fun(ds_nodash):
        import os
        check_path =os.path.expanduser(f"{BASE_DIR}/dt={ds_nodash}")
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

    def common_get_data(ds_nodash ,BASE_DIR= "/home/jacob/data/movies/dailyboxoffice",partitions=['dt'],url_param={}):
        from movie.api.call import call_api,list2df,save_df
        data=call_api(ds_nodash,url_param)
        df=list2df(data,ds_nodash,url_param)
        sv=save_df(df,BASE_DIR,partitions,url_param)
        return sv
        #print(ds_nodash,url_param,partition)
    
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn":"Y"},"ds_nodash":"{{ds_nodash}}"}
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"multiMovieYn":"N"},"ds_nodash":"{{ds_nodash}}"}
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"repNationCd":"K"},"ds_nodash":"{{ds_nodash}}"}
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{"repNationCd":"F"},"ds_nodash":"{{ds_nodash}}"}
    )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={"url_param":{},"ds_nodash":"{{ds_nodash}}"}
    )

    rm_dir = BashOperator(task_id='rm.dir',
                          bash_command=f"bash rm -rf {BASE_DIR}+/dt={{ds_nodash}}"
                         
    )

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