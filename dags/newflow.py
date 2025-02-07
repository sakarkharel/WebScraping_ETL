from  datetime import datetime , timedelta
from airflow import DAG
from scrape_etl import extract, author_bio, go_through, join, clean

#It is a task operator , allowing to execute arbitraty python  functions or callable objects 
from airflow.operators.python import PythonOperator
#default_arg support note = https://medium.com/@muratozcann/using-default-args-in-airflow-make-your-workflows-more-efficient-3be192a584f2



#parameter definition 
default_args = {
    'owner':'airflow',
    'depends_on_past':False, 
    'start_date': datetime(2025,2,6),
    'end_date': datetime(2025,2,7),
    'catch_up': False,
}


dag = DAG(
    'simple_etl',
    description = 'A simple ETL pipeline', 
    default_args = default_args, 
    schedule=timedelta(days=1),
    catchup = False
    
)
run_etl1 = PythonOperator(
    task_id = 'scraping_data1',
    python_callable = extract, 
    dag=dag
)


run_etl2 = PythonOperator(
   task_id = 'scraping_data2', 
   python_callable= author_bio,
   dag=dag
)

run_etl3 = PythonOperator(
    task_id = 'scraping_data3',
    python_callable= go_through,
    dag=dag 
)

run_etl4 = PythonOperator(
    task_id = 'scraping_data4',
    python_callable = join,
    dag=dag
)

run_etl5 = PythonOperator(
    task_id = 'transforming_data5',
    python_callable = clean,
    dag = dag
)

run_etl1 >> run_etl2 >> run_etl3 >> run_etl4 >> run_etl5

