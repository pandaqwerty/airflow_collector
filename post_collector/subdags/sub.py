from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks import S3_hook

from datetime import datetime, timedelta
from dateutil.parser import parse
import pandas as pd
import pymysql


def parse_ts(ts):
    print(parse(ts).strftime("%Y/%m/%d %H:%M:%S"))
    print((parse(ts) + timedelta(hours=7)).strftime("%Y/%m/%d %H:%M:%S"))
    return parse(ts) + timedelta(hours=7)

def parse_min_ts(ts):
    return datetime.combine(parse(ts), datetime.min.time())


def parse_max_ts(ts):
    return datetime.combine(parse(ts), datetime.max.time())


def all_process(dag_id, start_date, schedule_interval, default_args):
    with DAG(dag_id=dag_id, start_date=start_date, default_args=default_args, schedule_interval=schedule_interval,
             user_defined_filters={'parsets': parse_ts, 'mints': parse_min_ts, 'maxts': parse_max_ts}) as dag:

        # host = "localhost"
        # port = 3306
        # db = "fdbr_local"
        # user = "willioktavega"
        # password = "willioktavega"

        host = 'fdbr-prod.cif0p85z2xpg.ap-southeast-1.rds.amazonaws.com'
        port = 3306
        db = "fdbr"
        user = "serverteam"
        password = "DDKW31Kr31"

        connection_name = 'aws_conn_s3'
        bucket_name = 'dev-data-collector'

        # q1 = 'SELECT * FROM nubr_post limit 10'
        # start_date = ''
        # end_date = ''
        q1 = "select * from nubr_post;"\
             "inner join nubr_reviewer on nubr_post.rvwr_id = nubr_reviewer.rvwr_id where created_at != '0000-00-00 00:00:00' order by rvwr_post_date"\
             "between \'{{ (ts | mints).strftime('%Y-%m-%d %H:%M:%S') }}\' " \
             "and \'{{ (ts | maxts).strftime('%Y-%m-%d %H:%M:%S') }}\' "
        #'{lt}' and '{gt}'".format(lt='2013-09-13 00:00:00', gt='2013-12-10 23:59:59')

        get_nubr_post = PythonOperator(
            task_id='get_nubr_post',
            python_callable=query_to_rds,
            templates_dict={
                'host': host,
                'user': user,
                'port': port,
                'password': password,
                'dbname': db,
                'query': q1,
                'parse_dt': "{{ (ts | mints).strftime('%Y/%m/%d') }}",
                'connection_name': connection_name,
                'bucket_name': bucket_name,
                'start_date': "{{ (ts | mints).strftime('%Y-%m-%d %H:%M:%S') }}",
                'end_date': "{{ (ts | maxts).strftime('%Y-%m-%d %H:%M:%S') }}"
            },
            trigger_rule=TriggerRule.ALL_SUCCESS,
            xcom_push=True,
            provide_context=True,
            dag=dag
        )

        connection_name = 'aws_conn_s3'
        bucket_name = 'dev-data-collector'

        # upload_to_s3 = PythonOperator(
        #     task_id='test_bucket',
        #     python_callable=upload_file,
        #     templates_dict={
        #         'parse_dt': "{{ (ts | parsets) }}",
        #         'query_values': "{{ ti.xcom_pull(task_ids='get_nubr_post') }}",
        #         'connection_name': connection_name,
        #         'bucket_name': bucket_name
        #     },
        #     trigger_rule=TriggerRule.ALL_SUCCESS,
        #     provide_context=True,
        #     dag=dag
        # )

        end = DummyOperator(
            trigger_rule=TriggerRule.ALL_SUCCESS,
            task_id='end'
        )

        get_nubr_post >> end

        return dag



# def upload_file(**context):
#     print(context['templates_dict']['parse_dt'])
#     # print(context['templates_dict']['query_values'])
#     hook = S3_hook.S3Hook(context['templates_dict']['connection_name'])
#     hook.load_string(string_data=context['templates_dict']['query_values'],
#                      key="{base}/{dt}/{file}".format(base="post",
#                                                      dt=context['templates_dict']['parse_dt'],
#                                                      file='data_190326.json'),
#                      bucket_name=context['templates_dict']['bucket_name'], replace=True)
                     
def query_to_rds(**context):
    conn = pymysql.connect(context['templates_dict']['host'], user=context['templates_dict']['user'], port=context['templates_dict']['port'],
                           passwd=context['templates_dict']['password'], db=context['templates_dict']['dbname'])
    df = pd.read_sql(context['templates_dict']['query'], con=conn)

    print(context['templates_dict']['parse_dt'])
    print('Check start date query:', context['templates_dict']['start_date'])
    print('Check end date query:', context['templates_dict']['end_date'])
    hook = S3_hook.S3Hook(context['templates_dict']['connection_name'])
    hook.load_string(string_data=df.to_csv(sep=",", index=False, line_terminator='\n', encoding='utf-8'),
                     key="{base}/{dt}/{file}".format(base="post_collector",
                                                     dt=context['templates_dict']['parse_dt'],
                                                     file='data.csv'),
                     bucket_name=context['templates_dict']['bucket_name'], replace=True)

    return True


# def query_to_rds(host, user, port, password, dbname, query, **kwargs):

#     conn = pymysql.connect(host, user=user, port=port,
#                            passwd=password, db=dbname)
#     df = pd.read_sql(query, con=conn)

#     return df.to_csv(sep=",", index=False, line_terminator='\n', encoding='utf-8')