

from datetime import datetime, timedelta
from dateutil.parser import parse
import pandas as pd
import pymysql

host = "localhost"
port = 3306
db = "panadol"
user = "root"
password = "panadol"

def parse_ts(ts):
    return parse(ts).strftime("%Y/%m/%d")


# def all_process(dag_id, start_date, schedule_interval, default_args, project_name, project_version):
#     with DAG(dag_id=dag_id, start_date=start_date, default_args=default_args, schedule_interval=schedule_interval,
#              user_defined_filters={'parsets': parse_ts}) as dag:

# host = 'fdbr-prod.cif0p85z2xpg.ap-southeast-1.rds.amazonaws.com'
# port = 3306
# db = "fdbr"
# user = "serverteam"
# password = "DDKW31Kr31"

# q1 = 'SELECT * FROM nubr_review_products limit 10'
# start_date = ''
# end_date = ''
q1 = "SELECT * from nubr_reviews limit 10;"
q2 = "select * from nubr_products limit 10;"
# q3 = "select * from nubr_categories"
# q4 = "select * from nubr_brands"
q5 = "q1,q2 where nubr_reviews.review_date between '2019-03-26z 00:00:000' and '2019-03-26 23:59:59'"
#'{lt}' and '{gt}'".format(lt='2013-09-13 00:00:00', gt='2013-12-10 23:59:59')
#  get_nubr_reviewer = PythonOperator(
#             task_id='get_nubr_reviewer',
#             python_callable=query_to_rds,
#             op_kwargs={
#                 'host': host,
#                 'user': user,
#                 'port': port,
#                 'password': password,
#                 'dbname': db,
#                 'query': q1,
#             },
#             trigger_rule=TriggerRule.ALL_SUCCESS,
#             xcom_push=True,
#             provide_context=True,
#             dag=dag
#         )
bucket_name = 'dev-tester-collector'

#         upload_to_s3 = PythonOperator(
#             task_id='test_bucket',
#             python_callable=upload_file,
#             templates_dict={
#                 'parse_dt': "{{ (ts | parsets) }}",
#                 'query_values': "{{ ti.xcom_pull(task_ids='get_nubr_reviewer') }}",
#                 'connection_name': connection_name,
#                 'bucket_name': bucket_name
#             },
#             trigger_rule=TriggerRule.ALL_SUCCESS,
#             provide_context=True,
#             dag=dag
#         )

#         end = DummyOperator(
#             trigger_rule=TriggerRule.ALL_SUCCESS,
#             task_id='end'
#         )

#         get_nubr_reviewer >> upload_to_s3 >> end

#         return dag


conn = pymysql.connect(host, user=user, port=port,
                    passwd=password, db=db)
df1 = pd.read_sql(q1, con=conn)
# df2 = pd.read_sql(q3, con=conn)
# df3 = pd.read_sql(q4, con=conn)


print(df1)

# json_newline_delimited = ""
# for i in df.index:
#     json_newline_delimited += df.loc[i].to_json() + '\n'
