import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator
from datetime import timedelta

def post_to_bigquery():
    import pandas as pd
    import redis
    import logging
    import json
    from google.cloud import bigquery
    from google.oauth2 import service_account
    
    log = logging.getLogger(__name__)
    
    def btod(b):
        return b.decode("utf-8")

    try:
        r = redis.Redis(host='redis-cours', port=6379, db=0)
        r.ping() 
        log.info("Connexion à Redis réussie.")
    except redis.ConnectionError as e:
        log.error(f"Erreur de connexion à Redis : {e}")
    
    post_ids = r.keys()
    log.info(f"Keys post_ids got {post_ids}")
    aggregation_post = {}

    for key in post_ids:
        post_redis = r.get(btod(key))
        post_str = btod(post_redis)
        post_dict = json.loads(post_str)
        
        if post_dict.get("@OwnerUserId") is not None:
            user_id, view_count = post_dict["@OwnerUserId"], 0
            if post_dict.get("@ViewCount") is not None:
                view_count = int(post_dict["@ViewCount"])
                
            if aggregation_post.get(user_id) is not None:
                aggregation_post[user_id]["nb_post"] += 1
                aggregation_post[user_id]["total_view"] += view_count
            else:
                aggregation_post[user_id] = {"user_id": user_id, "nb_post": 1, "total_view": view_count}
                
    log.info(f"Aggregation posts : {aggregation_post}")
    key_path = "./data/movies-stackexchange/json/service-account.json"
    credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    
    aggregation_list = [(v["user_id"], v["nb_post"], v["total_view"]) for v in aggregation_post.values()]
    log.info(f"Agtype : {type(aggregation_list)}, values : {aggregation_list}")
    
    df = pd.DataFrame(aggregation_list, columns=['user_id', 'nb_post', 'total_view'])
    table = 'movieStackExchange.post_view'
    
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Load data to BigQuery table
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()

with DAG(
    dag_id="DAG_post_to_bigquery",
    schedule=timedelta(seconds=20),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    virtual_classic = PythonVirtualenvOperator(
        task_id="post_to_bigquery",
        requirements=["google-cloud-bigquery"],
        python_callable=post_to_bigquery,
    )
