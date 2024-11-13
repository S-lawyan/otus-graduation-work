import datetime
import pandas as pd
from pandas import DataFrame
from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.logging_config import log

from tempfile import gettempdir
from minio import Minio
from clickhouse_driver import Client


def get_files_from_minio(**kwargs) -> list[str]:
    '''
        Getting a list of files from minio bucket.
    :param kwargs:
    :return: list["minio_file_path"]
    '''

    minio = Minio(
        endpoint=kwargs["minio_endpoint"],
        access_key=kwargs["minio_access_key"],
        secret_key=kwargs["minio_secret_key"],
        secure=False
    )
    bucket_name = kwargs["bucket_name"]
    prefix = kwargs["input_dir"]

    objects = minio.list_objects(
        bucket_name=bucket_name,
        prefix=prefix,
        recursive=False
    )
    files = [obj.object_name for obj in objects]
    # TODO Получаем список всех файлов в input_dir, так как
    #      там могут лежать несколько файлов для персональной обработки.

    minio_filepath = files[0]

    return minio_filepath


def upload_data(minio_filepath: str, **kwargs) -> None:
    '''
        Uploading raw data to clickhouse.geo_ip.geolocation_flat.
    :param file: str - Path to the file
    :param kwargs:
    :return: None
    '''

    minio = Minio(
        endpoint=kwargs["minio_endpoint"],
        access_key=kwargs["minio_access_key"],
        secret_key=kwargs["minio_secret_key"],
        secure=False
    )

    bucket_name = kwargs["bucket_name"]
    temp_dir = gettempdir()

    filename = minio_filepath.split("/")[1]
    temp_filepath = f"{temp_dir}/{filename}"

    minio.fget_object(
        bucket_name=bucket_name,
        object_name=minio_filepath,
        file_path=temp_filepath
    )

    clickhouse = Client(
        host=kwargs["ch_server"],
        port=kwargs["ch_port"],
        user=kwargs["ch_user"],
        password=kwargs["ch_password"],
        connect_timeout=9999,
        secure=False,
    )

    for batch in pd.read_csv(
        filepath_or_buffer=temp_filepath,
        chunksize=kwargs["batch_size"],
        compression="gzip",
        # encoding="utf-8",
        sep=",",
        header=None,
        names=kwargs["columns"]
    ):
        batch: DataFrame
        batch['latitude'] = batch['latitude'].astype(str)
        batch['longitude'] = batch['longitude'].astype(str)

        result = clickhouse.insert_dataframe(
            query=f""" INSERT INTO geo_ip.{kwargs["ch_table"]} VALUES """,
            dataframe=batch,
            settings=dict(
                use_numpy=True,
            ),
        )

        if result:
            log.info(f"The upload of {result} rows to clickhouse has been completed successfully")


with DAG(
        dag_id="UPLOAD_GEO_IP_DATA",
        # description='',
        owner_links={'user13': 'https://www.youtube.com/'},
        start_date=datetime.datetime(2022, 2, 22),
        render_template_as_native_obj=True,
        max_active_tasks=5,
        max_active_runs=2,
        catchup=False,
        default_args={
            'owner': 'otus-gp',
            'depends_on_past': False,
            'retries': 5,
            'retry_delay': datetime.timedelta(seconds=10),
        },
        schedule='*/60 * * * *',
        params=dict(
            minio_endpoint="5.35.88.23:9010",
            minio_access_key="otus-airflow-key",
            minio_secret_key="inbmOFb7OmmijkeTDlxl3LNKqA8ZX1lGRfu5FyUl",
            bucket_name="data",
            input_dir="geo_ip/",
            ch_server="5.35.88.23",
            ch_port="9000",
            ch_user="default",
            ch_password="admin",
            ch_table="geolocation_flat",
            columns=[
                "ip_range_start",
                "ip_range_end",
                "region",
                "country_code",
                "district",
                "city",
                "latitude",
                "longitude",
            ],
            batch_size=100000,
        ),
) as dag:

    get_files = PythonOperator(
        task_id='get_files',
        python_callable=get_files_from_minio,
        op_kwargs=dict(
            minio_endpoint="{{ params.minio_endpoint }}",
            minio_access_key="{{ params.minio_access_key }}",
            minio_secret_key="{{ params.minio_secret_key }}",
            bucket_name="{{ params.bucket_name }}",
            input_dir='{{ params.input_dir }}',
            columns="{{ params.columns }}",
            batch_size="{{ params.batch_size }}"
        )
    )

    upload = PythonOperator(
        task_id='upload',
        python_callable=upload_data,
        op_kwargs=dict(
            ch_server="{{ params.ch_server }}",
            ch_port="{{ params.ch_port }}",
            ch_table="{{ params.ch_table }}",
            ch_user="{{ params.ch_user }}",
            ch_password="{{ params.ch_password }}",
            minio_endpoint="{{ params.minio_endpoint }}",
            minio_access_key="{{ params.minio_access_key }}",
            minio_secret_key="{{ params.minio_secret_key }}",
            bucket_name="{{ params.bucket_name }}",
            columns="{{ params.columns }}",
            batch_size="{{ params.batch_size }}",
            minio_filepath=get_files.output
        )
    )

    get_files >> upload

if __name__ == '__main__':
    dag.test()
