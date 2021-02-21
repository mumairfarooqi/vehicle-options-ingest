from airflow import DAG
from airflow.models import Variable
from airflow.utils import dates

from datetime import timedelta
from vehicle_options_ingest.files import config as file_configurations

from vehicle_options_ingest.helpers import GcstoGcsWithDestFilename, CSVtoJsonOperator
from vehicle_options_ingest.helpers import read_query

from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator

# Args to be provided as default
args = {
    "start_date": dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Dict of vars to be used through out the DAG processing
variables = {
    "local_dir": Variable.get(
        "local_storage_dir"
    ),  # Local directory inside airflow environment.
    "datalake": Variable.get(
        "datalake_project"
    ),  # GCP Project inside which the dataset and tables reside.
    "airflow_home_bucket": Variable.get(
        "airflow_home_bucket"
    ),  # Bucket created and used by Airflow for DAGs.
    "input_bucket": "umair_playground",  # The bucket where csv files are arriving.
}

with DAG(
    "vehicle-options-ingest",  # Presents the name of DAG inside Airflow Env
    default_args=args,  # Providing default args here for each task in DAG
    schedule_interval="@once",  # Scheduling it only once in airflow since the etl process is not recurring.
    max_active_runs=1,
) as dag:
    staging_directory = variables["local_dir"] + "/tempfiles/vehicle_options_ingest/"

    # Creating working directory using the out of the box bash operator
    create_staging_dir = BashOperator(
        task_id=f"create-staging-dir", bash_command=f"mkdir -p {staging_directory}"
    )
    # Deleting the working dir
    cleanup_staging_dir = BashOperator(
        task_id=f"cleanup-staging-dir", bash_command=f"rm -rf {staging_directory}"
    )

    # Below loop iterates through each of the dicts inside the list. i.e. Iterates over files to be ingested into BQ
    for config in file_configurations:

        filename = f"{config['base_filename']}"

        # Out of the box Sensor for checking inside the gcs bucket against the prefix provided.
        file_sensor = GoogleCloudStoragePrefixSensor(
            task_id=f"wait-for-{filename}",
            bucket=f'{variables["input_bucket"]}',
            prefix=filename,
            timeout=10000,
            poke_interval=2000,
            mode="reschedule",
        )

        """
        Once the above task is finished and sensor has verified the file, then below custom op will copy the files to 
        staging dir inside airflow bucket.
        """
        copy_file_to_staging = GcstoGcsWithDestFilename(
            task_id=f"copy-{filename}-to-staging",
            source_bucket=variables["input_bucket"],
            source_object=filename + ".csv",
            destination_bucket=variables["airflow_home_bucket"],
            destination_object="data/tempfiles/vehicle_options_ingest/"
            + filename
            + ".csv",
        )

        """
        Once the above task is finished and file is now inside the Staging dir, then below custom op will convert
        the csv to multiline json with the csv header becoming the keys for the json elements.
        Achieving this task makes it easier and agile to bring in new data sources on the fly since it is not 
        necessary to define the schema before storing the data. This concept is called Schema on Read.
        More can be read at: https://www.delltechnologies.com/en-us/blog/schema-read-vs-schema-write-started/
        """

        convert_to_json = CSVtoJsonOperator(
            task_id=f"convert-to-json-{filename}",
            input_filename=staging_directory + filename + ".csv",
            output_filename=staging_directory + filename + ".mjson",
        )

        """
        Once the above task is finished and mjson file has now been created, the below operator would dump the whole 
        Unstructured Json element as a string inside the Bq Raw Layer.
        The schema can be defined later on top of this raw json field.
        """

        load_to_raw = GoogleCloudStorageToBigQueryOperator(
            task_id=f"{filename}-gcs-to-raw",
            bucket=variables["airflow_home_bucket"],
            source_objects=[
                "data/tempfiles/vehicle_options_ingest/" + filename + ".mjson"
            ],
            destination_project_dataset_table=variables["datalake"]
            + ".airflow_playground."
            + filename
            + "_raw",
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
            schema_fields=[{"name": "content", "type": "STRING", "mode": "REQUIRED"}],
            create_disposition="CREATE_IF_NEEDED",
        )

        """
        The helper method that takes the relative path in order to resolve the absolute path to parse the .sql file.
        """

        raw_to_structured_sql = read_query(
            f"dags/vehicle_options_ingest/sql/raw_to_structured/{filename}.sql"
        )

        """
        This operator takes the sql from previous var as an input and executes it on the BigQuery with the 
        result set being written inside the destination dataset table. This is where the relevant schema gets created
        before writing into the table. (Schema on Write approach)
        """
        raw_to_structured = BigQueryOperator(
            location="EU",
            task_id=f"{filename}-raw-to-structured",
            use_legacy_sql=False,
            allow_large_results=True,
            destination_dataset_table=f'{variables["datalake"]}.airflow_playground.'
            + filename
            + "_structured",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            sql=raw_to_structured_sql,
        )

        raw_to_error_sql = read_query(
            f"dags/vehicle_options_ingest/sql/raw_to_error/{filename}.sql"
        )

        """
        This operator has been mentioned above also but only difference is that this time the destination table gets 
        populated with the erroneous records. (Data Validation Layer). It will dump the error recs with the reason
        for error as well splitted by Pipe '|' sign.
        """

        raw_to_error = BigQueryOperator(
            location="EU",
            task_id=f"{filename}-raw-to-error",
            use_legacy_sql=False,
            allow_large_results=True,
            destination_dataset_table=f'{variables["datalake"]}.airflow_playground.'
            + filename
            + "_error",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            sql=raw_to_error_sql,
        )

        structured_to_transformation_sql = read_query(
            "dags/vehicle_options_ingest/sql/transformation/production_cost_and_profit_calc.sql"
        )

        """
        This is where the actual transformation is happening based upon the requirements given by analyst in our team
        to enrich the base dataset with the production costs in order to calculate the profit.
        """

        structured_to_transformation = BigQueryOperator(
            location="EU",
            task_id="structured-to-transformation",
            use_legacy_sql=False,
            allow_large_results=True,
            destination_dataset_table=f'{variables["datalake"]}.airflow_playground.'
            + "production_cost_and_profit_calc",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            sql=structured_to_transformation_sql,
        )

        """
        Below given is the orchestration of the dependencies inside DAG. It would tell the DAG about the sequence of
        execution of tasks.
        """

        create_staging_dir >> file_sensor >> copy_file_to_staging >> convert_to_json >> load_to_raw
        load_to_raw >> raw_to_structured >> structured_to_transformation >> cleanup_staging_dir
        load_to_raw >> raw_to_error
