"""Example pipeline.

This pipeline is composed of the 3 common tasks of a data project:
- Ingestion
- Transformation
- Exposition

Several actions are performed in these tasks:
- Load application configuration
- Create a spark session
- Read, preprocess & write data using repository
- Transform data using domain functions
- Write output to Oracle
- Send an execution email

These 3 tasks are orchestrated using Airflow.
Please see the corresponding DAG in $UNXAPPLI/airflow/brc14_app00_demo.py.

"""
import os
import logging
import datetime
import argparse
import pandas as pd

from app_template.configuration import app, data, mail, spark_config
from app_template.infra.oracle_database import OracleDatabase
from app_template.infra.repositories import flow_repository
from app_template.domain import kpi
from app_template.interface import notification


TEMP_DIRPATH = os.path.join(os.environ["UNXDATA"], "temp")


def run_ingestion():
    """Run the ingestion pipeline."""
    # Configurations
    app.AppConfig()
    mailer = mail.Mailer()

    logging.info("*** Starting application pipeline ***")
    try:
        # Create a spark session
        _, spark_session = spark_config.get_spark(app_name="app_template")

        # Create temp dirpath
        if not os.path.exists(TEMP_DIRPATH):
            os.makedirs(TEMP_DIRPATH)

        # Read, preprocess & write production flow data
        flow_repo = flow_repository.FlowRepository(spark_session, TEMP_DIRPATH)
        flow_repo.read(layer="raw", preprocessing=True)
        flow_repo.write()

    except Exception as e:
        logging.error(e)
        # Error email
        notification.send_email(mailer, "error", exception=e)


def run_transformation():
    """Run the transformation pipeline."""
    # Configurations
    app.AppConfig()
    data_config = data.DataConfig()
    mailer = mail.Mailer()

    try:
        # Create a spark session
        _, spark_session = spark_config.get_spark(app_name="app_template")

        # Read, preprocess & write production flow data
        flow_repo = flow_repository.FlowRepository(spark_session, TEMP_DIRPATH)
        flow_repo.read(layer="standard")

        # Compute production KPIs
        kpis_production = kpi.compute_n_vins(
            flow_repo.df, ["site_code", "faml_grp_labl", "smon_date"]
        )

        # Format date
        kpis_production["smon_date"] = pd.to_datetime(
            kpis_production["smon_date"],
            format="%Y-%m-%d"
        )

        # Write KPIs
        kpis_table_name = data_config.infra_config["kpis"]["table_name"]
        kpis_filepath = os.path.join(TEMP_DIRPATH, f"{kpis_table_name}.csv")
        kpis_production.to_csv(kpis_filepath, index=False)

    except Exception as e:
        logging.error(e)
        # Error email
        notification.send_email(mailer, "error", exception=e)


def run_exposition():
    """Run the exposition pipeline."""
    # Configurations
    app.AppConfig()
    data_config = data.DataConfig()
    mailer = mail.Mailer()

    try:
        # KPIs table name
        kpis_table_name = data_config.infra_config["kpis"]["table_name"]

        # Read KPIs
        kpis_filename = os.path.join(TEMP_DIRPATH, f"{kpis_table_name}.csv")
        kpis_production = pd.read_csv(kpis_filename)

        # Execution KPIs
        execution_kpis = {}
        execution_kpis["Total number of vehicles produced"] = kpis_production['n_vin'].sum()

        # Write KPIs to Oracle
        logging.info(f"Writing production KPIs to {kpis_table_name}")
        oracle_db = OracleDatabase("cx_oracle")
        oracle_db.write_df_to_oracle(
            kpis_production,
            kpis_table_name,
            mode="overwrite"
        )

        # Store data
        current_date = datetime.datetime.now()
        storage_dirpath = os.path.join(
            os.environ["UNXHISTO"],
            f"year={current_date.year}",
            f"month={current_date.month:02d}",
            f"day={current_date.day:02d}"
        )
        if not os.path.exists(storage_dirpath):
            os.makedirs(storage_dirpath)
        os.system(f"mv {TEMP_DIRPATH}/* {storage_dirpath}")

        # Success email
        notification.send_email(
            mailer,
            "success",
            kpis_table_name=kpis_table_name,
            execution_kpis=execution_kpis
        )

        logging.info("*** Application pipeline finished ***")

    except Exception as e:
        logging.error(e)
        # Error email
        notification.send_email(mailer, "error", exception=e)


if __name__ == "__main__":
    # Parse the cli arguments
    parser = argparse.ArgumentParser(description="Application pipeline")
    parser.add_argument("task", help="specific pipeline task to run")
    args = parser.parse_args()

    if args.task == "ingestion":
        run_ingestion()
    elif args.task == "transformation":
        run_transformation()
    elif args.task == "exposition":
        run_exposition()
    else:
        raise ValueError("task should be 'ingestion', 'tranformation' or 'exposition'")
