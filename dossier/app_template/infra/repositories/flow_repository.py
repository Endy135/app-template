"""Example repository."""
import os
import logging
from datetime import datetime
from pyspark.sql import functions as F

from ...configuration.data import DataConfig
from ...utils import system


class FlowRepository(DataConfig):
    """Repository for production flow data collection."""

    def __init__(self, spark_session, temp_dirpath):
        """Initialize production flow repository."""
        DataConfig.__init__(self)
        self._spark_session = spark_session
        self._flow_config = self.infra_config["flow"]
        self._output_dirpath = system.get_hdfs_path(
            os.path.join(temp_dirpath, self._flow_config["output_filename"])
        )

    @property
    def df(self):
        """Return dataframe containing production flow data."""
        return self._df_flow

    def read(self, layer="raw", preprocessing=True):
        """Read production flow data."""
        if layer == "raw":
            logging.info(
                f"Reading production flow raw data from {self._flow_config['input_dirpath']}"
            )

            self._df_flow = (
                self._spark_session.read.parquet(self._flow_config["input_dirpath"])
                .filter(
                    F.col("site_code").isin(self._flow_config["perimeter"]["site_code"])
                )
                .filter(F.col("genr_door") == self._flow_config["perimeter"]["genr_door"])
                .filter(
                    F.col("pass_date")
                    >= datetime.strptime(
                        self._flow_config["perimeter"]["start_date"], "%Y-%m-%d"
                    )
                )
                .filter(
                    F.col("pass_date")
                    < datetime.strptime(
                        self._flow_config["perimeter"]["end_date"], "%Y-%m-%d"
                    )
                )
                .select(self._flow_config["columns"])
            )

            if preprocessing:
                self.preprocess()

        elif layer == "standard":
            logging.info(
                f"Reading production flow standard data from {self._output_dirpath}"
            )
            self._df_flow = (
                self._spark_session.read.parquet(self._output_dirpath)
            )

        else:
            raise ValueError("layer should be either 'raw' or 'standard'")

    def write(self):
        """Write production flow data."""
        logging.info(f"Writing production flow data to {self._output_dirpath}")
        self._df_flow.repartition(2).write.parquet(self._output_dirpath, "overwrite")

    def preprocess(self):
        """Preprocess production flow data."""
        # Remove inappropriate VINs
        self._df_flow = self._df_flow.filter(F.col("vin") != "                 ")

        # Remove duplicates
        self._df_flow = self._df_flow.distinct()

        # Create date column
        self._df_flow = self._df_flow.withColumn("smon_date", F.to_date("pass_date"))
