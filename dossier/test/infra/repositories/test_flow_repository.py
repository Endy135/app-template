"""Unit tests of the FlowRepository class."""
import os
import pytest
import pandas as pd
from pyspark.sql import functions as F
from pandas.testing import assert_series_equal

from app_template.infra.repositories.flow_repository import FlowRepository


flow_dirpath = "file:///" + os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "resources", "flow.csv"
    )
)


@pytest.fixture
def flow_repo(spark_session):
    """Return flow repository."""
    # Instantiate flow repository
    flow_repo = FlowRepository(spark_session, temp_dirpath="/gpfs/user/XXX")

    # Read data & filter
    flow_repo._df_flow = (
        flow_repo._spark_session.read.option("header", True).csv(flow_dirpath)
        .filter(F.col("gate") == "SOMV01")
        .select("site_code", "faml_grp_labl", "vin", "pass_date")
    )

    return flow_repo


def test_preprocess(flow_repo):
    """Test if flow data is correctly preprocessed."""
    flow_repo.preprocess()

    # Count
    assert flow_repo.df.count() == 666

    # Values
    expected_series = pd.Series(
        {
         "site_code": "PY",
         "faml_grp_labl": "208",
         "smon_date": pd.Timestamp(2019, 10, 22)
        },
        name=0
    )
    assert_series_equal(
        flow_repo.df.filter(F.col("vin") == "VF3CCHMRPKW128019").select(
            "site_code", "faml_grp_labl", "smon_date"
        ).toPandas().iloc[0].sort_index(),
        expected_series.sort_index(),
        check_exact=True
    )
