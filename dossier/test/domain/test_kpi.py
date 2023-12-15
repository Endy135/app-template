"""Unit tests of the domain/kpi module."""
import os
import pytest
from pyspark.sql import functions as F

from app_template.infra.repositories.flow_repository import FlowRepository
from app_template.domain import kpi


flow_filepath = "file:///" + os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "resources", "flow.csv"
    )
)


@pytest.fixture
def flow_repo(spark_session):
    """Return flow repository."""
    # Instantiate flow repository
    flow_repo = FlowRepository(spark_session, temp_dirpath="/gpfs/user/XXX")

    # Read data, filter & preprocess
    flow_repo._df_flow = (
        flow_repo._spark_session.read.option("header", True).csv(flow_filepath)
        .filter(F.col("gate") == "SOMV01")
        .select("site_code", "faml_grp_labl", "vin", "pass_date")
    )
    flow_repo.preprocess()

    return flow_repo


def test_compute_n_vins(flow_repo):
    """Test if KPIs are correctly computed."""
    expected_kpis = {
        "faml_grp_labl": {0: "208", 1: "D34"},
        "n_vin": {0: 485, 1: 181}
    }
    kpis = kpi.compute_n_vins(flow_repo.df, ["faml_grp_labl"]).to_dict()
    assert kpis == expected_kpis
