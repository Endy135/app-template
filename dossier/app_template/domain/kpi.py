"""Example domain function that builds business KPIs."""


def compute_n_vins(df, groupby_cols):
    """Compute number of vins.

    Parameters
    ----------
    df : pyspark.sql.dataframe.DataFrame
        Dataframe containing the vehicles produced.
    groupby_cols : list of str
        Columns to be grouped.

    """
    # Compute KPIs
    kpis_production = (
        df.select([*groupby_cols, "vin"])
        .distinct()
        .groupBy(groupby_cols)
        .count()
        .toPandas()
        .rename(columns={"count": "n_vin"})
        .sort_values(by=groupby_cols)
        .reset_index(drop=True)
    )

    return kpis_production
