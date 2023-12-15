"""Class to read/write data in Oracle."""
import os
import cx_Oracle
import sqlalchemy as sa
import pandas as pd

from ..configuration.app import AppConfig


class OracleDatabase(AppConfig):
    """Class to read/write data in Oracle."""

    def __init__(self, dialect="jdbc", spark_session=None):
        """Initialize Oracle database object."""
        AppConfig.__init__(self)
        self._dialect = dialect
        self.db_uri = self._create_db_uri(self.brcdb_config, dialect)
        self._schema = self.brcdb_config.get("username")

        # Spark session
        if dialect.startswith("jdbc"):
            if spark_session is None:
                raise ValueError(
                    "Please specify 'spark_session' parameter in order to use jdbc connector"
                )
            else:
                self._spark_session = spark_session

    def read_df_from_query(
        self,
        query,
        fetchsize=20000,
        partition_column=None,
        n_partitions=None,
        lower_bound=None,
        upper_bound=None,
    ):
        """Load Oracle SQL query output in a dataframe.

        Parameters
        ----------
        query : str
            SQL query (note: multi-line triple-quoted strings work).
        fetchsize : int, default 20000
            Number of rows to load per network call.
        partition_column : str, optional
            Column on which to partition.
        n_partitions : int, optional
            Number of partitions.
        lower_bound : int, optional
            Lower bound of the partition column data.
        upper_bound : int, optional
            upper bound of the partition column data.

        Returns
        -------
        pyspark.sql.dataframe.DataFrame or pd.DataFrame
            Output of the SQL query.

        """
        # Check that the query is correctly formatted
        self._check_query_brackets(query)

        if self._dialect.startswith("jdbc"):
            if partition_column:
                return (
                    self._spark_session.read.option("partitionColumn", partition_column)
                    .option("numPartitions", n_partitions)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                    .option("fetchsize", fetchsize)
                    .jdbc(self.db_uri, table=query)
                )
            else:
                return self._spark_session.read.option("fetchsize", fetchsize).jdbc(
                    self.db_uri, table=query
                )

        elif self._dialect == "cx_oracle":
            with self._connect() as connection:
                return pd.read_sql(query, connection)

    def write_df_to_oracle(self, df, table_name, mode="overwrite", dict_type=None):
        """Write dataframe in an Oracle table.

        Parameters
        ----------
        df : pd.Dataframe
            Dataframe to write.
        table_name : str
            Oracle SQL table name.
        mode : str
            How to behave if the table already exists: 'append', 'truncate', 'overwrite', 'error'.
        dict_type : dict, optional
            Columns types.

        """
        if self._dialect.startswith("jdbc"):
            if mode == "truncate":
                raise ValueError("'truncate' mode is not compatible with jdbc connector")
            df.write.jdbc(url=self.db_uri, table=table_name, mode=mode)

        elif self._dialect == "cx_oracle":
            with self._connect() as connection:
                if mode == "truncate":
                    connection.execute(f"TRUNCATE TABLE {table_name}")
                    mode = "append"

                dict_mode = {"append": "append", "overwrite": "replace", "error": "fails"}
                if dict_type is None:
                    dict_type = self._make_dtype_dict(df)
                df.to_sql(
                    name=table_name.lower(),
                    con=connection,
                    if_exists=dict_mode[mode],
                    index=False,
                    chunksize=10000,
                    dtype=dict_type,
                )

    def inventory(self, schema=None):
        """Retrieve all table names under the schema."""
        if schema is None:
            schema = self._schema
        with self._connect() as connection:
            meta = sa.MetaData(bind=connection, reflect=True, schema=schema)
            return meta.tables

    def _create_db_uri(self, db_config, dialect=None):
        """Create the database uri according to the configuration.

        Parameters
        ----------
        db_config : dict
            Database credentials (host, service_name, username, password, port).
        dialect : str, either 'jdbc' or 'cx_oracle'.
            Connector name (None by default).

        Returns
        -------
        db_uri : str
            URI to access to the database.

        """
        if dialect.startswith("jdbc"):
            db_uri = "{}:{}/{}@//{}:{}/{}".format(
                "jdbc:oracle:thin",
                db_config["username"],
                db_config["password"],
                db_config["host"],
                db_config["port"],
                db_config["service_name"],
            )

        elif dialect == "cx_oracle":
            dns = cx_Oracle.makedsn(
                host=db_config["host"], service_name=db_config["service_name"], port=db_config["port"]
            )
            db_uri = "{}://{}:{}@{}".format(
                "oracle+cx_oracle", db_config["username"], db_config["password"], dns
            )

        else:
            raise NotImplementedError("'dialect' should be among ['cx_oracle', 'jdbc']")

        return db_uri

    def _connect(self):
        """Create a sqlAlchemy connection to the database."""
        engine = sa.create_engine(self.db_uri, max_identifier_length=128)
        return engine.connect()

    def _check_query_brackets(self, query):
        """Check that the query is correctly formatted."""
        query = query.strip()
        first_char = query[0]
        last_char = query[-1]
        if (first_char != "(") or (last_char != ")"):
            raise ValueError("Your query must be enclosed in brackets.")

    def _make_dtype_dict(self, df):
        """Match the Oracle & pandas types.

        https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.DataFrame.to_sql.html
        This is required because for Oracle database, pandas default the type
        'object' to 'clob' which is super heavy. See:
        https://stackoverflow.com/questions/42727990/speed-up-to-sql-when-writing-pandas-dataframe-to-oracle-database-using-sqlalch
        https://docs.oracle.com/javadb/10.10.1.2/ref/rrefclob.html

        Parameters
        ----------
        df : pd.Dataframe
            Dataframe to write in Oracle.

        Returns
        -------
        dict
            {cols : VARCHAR(max_length in the series)} for object type columns in df.

        """
        return {
            col: sa.types.VARCHAR(self._get_max_length(df[col]))
            for col, dtype in df.dtypes.astype(str).to_dict().items()
            if dtype == 'object' or dtype == 'category' or dtype == 'string'
        }

    def _get_max_length(self, series):
        """Get the length of the longest element in the series.

        Parameters
        ----------
        series : pd.Series
            Specific series.

        Returns
        -------
        maxi : int
            Length of the longest element in the series.

        """
        maxi = series.str.len().max()
        if maxi and not pd.isnull(maxi):
            return int(maxi)
        return 1
