import os
import logging
import hashlib

from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql import dataframe as pydf
import pandas as pd


class Anonymizer:
    """Anonymizer."""

    def __init__(self, df, columns_to_anonymize, anonymization_key):
        """Initialize Anonymizer.

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            The dataframe containing the columns to anonymize.
        columns_to_anonymize : list of str
            List of the columns to anonymize.
        anonymization_key : str
            Key used to hash data.

        """
        # Dataframe to anonymize
        self._df_anon = df

        # List of columns to hash
        self._hashed_cols = columns_to_anonymize
        self._unhashed_cols = {
            c for c in self._df_anon.columns if c not in self._hashed_cols
        }

        # Secret key for hash
        self._secret_key = anonymization_key

    @property
    def df(self):
        """Return dataframe containing data anonymized."""
        return self._df_anon

    @property
    def df_hashed_value(self):
        """Return dataframe containing only data anonymized and their hash."""
        return self._df_mapping_hash

    def preprocess(self):
        """Preprocess the anonymization.

        Anonymize a column in the pyspark dataframe with the secret key.
        """
        logging.info("Anonymizer preprocess")

        if isinstance(self._df_anon, pd.DataFrame):
            self._preprocess_pandas()
        elif isinstance(self._df_anon, pydf.DataFrame):
            self._preprocess_pyspark()
        else:
            raise ValueError("Invalid type, df must be a Panda or a Pyspark dataframe")

    def _preprocess_pyspark(self):
        """Preprocess the anonymization for Pyspark dataframe."""
        logging.info("Anonymizer preprocess Pyspark")
        udf_hash = F.udf(_md5_hash, T.StringType())
        # hash values
        self._df_anon = self._df_anon.select(
            "*",
            *(
                udf_hash(c, F.lit(self._secret_key)).alias(f"{c}_HASH")
                for c in self._hashed_cols
            ),
        )
        # create dataframe only with values and their hash
        self._df_mapping_hash = self._df_anon.select(
            *(self._hashed_cols), *(F.col(f"{c}_HASH") for c in self._hashed_cols)
        ).dropDuplicates()
        # remove clear values
        self._df_anon = self._df_anon.select(
            *self._unhashed_cols,
            *(F.col(f"{c}_HASH").alias(f"{c}") for c in self._hashed_cols),
        )

    def _preprocess_pandas(self):
        """Preprocess the anonymization for Panda dataframe."""
        logging.info("Anonymizer preprocess Panda")
        # hash values
        for c in self._hashed_cols:
            self._df_anon[f"{c}_HASH"] = self._df_anon.apply(
                lambda row: _md5_hash(row[c], self._secret_key), axis=1
            )
        # create dataframe only with values and their hash
        self._df_mapping_hash = self._df_anon[
            [*(self._hashed_cols), *(f"{c}_HASH" for c in self._hashed_cols)]
        ]
        self._df_mapping_hash = self._df_mapping_hash.drop_duplicates()
        # drop clear values
        self._df_anon = self._df_anon.drop([c for c in self._hashed_cols], axis=1)
        # rename hash values
        self._df_anon = self._df_anon.rename(
            columns={c + "_HASH": c for c in self._hashed_cols}
        )


def _md5_hash(x, secret_key):
    """Return the MD5 hash of the value.

    Parameters
    ----------
    x : str
        The value to anonymize.

    Returns
    -------
    str
        The MD5 hash of the value

    """
    return hashlib.md5(x.encode("utf-8") + secret_key.encode("utf-8")).hexdigest()
