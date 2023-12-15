"""Useful system functions."""
import os
from dateutil.relativedelta import relativedelta


def get_hdfs_path(gpfs_path):
    """Get the HDFS path from the GPFS path.

    Parameters
    ----------
    gpfs_path : str
        GPFS path, starting with '/gpfs'

    """
    return gpfs_path.split("/gpfs")[1]


def get_execution_time(start, end):
    """Get the time between two timestamps in human readable format.

    Parameters
    ----------
    start : datetime.datetime
        Start datetime.
    end : datetime.datetime
        End datetime.

    """
    rd = relativedelta(end, start)
    return "%d hours, %d minutes and %d seconds" % (rd.hours, rd.minutes, rd.seconds)
