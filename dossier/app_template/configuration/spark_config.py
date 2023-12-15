"""Module to create a Spark application."""
import os
import logging
import atexit
from pyspark.sql import SparkSession


def get_spark(
    app_name,
    driver_cores=1,
    driver_mem="4g",
    max_executors=8,
    executor_cores=4,
    executor_mem="4g",
    queue="default"
):
    """Create Spark application.

    Parameters
    ----------
    app_name : string
        Spark application name which must contain the application prd.
    driver_cores : int
        Number of cores to use for the driver process.
    driver_mem : string, e.g. "4g"
        Amount of memory to use for the driver process.
    max_executors : int
        Maximum number of executors to run (with dynamic allocation).
    executor_cores : int
        Number of cores to use on each executor.
    executor_mem : string, e.g. "8g"
        Amount of memory to use per executor process.
    queue : string, either "default", "preprod", "prod"
        Name of the yarn queue to which the application is submitted.

    Returns
    -------
    spark_context : pyspark.context.SparkContext
        Context of the Spark application.
    spark_session : pyspark.sql.session.SparkSession
        Session of the Spark application.

    """
    logging.info("Creating Spark application with the provided configuration")
    logging.info("Environment variables are the following:")
    logging.info("\n" + _get_spark_environment_variables())
    spark_session = _build_spark_session(
        app_name=app_name,
        driver_cores=driver_cores,
        driver_mem=driver_mem,
        max_executors=max_executors,
        executor_cores=executor_cores,
        executor_mem=executor_mem,
        queue=queue
    )
    spark_context = spark_session.sparkContext
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
    atexit.register(lambda: spark_context.stop())

    return spark_context, spark_session


def _get_spark_environment_variables():
    """Get Spark environment variables."""
    return "\n".join([
        "{} : {}".format(var, os.environ.get(var, "-variable is unset-"))
        for var in [
            "ENVIRONMENT",
            "PYSPARK_PYTHON",
            "PYSPARK_DRIVER_PYTHON",
            "SPARK_CLASSPATH",
            "SPARK_HOME",
            "PYTHONPATH",
        ]
    ])


def _build_spark_session(
    app_name, driver_cores, driver_mem, max_executors, executor_cores,
    executor_mem, queue
):
    """Build Spark session."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.master", "yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.cores", driver_cores)
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.cores", executor_cores)
        .config("spark.executor.memory", executor_mem)
        .config("spark.shuffle.service.enabled", True)
        .config("spark.dynamicAllocation.enabled", True)
        .config("spark.dynamicAllocation.minExecutors", 0)
        .config("spark.dynamicAllocation.maxExecutors", max_executors)
        .config("spark.executor.memoryOverhead", 2048)
        .config("spark.driver.memoryOverhead", 1024)
        .config("spark.yarn.queue", queue)
        # .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraClassPath", "/soft/ora1210/db/jdbc/lib/ojdbc6.jar")
        .config("spark.executor.extraClassPath", "/soft/ora1210/db/jdbc/lib/ojdbc6.jar")
        .getOrCreate()
    )
