"""SparkSession construction.

Defaults to local[*] so the exact same code runs on a laptop against the
sample data/ directory and against a real cluster — only the environment
changes, not the application code. Point SPARK_MASTER at a real cluster
(yarn, k8s://..., spark://...) and everything else is unchanged.
"""

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "revenue-pipeline") -> SparkSession:
    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        # Default (200) is tuned for cluster-scale shuffles; way oversized for a
        # laptop run over a small sample, where it just creates task overhead.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        # Spark defaults to ANSI mode (as of Spark 4), where an invalid cast
        # RAISES instead of returning null. Every cast in this pipeline is
        # written the way pandas' `errors="coerce"` works — parse failure
        # becomes null, which QC then counts and reports — so ANSI's raise-on-
        # invalid-input would crash the job on exactly the malformed values
        # this pipeline exists to catch. Off, to get that coercing behavior.
        .config("spark.sql.ansi.enabled", "false")
    )
    return builder.getOrCreate()
