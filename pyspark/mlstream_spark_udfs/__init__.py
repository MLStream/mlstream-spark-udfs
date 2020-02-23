import os
from pyspark.sql.session import SparkSession


def _ensure_spark_context(spark):
    if spark is None:
       return SparkSession.builder.getOrCreate()
    if not isinstance(spark, SparkSession):
        raise TypeError("Expecting a SparkSession object but got object of type {}".format(type(spark)))
    return spark

def jarPath(spark=None):
    """ Returns the absolute path to the mlstream-spark-udfs for the currently used version of Spark """

    spark=_ensure_spark_context(spark)

    spark_to_scala = {
        "2.4.3": "2.11",
	"2.4.4": "2.11"
    }

    scala_to_spark = {
        "2.11": "mlstream-spark-udfs_2.11-0.1-SNAPSHOT.jar",
        "2.12": "mlstream-spark-udfs_2.12-0.1-SNAPSHOT.jar"
    }

    spark_version = spark.sparkContext.version

    if spark_version not in spark_to_scala:
        raise RuntimeError("I don't know about Spark version {}".format(spark_version))
    scala_version = spark_to_scala[spark_version]
    if scala_version not in scala_to_spark:
        raise RuntimeError("I don't know about Spark version {} with which Spark version {} was build". \
                           format(spark_version, scala_version))
    jar_file = scala_to_spark[scala_version] 
    return os.path.abspath(os.path.join(__file__, "..", "..", "jars", jar_file))

def registerUDFs(spark=None):
    """ Registers MLStream spark udf functions """
    spark = _ensure_spark_context(spark)

    spark._jvm.mlstream.udf.topk.spark.SparkTopK.registerUDFs(spark._jsparkSession)

def unregisterUDFs(spark=None):
    """ Unregisters MLStream spark udf functions """
    spark = _ensure_spark_context(spark)

    spark._jvm.mlstream.udf.topk.spark.SparkTopK.unregisterUDFs(spark._jsparkSession)

def approx_top_k(key, value, top_items, max_memory):
    """ The approximate topk udf which can be used from PySpark """
    pass

