from pyspark.sql import SparkSession


def getSparkSession(app_name: str):
    spark = start_spark(
        app_name=f'{app_name}',
        # Enable broadcast Join and
        # Set Threshold limit of size in bytes to 200 MB of a DataFrame to broadcast
        spark_config={"spark.sql.session.timeZone": "UTC",
                      "spark.sql.caseSensitive": "true",
                      "spark.sql.analyzer.failAmbiguousSelfJoin": "false",
                      "spark.sql.autoBroadcastJoinThreshold": 209715200,
                      "spark.sql.adaptive.enabled": True,
                      "spark.sql.adaptive.advisoryPartitionSizeInBytes": 209715200,
                      "spark.sql.adaptive.coalescePartitions.minPartitionSize": 10485760})

    return spark


def start_spark(app_name, jar_packages=[], spark_config={}):
    spark_builder = (SparkSession.builder.appName(app_name))

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    spark_sess = spark_builder \
        .master('local[4]') \
        .getOrCreate()

    # spark_logger = Log4j(spark_sess)
    # spark_logger.info("start_spark finish")
    spark_sess.sparkContext.setCheckpointDir("/tmp/checkpoints")

    return spark_sess  # , spark_logger
