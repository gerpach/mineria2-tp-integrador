from pyspark.sql import SparkSession


def get_spark(app_name: str = "tp-mineria2", shuffle_partitions: int = 8) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Los timestamps de los eventos vienen en UTC ("Z"): fechas de partición en UTC.
        .config("spark.sql.session.timeZone", "UTC")
        # Re-ejecutar una escritura reemplaza solo las particiones que se vuelven a escribir (idempotencia).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
