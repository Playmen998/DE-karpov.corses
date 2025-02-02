import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """

    flights = spark.read \
        .option("header", "true") \
        .parquet(flights_path)

    airlines = spark.read \
        .option("header", "true") \
        .parquet(airlines_path)

    airports = spark.read \
        .option("header", "true") \
        .parquet(airports_path)

    joined_datamart = flights \
        .join(airlines, on=flights['AIRLINE'] == airlines['IATA_CODE'], how='inner') \
        .join(airports.alias('origin'), on=flights['ORIGIN_AIRPORT'] == F.col('origin.IATA_CODE'), how='inner') \
        .join(airports.alias('destination'), on=flights['DESTINATION_AIRPORT'] == F.col('destination.IATA_CODE'),
              how='inner') \
        .select(
        airlines['AIRLINE'].alias('AIRLINE_NAME'),
        F.col('TAIL_NUMBER').alias('TAIL_NUMBER'),
        F.col('origin.COUNTRY').alias('ORIGIN_COUNTRY'),
        F.col('origin.AIRPORT').alias('ORIGIN_AIRPORT_NAME'),
        F.col('origin.LATITUDE').alias('ORIGIN_LATITUDE'),
        F.col('origin.LONGITUDE').alias('ORIGIN_LONGITUDE'),
        F.col('destination.COUNTRY').alias('DESTINATION_COUNTRY'),
        F.col('destination.AIRPORT').alias('DESTINATION_AIRPORT_NAME'),
        F.col('destination.LATITUDE').alias('DESTINATION_LATITUDE'),
        F.col('destination.LONGITUDE').alias('DESTINATION_LONGITUDE')
    )

    joined_datamart.write.mode('append').parquet(result_path)


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
