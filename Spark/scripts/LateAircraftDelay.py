import argparse

from pyspark.sql import SparkSession

def late_aircraft_delay(data_source, output_uri):
    """
    :param data_source: The URI of your delayedflights data CSV, such as 's3://DOC-EXAMPLE-BUCKET/delayedflights.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/output'.
    """
    with SparkSession.builder.appName("Calculate late_aircraft delay").getOrCreate() as spark:
        if data_source is not None:
            delays_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        delays_df.createOrReplaceTempView("delay_data")

        late_aircraft_delay_data = spark.sql("""SELECT Year, avg((LateAircraftDelay/ArrDelay)*100) FROM delay_data GROUP BY Year""")
        # Write the results to the specified output URI
        late_aircraft_delay_data.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV delayedflights data, S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, S3 bucket location.")
    args = parser.parse_args()

    late_aircraft_delay(args.data_source, args.output_uri)
			