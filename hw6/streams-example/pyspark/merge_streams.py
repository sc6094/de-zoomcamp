from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import RIDE_SCHEMA, CONSUME_TOPIC_RIDES_CSV, TOPIC_WINDOWED_VENDOR_ID_COUNT, CONSUME_TOPIC_FHV_RIDES_CSV, \
    CONSUME_TOPIC_GREEN_RIDES_CSV, FHV_RIDE_SCHEMA, GREEN_RIDE_SCHEMA, TOPIC_ALL, TOPIC_WINDOWED_PU_ID_COUNT, \
    ALL_RIDE_SCHEMA, TOPIC_PU_ID_COUNT


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream



def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode('complete') \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    columns = df.columns

    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


def op_windowed_groupby(df, window_duration, slide_duration):
    df_windowed_aggregation = df.groupBy(
        F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration, slideDuration=slide_duration),
        df.vendor_id
    ).count()
    return df_windowed_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # read_streaming data
    df_consume_stream_fhv = read_from_kafka(consume_topic=CONSUME_TOPIC_FHV_RIDES_CSV)
    print(df_consume_stream_fhv.printSchema())

    df_consume_stream_green = read_from_kafka(consume_topic=CONSUME_TOPIC_GREEN_RIDES_CSV)
    print(df_consume_stream_green.printSchema())

    # parse streaming data
    df_fhv_rides = parse_ride_from_kafka_message(df_consume_stream_fhv, FHV_RIDE_SCHEMA)
    print(df_fhv_rides.printSchema())

    sink_console(df_fhv_rides, output_mode='append')

    df_green_rides = parse_ride_from_kafka_message(df_consume_stream_green, GREEN_RIDE_SCHEMA)
    print(df_green_rides.printSchema())

    sink_console(df_green_rides, output_mode='append')

    df_fhv_rides = df_fhv_rides.withColumnRenamed('dispatching_base_num', 'id')
    df_green_rides = df_green_rides.withColumnRenamed('vendor_id', 'id')

    df_fhv_messages = prepare_df_to_kafka_sink(df=df_fhv_rides, value_columns=['id'], key_column='pu_location_id')

    df_green_messages = prepare_df_to_kafka_sink(df=df_green_rides, value_columns=['id'], key_column='pu_location_id')

    sink_kafka(df=df_fhv_messages, topic=TOPIC_ALL)

    sink_kafka(df=df_green_messages, topic=TOPIC_ALL)

    df_consume_stream_all = read_from_kafka(consume_topic=TOPIC_ALL)

    df_all_rides = parse_ride_from_kafka_message(df_consume_stream_all, ALL_RIDE_SCHEMA)
    print(df_all_rides.printSchema())

    df_trip_count_by_pu_location_id = op_groupby(df_all_rides, ['pu_location_id'])

    # write the output out to the console for debugging / testing
    sink_console(df_trip_count_by_pu_location_id)
    # write the output to the kafka topic
    df_trip_count_messages = prepare_df_to_kafka_sink(df=df_trip_count_by_pu_location_id,
                                                      value_columns=['count'], key_column='pu_location_id')
    kafka_sink_query = sink_kafka(df=df_trip_count_messages, topic=TOPIC_PU_ID_COUNT)

    spark.streams.awaitAnyTermination()