import pyspark.sql.types as T

INPUT_DATA_PATH = '../../resources/rides.csv'

INPUT_FHV_PATH = '../../resources/fhv_rides.csv'
INPUT_GREEN_PATH = '../../resources/green_rides.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

TOPIC_PU_ID_COUNT = 'pickup_id_counts'

TOPIC_ALL = 'rides_all'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv'

PRODUCE_TOPIC_FHV_RIDES_CSV = CONSUME_TOPIC_FHV_RIDES_CSV = 'rides_fhv'
PRODUCE_TOPIC_GREEN_RIDES_CSV = CONSUME_TOPIC_GREEN_RIDES_CSV = 'rides_green'

RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField('tpep_pickup_datetime', T.TimestampType()),
     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
     ])

FHV_RIDE_SCHEMA = T.StructType(
    [T.StructField("dispatching_base_num", T.StringType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropoff_datetime', T.TimestampType()),
     T.StructField("pu_location_id", T.IntegerType()),
     T.StructField("do_location_id", T.IntegerType()),
     ])

GREEN_RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField('lpep_pickup_datetime', T.TimestampType()),
     T.StructField('lpep_dropoff_datetime', T.TimestampType()),
     T.StructField("pu_location_id", T.IntegerType()),
     T.StructField("do_location_id", T.IntegerType()),
     ])

ALL_RIDE_SCHEMA = T.StructType(
    [T.StructField("id", T.StringType()),
     T.StructField("pu_location_id", T.IntegerType()),
     ])
