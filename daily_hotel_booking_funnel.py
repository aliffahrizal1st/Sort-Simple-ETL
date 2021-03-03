#--Library--
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
  spark = SparkSession \
    .builder \
    .appName("") \
    .config("") \
    .getOrCreate()

#--Extract--
#---Read Dataset---
dataset = spark.read.csv('Pegipegi Case Study - Data Engineer Technical Test (Data Source).csv',header=True)
print("Total Row : ",dataset.count())
# dataset.show(5)

#---Checking The Schema---
# dataset.printSchema()

dataset.registerTempTable("dataset")

#--Transform--
transform = spark.sql(
  "select date(event_time) as event_date, "+
  "interface_type, app_version, "+
  "count(case when event_name in ('visit') then 1 else 0 end) as num_visit, "+
  "count(case when event_name in ('search') then 1 else 0 end) as num_search, "+
  "count(case when event_name in ('view_product') then 1 else 0 end) as num_product_viewed, "+
  "count(case when event_name in ('add_to_chart') then 1 else 0 end) as num_add_to_cart, "+
  "count(case when event_name in ('checkout') then 1 else 0 end) as num_checkout, "+
  "count(case when event_name in ('select_payment') then 1 else 0 end) as num_payment_selected, "+
  "count(case when event_name in ('create_booking') then 1 else 0 end) as num_booking_created, "+
  "count(distinct session_id, case when event_name in ('visit') then 1 else 0 end) as num_session_visit, "+
  "count(distinct session_id, case when event_name in ('search') then 1 else 0 end) as num_session_search, "+
  "count(distinct session_id, case when event_name in ('view_product') then 1 else 0 end) as num_session_product_viewed, "+
  "count(distinct session_id, case when event_name in ('add_to_chart') then 1 else 0 end) as num_session_add_to_cart, "+
  "count(distinct session_id, case when event_name in ('checkout') then 1 else 0 end) as num_session_checkout, "+
  "count(distinct session_id, case when event_name in ('select_payment') then 1 else 0 end) as num_session_payment_selected, "+
  "count(distinct session_id, case when event_name in ('create_booking') then 1 else 0 end) as num_session_booking_created "+
  "from dataset "+
  "group by event_date, interface_type, app_version "+
  "order by event_date, interface_type, app_version")

transform.show(5)
#---Checking The Schema---
transform.printSchema()

#---Change the Data Type Since It's Using 'Long'---
columnNames = transform.schema.names
for i in range(len(columnNames)):
  if i > 2 :
    transform = transform.withColumn(columnNames[i], transform[columnNames[i]].cast(IntegerType()))

transform.show(5)
#---Checking The Schema---
transform.printSchema()

transform.registerTempTable("dataset")

#--Load--
#---Convert Final Tranform to CSV format---
transform.toPandas().to_csv("Daily_Hotel_Booking_Funnel.csv")

