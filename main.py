import pyspark.sql
from pyspark import *
from pyspark.sql.functions import col

pyspark = pyspark.sql.SparkSession.builder.master("local[*]").appName("straptigens_task").getOrCreate()
weather_data = pyspark.read.format("csv").option("Header","true").option("inferSchema","true").load("/home/austin/Desktop/Default_workspace2/Newport_s_job_assessment/cardiff_bute_park_weather_data.csv")
total_by_year = weather_data.groupBy("year").sum("rain_mm").show() ## The total rainfall per year
total_by_year_last10Years = weather_data.groupBy("year").sum("rain_mm").orderBy("year").where("year > 2011").show() ## The total rainfall per year for the last 10 years only
highest_avg_rainfall_month=weather_data.groupBy("month").avg("rain_mm").orderBy(col("avg(rain_mm)").desc()).limit(1).show() ## Which month sees the most rainfall on average
lowest_temp_recorded=weather_data.orderBy(col("min_degrees_c").desc()).limit(1).show()## The lowest temperature recorded plus the year and month in which it was recorded
