from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
#from pyspark.sql.functions import regexp_replace, col
#from pyspark.sql.functions import count,desc

#from pyspark.sql.functions import row_number
#from pyspark.sql.functions import col,avg,sum,min,max,row_number 


spark = SparkSession.builder.master("yarn").appName("project").getOrCreate()

#Reading CSV file from GCS into DataFrame
df_startup = spark.read.csv("gs://hitesh-for-project/data", header=True, inferSchema=True)
#df_startup.show(3)

#Names of the columns 
df_startup.columns

#Total no of columns 
len(df_startup.columns)

#Total no of Rows Before Cleansing
df_startup.count()

#removing of duplicates recordss
df_startup_nodup = df_startup.dropDuplicates()

#Total no of rows after removal of duplicates
df_startup_nodup.count()

#no of startup founded between 2010 to 2020
df_startup_nodup = df_startup.dropDuplicates()
df_betyear = df_startup_nodup.filter("Founded >= 2010 and Founded<=2020")

#to remove ',' and 'disclose' and '$' from Amount column 
#from pyspark.sql.functions import regexp_replace
df_betyear33 = df_betyear.withColumn("Amount",regexp_replace(col("Amount"), ",", "")).withColumn("Amount",regexp_replace(col("Amount"), "Undisclosed", "0")).withColumn('Amount', substring('Amount', 2,20))


#to change data type of Amount column from string to Integer
df_betyear33 = df_betyear33.withColumn('Amount', col('Amount').cast(IntegerType()))

#loading data to GCS in only one csv file
df_betyear33.repartition(1).write.save("gs://hitesh-for-project/dataout/noduplicateclean.csv",format='csv', header=True)

#No of startup founded in a particular city between 2010 to 2020
df_founbasedoncity = df_betyear33.groupBy("Headquarters").agg(count("Headquarters").alias("no of startup")).sort(desc("no of startup"))

df_founbasedoncity.show()

#saving data df_founbasedoncity in google cloud storage
df_founbasedoncity.repartition(1).write.save("gs://hitesh-for-project/dataout/noofstartupincities.csv",format='csv', header=True)

#no of startup founded in a perticular year between 2010 to 2020
from pyspark.sql.functions import count,desc
df_founbasedonyear = df_betyear33.groupBy("Founded").agg(count("Founded").alias("no of startups")).sort(desc("no of startups"))
df_founbasedonyear.show()

#saving data df_founbasedonyear in google cloud storage
df_founbasedonyear.repartition(1).write.save("gs://hitesh-for-project/dataout/noofstartupinyears.csv",format='csv', header=True)


#ranking of startup based on amount from a perticular Headquaters
df_windows1 = Window.partitionBy("Headquarters").orderBy("Amount")
df_new2 = df_betyear.withColumn("Dense Rank", dense_rank().over(df_windows1))

df_new2.show()


#saving data ranking of startup based on amount from a perticular Headquaters
df_new2.repartition(1).write.save("gs://hitesh-for-project/dataout/rankingforperticularheadquatersgrp.csv",format='csv', header=True)

#Top funded sectors in 2016
df_topfunded_Sector_2016 = df_betyear33.filter("Founded = 2016").groupBy("Sector").agg(sum("Amount").alias("total amount")).sort(desc("total amount"))
df_topfunded_Sector_2016.show()

#saving data df_topfunded_Sector_2016 in google cloud storage
df_topfunded_Sector_2016.repartition(1).write.save("gs://hitesh-for-project/dataout/topfunded_Sector_2016.csv",format='csv', header=True)


#Top funded sectors between 2010 to 2020
df_topfunded_Sector_2010to2020 = df_betyear33.groupBy("Sector").agg(sum("Amount").alias("total amount")).sort(desc("total amount"))

df_topfunded_Sector_2010to2020.show()


#saving data df_topfunded_Sector_2010to2020 in google cloud storage
df_topfunded_Sector_2010to2020.repartition(1).write.save("gs://hitesh-for-project/dataout/topfunded_Sector_2010to2020.csv",format='csv', header=True)


df_topfunded_Sector_2010to2020.count()

len(df_topfunded_Sector_2010to2020.columns)

df_topfunded_Sector_2010to2020.columns


