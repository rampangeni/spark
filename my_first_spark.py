$ sudo pyspark

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate() 
spark

###### LOAD reported-crimes.csv file

from pyspark.sql.functions import to_timestamp,col,lit
rc = spark.read.csv('/Users/prishapangeni/Desktop/ram-practice/python-spark/reported-crimes.csv',header=True).withColumn('Date',to_timestamp(col('Date'),'MM/dd/yyyy hh:mm:ss a')).filter(col('Date') <= lit('2018-11-11'))
rc.show(5)


###### playing with Schema

rc.printSchema()

labels = [
    ('ID', StringType()),
 ('Case Number', StringType()),
 ('Date', TimestampType()),
 ('Block',StringType()),
 ('IUCR',StringType()),
 ('Primary Type',StringType()),
 ('Description',StringType()),
 ('Location Description',StringType()),
 ('Arrest',StringType()),
 ('Domestic',BooleanType()),
 ('Beat',StringType()),
 ('District',StringType()),
 ('Ward',StringType()),
 ('Community Area',StringType()),
 ('FBI Code',StringType()),
 ('X Coordinate',StringType()),
 ('Y Coordinate',StringType()),
 ('Year',IntegerType()),
 ('Updated On',StringType()),
 ('Latitude',DoubleType()),
 ('Longitude',DoubleType()),
 ('Location',StringType())
]

schema = StructType([StructField(x[0],x[1],True) for x in labels])
schema

rc = spark.read.csv('reported-crimes.csv',schema=schema)
rc.printSchema()

rc.show(5)

###### working with columns

Access a column(s)
rc['ID']
rc.ID
rc.select(col('ID'))

rc.select('ID').show(5)
rc.select(rc.ID).show(5)
rc.select(col('ID')).show(5)

Select multiple columns
rc.select('ID','Case Number').show(3)

List columns
rc.columns

Add a column 

(adding new column 'One' and initializing it with value 1)
from pyspark.sql.functions import lit
rc.withColumn('One', lit(1)).show(5)

(adding new column 'New Column' having value twice of 'Year' column
df = rc.withColumn('New Column', 2*rc['Year'])
df.show(5)

Rename a column name
df = df.withColumnRenamed('New Column', 'Renamed Column')
df.show(5)

Deleting a column
df = df.drop('Renamed Column')

Group by column
df = df.groupBy('ID')

###### working with rows

Filter rows with certain condition
df.filter(col('District')<5).show(5)

Get unique rows
rc.select('YEAR').distinct().show()

Sort Rows
rc.orderBy(col('Year')).show(5)
rc.orderBy('Year', ascending=False).show(5)

Append rows (concatinate the 2 DFs - both must have same schema and rows)
df = df1.union(df2)

Get count of crimes on a particular day
one_day = spark.read.csv('/Users/prishapangeni/Desktop/ram-practice/python-spark/reported-crimes.csv',header=True).withColumn('Date',to_timestamp(col('Date'),'MM/dd/yyyy hh:mm:ss a')).filter(col('Date') == lit('2018-11-11'))
one_day.count()

Sort based on 'Primary Type'
rc.groupBy('Primary Type').count().show()
rc.groupBy('Primary Type').count().orderBy('count',ascending=False).show()

Problem1: Find the percentage of reported crimes that resulted in an arrest
rc.filter(col('Arrest') == 'true').count() / rc.select('Arrest').count()

Problem2: Find the top 3 locations for reported crimes
rc.groupBy('Location Description').count().orderBy('count',ascending=False).show(3)


###### in-built functions

Display all in-build functions
from pyspark.sql import functions
print(dir(functions))


Problem1: Display the Primary Type column in lower and upper case characters, and the first 4 characters of the column
from pyspark.sql.functions import lower,upper,substring
rc.select(lower(col('Primary Type')), upper(col('Primary Type')), substring(col('Primary Type'),1,4)).show(5)

Problem2: Show the oldest date and the most recent date
from pyspark.sql.functions import min,max
rc.select(min(col('Date')),max(col('Date'))).show(1)

Problem3: What is 3 days earlier than the oldest date and 3 days later than the most recent date?
from pyspark.sql.functions import date_add,date_sub
rc.select(date_sub(min(col('Date')),3),date_add(max(col('Date')),3)).show(1)


###### working with dates

## 2019-12-25 13:30:00
from pyspark.sql.functions import to_date,to_timestamp,lit
df = spark.createDataFrame([('2019-12-25 13:30:00',)],['Christmas'])
df.show()

df.select(to_date(col('Christmas'),'yyyy-MM-dd HH:mm:ss'), to_timestamp(col('Christmas'),'yyyy-MM-dd HH:mm:ss')).show()

## 25/Dec/2019 13:30:00
from pyspark.sql.functions import to_date,to_timestamp,lit
df = spark.createDataFrame([('25/Dec/2019 13:30:00',)],['Christmas'])
df.show()

df.select(to_date(col('Christmas'),'dd/MMM/yyyy HH:mm:ss'), to_timestamp(col('Christmas'),'dd/MMM/yyyy HH:mm:ss')).show()

## 12/25/2019 01:30:00 PM
from pyspark.sql.functions import to_date,to_timestamp,lit
df = spark.createDataFrame([('12/25/2019 01:30:00 PM',)],['Christmas'])
df.show()
df.show(truncate=False)

df.select(to_date(col('Christmas'),'MM/dd/yyyy hh:mm:ss a'), to_timestamp(col('Christmas'),'MM/dd/yyyy hh:mm:ss a')).show()


###### working with joins

rc = spark.read.csv('/Users/prishapangeni/Desktop/ram-practice/python-spark/reported-crimes.csv',header=True).show(5)
ps = spark.read.csv('/Users/prishapangeni/Desktop/ram-practice/python-spark/police-stations.csv',header=True).show(5)

## The reported crimes dataset has only the district number. Add the district name by joining with the police station dataset

// cache is lazy evaluation, therefore using count here will bring the df to cache
rc.cache() 
rc.count()

rc.select(col('District')).distinct().show(5)
ps.select(col('DISTRICT')).distinct().show(5)

from pyspark.sql.functions import lpad
help(lpad)
ps.select(lpad(col('DISTRICT'),3,'0')).show()

ps = ps.withColumn('Format_District',lpad(col('DISTRICT'),3,'0'))
ps.show(5)

rc.join(ps, rc.District == ps.Format_District, 'left_outer').show()

ps.columns
rc.join(ps, rc.District == ps.Format_District, 'left_outer').drop('ADDRESS', 'CITY', 'STATE', 'ZIP', 'WEBSITE', 'PHONE', 'FAX', 'TTY', 'X COORDINATE', 'Y COORDINATE', 'LATITUDE', 'LONGITUDE', 'LOCATION', 'Format District').show()

Problem1: Find the most frequently reported noncriminal activity 
rc.select(col('Primary Type')).distinct().count()
rc.select(col('Primary Type')).distinct().show(36)
rc.select(col('Primary Type')).distinct().orderBy(col('Primary Type')).show(36)
rc.select(col('Primary Type')).distinct().orderBy(col('Primary Type')).show(36, truncate=False)
nc = rc.filter((col('Primary Type') == 'NON - CRIMINAL') | (col('Primary Type') == 'NON-CRIMINAL') | (col('Primary Type') == 'NON-CRIMINAL (SUBJECT SPECIFIED)'))
nc.show(50)
nc.groupBy(col('Description')).count().orderBy('count',ascending=False).show()
nc.groupBy(col('Description')).count().orderBy('count',ascending=False).show(truncate=False)

Problem2: Find the day of the week with the most reported crime
>>> rc = spark.read.csv('/Users/prishapangeni/Desktop/ram-practice/python-spark/reported-crimes.csv',header=True).withColumn('Date',to_timestamp(col('Date'),'MM/dd/yyyy hh:mm:ss a')).filter(col('Date') <= lit('2018-11-11'))
from pyspark.sql.functions import dayofweek
help(dayofweek)
rc.select(col('Date'),dayofweek(col('Date'))).show(5)
from pyspark.sql.functions import date_format
rc.select(col('Date'),dayofweek(col('Date')),date_format(col('Date'),'E')).show(5)
rc.groupBy(date_format(col('Date'),'E')).count().show()
rc.groupBy(date_format(col('Date'),'E')).count().orderBy(col('count'),ascending=False).show()

rc.groupBy(date_format(col('Date'),'E')).count().collect()
dow = [x[0] for x in rc.groupBy(date_format(col('Date'),'E')).count().collect()]
dow

cnt = [x[1] for x in rc.groupBy(date_format(col('Date'),'E')).count().collect()]
cnt






