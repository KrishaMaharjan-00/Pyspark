'''Importing The Necessary Modules'''
import pyspark
import findspark
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
findspark.init()
from ExtractingRawData import buildingSession


def readCSV(s):
    '''Reading Facts Data From Previously Stored CSV Files'''
    fact_data = s.read.options(header='True', inferSchema='True').csv("E:/databricks_projects/tables/factstable")
    return fact_data


def aggregateFunction(fd):
    '''Aggregating Facts Data By Date Key'''
    aggregate_date = fd.groupBy('date_key').agg(f.max("open").alias("MaxOpen"), 
                        f.min("open").alias("MinOpen"), 
                        f.avg("open").alias("AverageOpen"),
                        f.max("close").alias("MaxClose"), 
                        f.min("close").alias("MinClose"), 
                        f.avg("close").alias("AverageClose"),
                        f.max("high").alias("MaxHigh"), 
                        f.min("high").alias("MinHigh"), 
                        f.avg("high").alias("AverageHigh"),
                        f.max("low").alias("MaxLow"), 
                        f.min("low").alias("MinLow"), 
                        f.avg("low").alias("AverageLow"),
                        f.max("volume").alias("MaxVolume"), 
                        f.min("volume").alias("MinVolume"), 
                        f.avg("volume").alias("AverageVolume")).orderBy('date_key')
    return aggregate_date


def printFunction(ad):
    '''Showing The Aggregated Date Data'''
    print(ad.show())


def saveFunction(ad):
    '''Saving The Aggregate Date As CSV File'''
    ad.write.mode('overwrite').option("header",True).csv("E:/databricks_projects/tables/dateaggregation")


def readAggregateDate(s):
    '''Reading The Aggregated Date Data'''
    date_aggregate = s.read.options(header='True', inferSchema='True').csv("E:/databricks_projects/tables/dateaggregation")
    return date_aggregate


def main():
    '''Main Function'''
    sparks = buildingSession()
    rawdatas = readCSV(sparks)
    aggregate = aggregateFunction(rawdatas)
    printFunction(aggregate)
    saveFunction(aggregate)
    date_agg = readAggregateDate(sparks)
    printFunction(date_agg)
  

if __name__ == '__main__':
    main()