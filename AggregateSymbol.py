'''Importing The Necessary Modules'''
import pyspark
import findspark
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
findspark.init()
from ExtractingRawData import buildingSession
from AggregateDate import readCSV


def aggregateFunction(fd):
    '''Aggregating Facts Data By Symbol Key'''
    aggregate_symbol = fd.groupBy('symbol_key').agg(f.max("open").alias("MaxOpen"), 
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
                        f.avg("volume").alias("AverageVolume")).orderBy('symbol_key')
    return aggregate_symbol

def saveFunction(asym):
    '''Saving The Aggregate Symbol As CSV File'''
    asym.write.mode('overwrite').option("header",True).csv("E:/databricks_projects/tables/symbolaggregation")


def readAggregateSymbol(s):
    '''Reading The Aggregated Symbol Data'''
    symbol_aggregate = s.read.options(header='True', inferSchema='True').csv("E:/databricks_projects/tables/symbolaggregation")
    return symbol_aggregate


def printFunction(symb_agg):
    '''Showing The Aggregated Symbol Data'''
    print(symb_agg.show())


def main():
    '''Main Function'''
    sparks = buildingSession()
    rawdatas = readCSV(sparks)
    aggregate = aggregateFunction(rawdatas)
    printFunction(aggregate)
    saveFunction(aggregate)
    symbol_agg = readAggregateSymbol(sparks)
    printFunction(symbol_agg)
  

if __name__ == '__main__':
    main()