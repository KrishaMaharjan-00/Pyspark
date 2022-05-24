'''Importing The Necessary Modules'''
import pyspark
import findspark
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
findspark.init()
from ExtractingRawData import buildingSession


def readCSV(s):
    '''Reading Raw Data From Previously Stored CSV Files'''
    raw_data = s.read.options(header='True', inferSchema='True').csv("E:/databricks_projects/tables/rawdatas")
    return raw_data


def createSparkDateFrame(rd):
    '''Creating Date Spark Dataframe'''
    dateframe = rd.drop('Symbol','Open','High','Low','Close','Volume','Trades')
    dateframe = dateframe.select('Date').distinct().orderBy('Date')
    dateframe = dateframe.withColumn('date_key',monotonically_increasing_id())
    dateframe = dateframe.withColumn('years',f.year(f.to_timestamp('Date', 'yyyy-MM-dd'))).withColumn('months',f.month(f.to_timestamp('Date', 'yyyy-MM-dd'))).withColumn('days',f.dayofmonth(f.to_timestamp('Date', 'yyyy-MM-dd')))
    return dateframe


def createSparkSymbolFrame(rd):
    '''Creating Symbol Spark Dataframe'''
    symbolframe = rd.drop('Date','Open','High','Low','Close','Volume','Trades')
    symbolframe = symbolframe.select('Symbol').distinct()
    symbolframe = symbolframe.withColumn('symbol_key',monotonically_increasing_id())
    return symbolframe


def createSparkFactFrame(rd, df, sf):
    '''Creating Fact Spark Dataframe'''
    factframe = rd.join(df, df.Date == rd.Date, 'inner').select(df['date_key'], rd['*'])
    factframe = factframe.join(sf, sf.Symbol == factframe.Symbol, 'inner').select(sf['symbol_key'], factframe['*'])
    factframe = factframe.withColumn('fact_key',monotonically_increasing_id())
    return factframe


def printFunctions(rd, df, sf, ff):
    '''Printing Functions'''
    '''Showing The Schema Of Raw Data'''
    print(rd.schema)

    '''Showing The Raw Data'''
    print(rd.show())

    '''Showing Date Dataframe'''
    print(df.show())

    '''Showing Symbol Dataframe'''
    print(sf.show())

    '''Showing Fact Dataframe'''
    print(ff.show())

    '''Counting Number Of Datas In Fact Dataframe'''
    print(ff.count())


def saveFunction(ff):
    '''Saving The Facts Data As CSV File'''
    ff.write.mode('overwrite').option("header",True).csv("E:/databricks_projects/tables/factstable")


def main():
    '''Main Function'''
    sparks = buildingSession()
    rawdatas = readCSV(sparks)
    dateframes = createSparkDateFrame(rawdatas)
    symbolframes = createSparkSymbolFrame(rawdatas)
    factframes = createSparkFactFrame(rawdatas, dateframes, symbolframes)
    printFunctions(rawdatas, dateframes, symbolframes, factframes)
    saveFunction(factframes)


if __name__ == '__main__':
    main()