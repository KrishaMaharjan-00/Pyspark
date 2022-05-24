'''Importing The Necessary Modules'''
import pyspark
import findspark
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
findspark.init()


def buildingSession():
        '''Building Spark Session'''
        spark = SparkSession.builder.appName('Databricks_Projects').master('local').getOrCreate()
        return spark

 
def readCSV(s, p):
        '''Reading Multiple CSVs Files'''
        raw_data = s.read.csv(p, inferSchema=True, header=True)
        return raw_data


def printFunctions(rd):
        '''Print Functions'''

        '''Showing The Spark Dataframe Columns'''
        print(f'Columns:: {rd.columns}')

        '''Showing The Number Of Raw Data'''
        print(f'Number Of Raw Data:: {rd.count()}')

        '''Showing The Final Raw Data'''
        print(rd.show())


def dropFunction(rd):
        '''Dropping Unnecessary Columns'''
        raw_data = rd.drop('Series', 'Prev Close', 'Last', 'VWAP', 'Turnover', 'Trades', 'Deliverable Volume', '%Deliverble')
        return raw_data


def fillNullValue(rd):
        '''Filling Null Values With 0'''
        raw_data = rd.fillna(0)
        return raw_data


def timeConversion(rd):
        '''Converting unix timestamp into standard time'''
        raw_data = rd.withColumn("Date", func.to_date(func.col("Date")))
        return raw_data


# raw_data.write.saveAsTable("raw_data", format="parquet", mode="overwrite", path="E:/tables/factdimension")
def saveFunction(rd):
        '''Saving The Final Raw Data As CSV File'''
        rd.write.mode('overwrite').option("header",True).csv("E:/databricks_projects/tables/rawdatas")


def main():
        '''Main Function'''
        sparks = buildingSession()
        '''Defining Path List To CSV Files'''
        path = ['E:/databricks_projects/nifty_stock_data/AXISBANK.csv', 
                'E:/databricks_projects/nifty_stock_data/BRITANNIA.csv',
                'E:/databricks_projects/nifty_stock_data/MARUTI.csv',
                'E:/databricks_projects/nifty_stock_data/RELIANCE.csv',
                'E:/databricks_projects/nifty_stock_data/TITAN.csv'
                ]
        rawdata = readCSV(sparks, path)
        printFunctions(rawdata)
        drop_rd = dropFunction(rawdata)
        fill_nv = fillNullValue(drop_rd)
        time_con = timeConversion(fill_nv)
        printFunctions(time_con)
        saveFunction(time_con)

        
if __name__ == '__main__':
        main()