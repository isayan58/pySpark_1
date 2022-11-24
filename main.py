import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

spark = SparkSession.builder.appName('Basics').getOrCreate()
def print_hi(name):
    print(f'Hi, from {name}.')
    df = spark.read.json('people.json')
    df.show()
    df.printSchema()
    df.describe().show()

if __name__ == '__main__':
    print_hi('Sayan')


