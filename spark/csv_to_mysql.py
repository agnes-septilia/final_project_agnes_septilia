# import libraries
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# build connection with spark
spark = SparkSession \
    .builder \
    .config("spark.jars", "/home/agnes/spark/jars/mysql-connector-java-8.0.30.jar") \
    .master("local[*]") \
    .appName("final_project") \
    .getOrCreate()


# get data for application_train
application_train = spark.read \
                .format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load('/home/agnes/Documents/digital_skola/Project/final_project/spark/source/application_train.csv')
# application_train.show()


# get data for application_test
application_test = spark.read \
                .format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load('/home/agnes/Documents/digital_skola/Project/final_project/spark/source/application_test.csv')
    

# upload data application_train to mysql
application_train.write.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_train',
    user='root',
    password='mysqlroot').mode('ignore').save()


# upload data application_test to mysql
application_test.write.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_test',
    user='root',
    password='mysqlroot').mode('ignore').save()