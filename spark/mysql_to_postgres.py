# import libraries
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



# build mysql connection with spark
spark_mysql = SparkSession \
    .builder \
    .config("spark.jars", "/home/agnes/spark/jars/mysql-connector-java-8.0.30.jar") \
    .master("local[*]") \
    .appName("final_project") \
    .getOrCreate()


# get application_train dataframe from mysql 
application_train = spark_mysql.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_train',
    user='root',
    password='mysqlroot').load()    


# get application_test dataframe from mysql 
application_test = spark_mysql.read.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/mysql',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='sys.home_credit_default_risk_application_test',
    user='root',
    password='mysqlroot').load()    
# application_test.show()




# build postgres connection with spark
spark_postgres = SparkSession \
    .builder \
    .config("spark.jars", "/home/agnes/spark/jars/postgresql-42.5.1.jar") \
    .master("local[*]") \
    .appName("final_project_postgres") \
    .getOrCreate()

# upload application_train dataframe to postgres
application_train.write.format('jdbc').options(
    url='jdbc:postgresql://localhost:5432/postgres',
    driver='org.postgresql.Driver',
    dbtable='final_project.home_credit_default_risk_application_train',
    user='postgres',
    password='1234').mode('ignore').save() 

# upload application_test dataframe to postgres
application_test.write.format('jdbc').options(
    url='jdbc:postgresql://localhost:5432/postgres',
    driver='org.postgresql.Driver',
    dbtable='final_project.home_credit_default_risk_application_test',
    user='postgres',
    password='1234').mode('ignore').save() 

