# DS9 Final Project Agnes Septilia

This repository is a Final Project for Data Engineer Bootcamp batch 9 by Digital Skola. <br />
The project starts with extracting data from various sources and forms, transform as per request, and load to Data Warehouse. <br />
From the data in Data Warehouse, create dimension tables, fact tables, and do machine learning. <br />
Put all the process in scheduler. 

<br />

## SET UP 

Data Warehouse in dbeaver MySQL <br />
    &emsp; - host : localhost <br />
    &emsp; - port : 3306 <br />
    &emsp; - username : root <br />
    &emsp; - password : mysqlroot <br />
    &emsp; - database : mysql <br />
    &emsp; - schema : sys <br />
 <br />
Data Warehouse in dbeaver Postgres
    &emsp; - host : localhost <br />
    &emsp; - port : 5432 <br />
    &emsp; - username : postgres <br />
    &emsp; - password : 1234 <br />
    &emsp; - database : postgres <br />
    &emsp; - schema : final_project <br />
 <br />
Virtual environment python : final_project

<br />

## STEP BY STEP
1. ETL Data Batch Processing <br />
    Tools: Spark, Python (VS Code) <br /> 
    Libraries: Findspark, PySpark <br />
    When using JDBC Driver, download the necessary .jar files, and save in ./spark/jars/ folder <br />

    *  Take csv file from kaggle <br />
        &emsp; https://www.kaggle.com/competitions/home-credit-default-risk/data?select=application_train.csv <br />
        &emsp; ://www.kaggle.com/competitions/home-credit-default-risk/data?select=application_test.csv 

    * Read the csv file using Spark, then upload the result to MySQL through JDBC driver <br />
        > $ python3 spark/csv_to_mysql.py
    
    * Read the data from MySQL using Spark, then upload the result to Postgres through JDBC driver <br />
        > $ python3 spark/mysql_to_postgres.py
 <br />

2. ETL Data NoSQL <br />
    Tools: MongoDB, Python (VS Code) <br />
    Libraries: PyMongo, Pandas, SQL Alchemy <br />
    
    * Create connection with MongoDB Atlas using PyMongo

    * Take document Sample Training - Zips : flatten loc to latitute and longitude --> load to Postgres as: sample_training_zips <br />
        > $ python3 mongodb/mongodb_etl_zips.py 
    * Take document Sample Training - Companies : take only non-array value + only first array of offices --> load to Postgres as: sample_training_companies <br />
        > $ python3 mongodb/mongodb_etl_companies.py    
 <br />

3. ETL Data Stream Processing <br />
    Tools: Kafka, Python (VS Code) <br />
    Libraries: Kafka, json, SQL Alchemy <br />
    
    * Create Kafka topic TopicCurrency <br />
        > $ bin/kafka-topics.sh --create --topic TopicCurrency --bootstrap-server localhost:9092

    * Open link for streaming data from API:  <br />
        &emsp; https://www.freeforexapi.com/api/live?pairs=EURUSD,EURGBP,USDEUR
    
    * Activate the zookeeper <br />
        > $ kafka_2.12-3.2.3/bin/zookeeper-server-start.sh kafka_2.12-3.2.3/config/zookeeper.properties
    
    * Activate the server <br />
        > $ kafka_2.12-3.2.3/bin/kafka-server-start.sh kafka_2.12-3.2.3/config/server.properties
    
    * Activate producer console  <br />
        > $ kafka_2.12-3.2.3/bin/kafka-console-producer.sh --topic TopicCurrency --bootstrap-server localhost:9092
    
    * Activate consumer console <br />
        > $ kafka_2.12-3.2.3/bin/kafka-console-consumer.sh --topic TopicCurrency --from-beginning --bootstrap-server localhost:9092
    
    * Create the Python script as Producer to get the data from API, and transform the data to informative table <br />
        > $ python3 /kafka/kafka_producer.py
    
    * Create the Python script as Consumer to load the result to Postgres data warehouse  <br />
        > $ python3 /kafka/kafka_consumer.py
 <br />

4. Create Dimension table <br />
    Tools: SQL <br />

    * Create dim country from table sample_training_companies (id, country_code) 

    * Create dim state from table sample_training_companies + sample_training_zips (id, country_id, state_code).  <br />
        &emsp; The country_id in dim_state = id in dim_country 

    * Create dim city from table sample_training_companies + sample_training_zips (id, country_id, state_id, city_name, zip_code).  <br />
        &emsp; The country_id in dim_city = country_id in dim_state = id in dim_country  <br />
        &emsp; The state_id in dim_city = id in dim_state. 
    
    * Create dim currency from table topic_currency (id, currency_name, currency_code)
 <br />

5. Create Fact table <br />
    Tools: SQL <br />
    
    * Create fact total city and office per state from table sample_training_companies

    * Create fact daily average currency from table topic_currency (using airflow macros parameter)  <br />
        &emsp; The data will be run everyday at midnight, and collect data from the previous day
    
    * Create fact monthly average currency from table topic_currency (using airflow macros parameter)  <br />
        &emsp; The data will be run at first-day-of-month midnight, and collect data from the previous month
 <br />

6. Machine Learning <br />
    Tools: Python (Jupyter Notebook) <br />
    Libraries: Scikit-learn, Pandas, Imblearn, SQL Alchemy & Psycopg2 <br />

    * Extract table home_credit_default_risk_train and home_credit_default_risk_test from Postgres

    * Using data train, drop columns with null value more than 60%

    * For the remain columns: impute null values with most-frequent for categorical columns, and with median for numerical columns

    * Load clean data to Postgres

    * Split train data to features(X_train) and target(y_train). While for test data, it only acts as features (X_test).
    
    * Build modelling using Logistic Regression, and fit to X_train and y_train

    * Predict model for X_test to get the y_test, get the probability result

    * Load machine learning result to Postgres
 <br />

7. Scheduler <br />
    Tools: Airflow, Python (VS Code) <br />
    Libraries: Airflow, Datetime <br />

    * Create DAG etl_spark for ETL SQL data. <br />
        &emsp; Schedule: Unscheduled <br />
        &emsp; Operator: BashOperator <br />

    * Create DAG etl_mongodb for ETL NoSQL data.  <br />
        &emsp; Schedule: Unscheduled <br />
        &emsp; Operator: BashOperator <br />

    * For Kafka, there is no scheduler set up since the environment can only be set up in local

    * Create DAG machine_learning for machine learning script <br />
        &emsp; Schedule: Unscheduled <br />
        &emsp; Operator: PythonOperator, BashOperator <br />
        &emsp; Dependencies: dag_spark <br />
        &emsp; To make the script executable, convert .ipynb file to .py <br /> 

    * Create DAG dim_tables for processing all dimension tables <br />
        &emsp; Schedule: Unscheduled <br />
        &emsp; Operator: PythonOperator, PostgresOperator <br />
        &emsp; Dependencies: dag_spark and dag_mongodb <br />

    * Create DAG fact_table_daily for processing daily schedule fact table <br />
        &emsp; Schedule: daily (using macros) <br />
        &emsp; Operator: PythonOperator, PostgresOperator <br />
        &emsp; Dependencies: dag_dim_tables <br />

    * Create DAG fact_table_monthly for processing monthly schedule fact table <br />
        &emsp; Schedule: monthly (using macros) <br />
        &emsp; Operator: PythonOperator, PostgresOperator <br />
        &emsp; Dependencies: dag_dim_tables <br />

<br />

## NOTES
* All set up is done in local, the docker file is not updated to the actual set up
* Change all the file paths and dbeaver set up to match yours 
