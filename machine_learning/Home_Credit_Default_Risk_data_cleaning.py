### To make the machine learning script executable, download file .ipynb as python and do the syntax cleaning

## Part 1 : Data Cleaning
 
# import libraries
import numpy as np
import pandas as pd 
from sqlalchemy import create_engine


## Data Overview

# connection to postgres using sqlalchemy
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)


# extract train data
application_train = pd.read_sql_query('SELECT * FROM final_project.home_credit_default_risk_application_train', con=engine)


# extract test data
application_test = pd.read_sql_query('SELECT * FROM final_project.home_credit_default_risk_application_test', con=engine)


## Data Cleaning

# drop columns if null value is more than 60
drop_columns = []
for col in application_train.columns:
    null_pct = application_train[col].isna().sum() / len(application_train)
    if null_pct > 0.06:
        drop_columns.append(col)

application_train_clean = application_train.drop(drop_columns, axis=1)


# also drop colums for test data
application_test_clean = application_test.drop(drop_columns, axis=1)


# import libraries
import psycopg2 as pg


# Export data to csv
application_train_clean.to_csv('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/application_train_clean.csv', index=False)
application_test_clean.to_csv('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/application_test_clean.csv', index=False)


# Create empty table in postgres
application_train_clean.head(0).to_sql('home_credit_default_risk_application_train_clean', index=False, con=engine, schema='final_project', if_exists='replace')
application_test_clean.head(0).to_sql('home_credit_default_risk_application_test_clean', index=False, con=engine, schema='final_project', if_exists='replace')


# connection to postgres to fill the table -- using psycopg2
conn = pg.connect(host='localhost', database='postgres', port='5432', user='postgres', password='1234')
cur = conn.cursor()


# upload application_train_clean data
sql_train = """
    COPY final_project.home_credit_default_risk_application_train_clean 
    FROM STDIN DELIMITER ','
    CSV HEADER;
"""

cur.copy_expert(sql_train, open('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/application_train_clean.csv', 'r'))


# upload application_test_clean data
sql_test = """
    COPY final_project.home_credit_default_risk_application_test_clean 
    FROM STDIN DELIMITER ','
    CSV HEADER;
"""

cur.copy_expert(sql_test, open('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/application_test_clean.csv', 'r'))


conn.commit()

