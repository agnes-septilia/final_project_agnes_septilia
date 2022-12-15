### import libraries
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg


### connect to MongoDB and take necessary collections
client = MongoClient("mongodb://mongo_agnes_user:dev@ac-ccoug6t-shard-00-00.54gwtdp.mongodb.net:27017,ac-ccoug6t-shard-00-01.54gwtdp.mongodb.net:27017,ac-ccoug6t-shard-00-02.54gwtdp.mongodb.net:27017/?ssl=true&replicaSet=atlas-bzzp8q-shard-0&authSource=admin&retryWrites=true&w=majority")
sample_training = client["sample_training"]
companies_coll = sample_training["companies"]


### For collection companies, take all columns that is not array, and take first object of office field

### DATA PROCESSING

# create empty dataframe to capture the result of data transformation
sample_training_companies = pd.DataFrame()

# create empty list to capture all array fields except offices
array_fields = []


for field in companies_coll.find():
    comps_dict = {}
    
    for key, value in field.items():
        # convert ObjectID  as id
        if key == '_id':
            comps_dict["id"] = str(value)

        # for field offices, take only first object, and flatten the result
        elif key == 'offices':
            if len(value) > 0:
                for k, v in value[0].items():
                    comps_dict["offices_" + k] = v
            else:
                pass

        # for other array or list field, append to array_column list, and later we will drop these columns
        elif type(value) == dict or type(value) == list:
            if key not in array_fields:
                array_fields.append(key)        
            else:
                pass
        
        # for remained columns, take field as it is
        else:
            comps_dict[key] = value
        
    # convert dictionary as dataframe row
    df_value = pd.DataFrame([comps_dict])
    
    # append row to main dataframe
    sample_training_companies = pd.concat([sample_training_companies,df_value])

# drop columns in array_fields
for col in sample_training_companies.columns:
    if col in array_fields:
        sample_training_companies.drop(col, axis=1, inplace=True)
        
# print(sample_training_companies.head())



### UPLOAD DATA TO POSTGRES

""" NOTE
Since directly upload pandas dataframe takes long time,
here I convert the data in csv first, and then upload to postgres using the csv files
"""

# Export data to csv
sample_training_companies.to_csv('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/sample_training_companies_test.csv', index=False)

# connection to postgres
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)

# Create empty table in postgres
sample_training_companies.head(0).to_sql('sample_training_companies_test', index=False, con=engine, schema='final_project', if_exists='replace')

# connection to postgres to fill the table -- using psycopg2
conn = pg.connect(host='localhost', database='postgres', port='5432', user='postgres', password='1234')
cur = conn.cursor()

# upload application_train_clean data
sql = """
    COPY final_project.sample_training_companies_test
    FROM STDIN DELIMITER ','
    CSV HEADER;
"""

cur.copy_expert(sql, open('/home/agnes/Documents/digital_skola/Project/final_project/csv_file/sample_training_companies_test.csv', 'r'))
conn.commit()
