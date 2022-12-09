### import libraries
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


### connect to MongoDB and take necessary collections
client = MongoClient("mongodb+srv://mongo_agnes_user:dev@cluster0.54gwtdp.mongodb.net/?retryWrites=true&w=majority")
sample_training = client["sample_training"]
zips_coll = sample_training["zips"]



### TASK 1: for collection zips, take all columns and flatten the loc field to become latitude and longitude 

sample_training_zips = pd.DataFrame()

for field in zips_coll.find():
    zips_column = []
    zips_value = []
    
    for key, value in field.items():
        if key == '_id':
            zips_column.append("id")
            zips_value.append(str(value))
        
        elif key == "loc":
            for k, v in value.items():
                if k == "y":
                    zips_column.append("latitude")
                    zips_value.append(v)
                else:
                    zips_column.append("longitude")
                    zips_value.append(v)
        else:
            zips_column.append(key)
            zips_value.append(value)
    
    df_value = pd.DataFrame([zips_value], columns = zips_column)
    sample_training_zips = pd.concat([sample_training_zips,df_value])

# print(sample_training_zips.head())



# connection to postgres
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)

# export data to postgres
sample_training_zips.to_sql('sample_training_zips', index=False, con=engine, schema='final_project', if_exists='replace')
