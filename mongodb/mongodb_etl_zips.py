### import libraries
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


### connect to MongoDB and take necessary collections
client = MongoClient("mongodb://mongo_agnes_user:dev@ac-ccoug6t-shard-00-00.54gwtdp.mongodb.net:27017,ac-ccoug6t-shard-00-01.54gwtdp.mongodb.net:27017,ac-ccoug6t-shard-00-02.54gwtdp.mongodb.net:27017/?ssl=true&replicaSet=atlas-bzzp8q-shard-0&authSource=admin&retryWrites=true&w=majority")
sample_training = client["sample_training"]
zips_coll = sample_training["zips"]


### For collection zips, take all columns and flatten the loc field to become latitude and longitude 

### DATA PROCESSING

# create empty dataframe to capture the result of data transformation
sample_training_zips = pd.DataFrame()


for field in zips_coll.find():
    zips_column = []
    zips_value = []
    
    for key, value in field.items():
        # convert ObjectID  as id
        if key == '_id':
            zips_column.append("id")
            zips_value.append(str(value))
        
        # flatten loc array to latitude and longitude
        elif key == "loc":
            for k, v in value.items():
                if k == "y":
                    zips_column.append("latitude")
                    zips_value.append(v)
                else:
                    zips_column.append("longitude")
                    zips_value.append(v)
        
        # take other fields as it is
        else:
            zips_column.append(key)
            zips_value.append(value)
    
    # convert dictionary as dataframe row
    df_value = pd.DataFrame([zips_value], columns = zips_column)

    # append row to main dataframe
    sample_training_zips = pd.concat([sample_training_zips,df_value])

# print(sample_training_zips.head())



### UPLOAD DATA TO POSTGRES

# connection to postgres
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)

# export data to postgres
sample_training_zips.to_sql('sample_training_zips_test', index=False, con=engine, schema='final_project', if_exists='replace')
