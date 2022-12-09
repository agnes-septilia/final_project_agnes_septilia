### import libraries
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


### connect to MongoDB and take necessary collections
client = MongoClient("mongodb+srv://mongo_agnes_user:dev@cluster0.54gwtdp.mongodb.net/?retryWrites=true&w=majority")
sample_training = client["sample_training"]
companies_coll = sample_training["companies"]



### TASK 2: for collection companies, take all columns that is not array, and take first object of office field

sample_training_companies = pd.DataFrame()
array_columns = []

for field in companies_coll.find():
    comps_dict = {}
    
    for key, value in field.items():
        if key == '_id':
            comps_dict["id"] = str(value)

        elif key == 'offices':
            if len(value) > 0:
                for k, v in value[0].items():
                    comps_dict["offices_" + k] = v
            else:
                pass

        elif type(value) == dict or type(value) == list:
            if key not in array_columns:
                array_columns.append(key)        
            else:
                pass
        
        else:
            comps_dict[key] = value
        
    df_value = pd.DataFrame([comps_dict])
    
    sample_training_companies = pd.concat([sample_training_companies,df_value])


# drop columns that have array value in other documents
sample_training_companies.drop(array_columns, axis=1, inplace=True)

# print(sample_training_companies.head())


# connection to postgres
url = 'postgresql://postgres:1234@localhost:5432/postgres'
engine = create_engine(url)

# export data to postgres
sample_training_companies.to_sql('sample_training_companies', index=False, con=engine, schema='final_project', if_exists='replace')
