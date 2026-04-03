import pandas as pd 
import numpy as np
import os
from sqlalchemy import create_engine
import logging
import time


logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format= "%(asctime)s - %(levelname)s-%(message)s",
    filemode='a'
)

engine=create_engine('sqlite:///inventory.db')

def ingest_db(df,table_name , engine):
    '''this function will ingest the dataframe into database table '''
    df.to_sql(table_name , con=engine , if_exists='replace',index=False)

def load_raw_data():
    start=time.time()
    for file in os.listdir('Data'):
         df=pd.read_csv('Data/'+file)
         logging.info(f'Ingesting {file} in db')
         end=time.time()
         ingest_db(df,file[0:-4],engine)
    end=time.time()
    total_time=(end-start)/60
    logging.info('Ingestion Completed')
    logging.indo(f'total time taken: {total_time} minutes')

if __name__=='__main__':
    load_raw_data()