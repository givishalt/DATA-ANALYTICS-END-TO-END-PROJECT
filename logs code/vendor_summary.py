import pandas as pd 
import numpy as np
import os
from sqlalchemy import create_engine
import logging
import time
from ingestion_db import ingest_db


logging.basicConfig(
    filename='logs/vendor_summary_db.log',
    level=logging.DEBUG,
    format= "%(asctime)s - %(levelname)s-%(message)s",
    filemode='a'
)
                   
def create_vendor_summary(conn):
    """ this function wlll merge the differenet tables to get the overall vendor summary and adding new columns inn the resultant data"""
    vendor_sales_summary=pd.read_sql_query("""
     WITH FreightSummary AS (
        SELECT 
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS actualprice,
            pp.Volume,
            SUM(p.Quantity) AS totalpurchasequantity,
            SUM(p.Dollars) AS totalpurchasedollars
        FROM purchases p
        INNER JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS totalsalesquantity,
            SUM(SalesDollars) AS totalsalesdollars,
            SUM(SalesPrice) AS totalsalesprice,
            SUM(ExciseTax) AS totalexcisetax
        FROM sales
        GROUP BY VendorNo, Brand
    )                        
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.actualprice,
        ps.Volume,
        ps.totalpurchasequantity,
        ps.totalpurchasedollars,
        ss.totalsalesquantity,
        ss.totalsalesdollars,
        ss.totalsalesprice,
        ss.totalexcisetax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.totalpurchasedollars DESC
""", conn)
                                           
    return vendor_sales_summary                                     
                   

def clean_data(df):
    '''this function will clean the data'''
    # changing datatype to float
    df['Volume']=df['Volume'].astype('float')

def ingest_db(df,table_name , engine):
    '''this function will ingest the dataframe into database table '''
    df.to_sql(table_name , con=engine , if_exists='replace',index=False)

    # filling missing with 0
    df.fillna(0,inplace=True)

    # removing spaces from categorical columns
    df['VendorName']= df['VendorName'].str.strip()
    df['Description']=df['Description'].str.strip()


    # creating new column
    vendor_sales_summary['Gross Profit']= vendor_sales_summary['totalsalesdollars']-vendor_sales_summary['totalpurchasedollars']    
    vendor_sales_summary['profitmargin']=vendor_sales_summary['Gross Profit']/vendor_sales_summary['totalsalesdollars']*100
    vendor_sales_summary['stockturnover']=vendor_sales_summary['totalsalesquantity']/vendor_sales_summary['totalpurchasequantity']
    vendor_sales_summary['salestopurchaseratio']=vendor_sales_summary['totalsalesdollars']/vendor_sales_summary['totalpurchasedollars']

    return df

if __name__=='__main__':
    # creating database connection
    conn= sqlite3.connect('inventory.db')

    logging.info('creating vendor summary table')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging_info('Cleaning data')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('ingesting data')
    ingest_db(clean_df, 'vendor_sales_summary' , conn)
    logging.info('completed')


                   
                   
                   
                   
                   














