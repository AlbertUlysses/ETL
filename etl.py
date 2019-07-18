import os
import glob
import psycopg2 as pg2
import pandas as pd
from sql_queries import *

def process_payroll_file(cur, filepath):
    """Inserts records into the database from the csv files"""
    
    df = pd.read_csv(filepath, dtype = object)
    
    # converts the datatypes to datetime
    df['PAY_PERIOD_BEGIN_DATE'] = pd.to_datetime(df['PAY_PERIOD_BEGIN_DATE']).astype(object)
    df['PAY_PERIOD_END_DATE'] = pd.to_datetime(df['PAY_PERIOD_END_DATE']).astype(object)
    df['CHECK_DATE'] = pd.to_datetime(df['CHECK_DATE']).astype(object)
    df['YEAR'] = df['PAY_PERIOD_END_DATE'].dt.year.astype(object)
    
    #inserts payperiod table data
    payperiod_data = df.loc[0,['PAY_PERIOD', 'YEAR', 'PAY_PERIOD_BEGIN_DATE', 'PAY_PERIOD_END_DATE', 'CHECK_DATE']]
    cur.execute(pay_period_table_insert, payperiod_data)
    
    # inserts payroll_type table data
    payroll_type_data = df[['PAYROLL_TYPE']].drop_duplicates()
    for i, row in payroll_type_data.iterrows():
        cur.execute(payroll_type_table_insert, row)
        
    # inserts city table data
    city_data = df[['CITY']].drop_duplicates()
    for i, row in city_data.iterrows():
        cur.execute(city_table_insert, row)
    
    # inserts legislative entity table data
    legislative_entity_data = df[['LEGISLATIVE_ENTITY']].drop_duplicates()
    for i, row in legislative_entity_data.iterrows():
        cur.execute(legislative_entity_insert, row)
    
    # inserts employee table data
    employee_data = df[['EMPLOYEE_NAME', 'EMPLOYEE_TITLE', 'LEGISLATIVE_ENTITY']].drop_duplicates()
    for i, row in employee_data.iterrows():
        cur.execute(employee_table_insert, row)
    
    # inserts office table data
    office_data = df[['OFFICE', 'CITY']].drop_duplicates()
    for i, row in office_data.iterrows():
        cur.execute(office_table_insert, row)
    
    # inserts pay table data
    pay_data = df[['BIWEEKLY/HOURLY_RATE', 'PAY_PERIOD', 'YEAR', 'PAYROLL_TYPE', 'EMPLOYEE_NAME', 'CITY', 'OFFICE', 'LEGISLATIVE_ENTITY', 'EMPLOYEE_TITLE']].drop_duplicates()
    for i, row in pay_data.iterrows():
        cur.execute(pay_table_insert, row)
    
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.csv'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files
    
def process_data(cur, conn, filepath, func):
    """Iterate through all data files"""
    #get all files
    all_files = get_files(filepath)
    
    #total number of file found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # iterate
    for i, datafile in enumerate(all_files, 1):
          func(cur, datafile)
          conn.commit()
          print('{}/{} files processed'.format(i, num_files))

def update_columns(cur, conn):
    """Updates key columns for the necessary tables"""
    for query in update_columns_queries:
        cur.execute(query)
        conn.commit()
            
def delete_duplicates(cur, conn):
    """Deletes all duplicate rows"""
    for query in delete_duplicate_rows_queries:
        cur.execute(query)
        conn.commit()
            
def drops_columns(cur, conn):
    """Drops all temp columns in the DB"""
    for query in drop_column_queries:
        cur.execute(query)
        conn.commit()

def main():
    conn = pg2.connect(database = 'governmentpayroll', user = '### Insert user name here', password = '### Insert password here')
    cur = conn.cursor()
    
    process_data(cur, conn, filepath ='payroll', func = process_payroll_file)
    delete_duplicates(cur, conn)
    update_columns(cur, conn)
    drops_columns(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()