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
    
    # inserts payroll_type table
    payroll_type_data = df[['PAYROLL_TYPE']]
    for i, row in payroll_type_data.iterrows():
        cur.execute(payroll_type_table_insert, row)


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
    
def main():
    conn = pg2.connect(database = 'governmentpayroll', user = 'postgres', password = 'poop1234')
    cur = conn.cursor()
    
    process_data(cur, conn, filepath ='payroll', func = process_payroll_file)
    conn.close()

if __name__ == "__main__":
    main()