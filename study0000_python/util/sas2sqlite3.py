# -*- coding: utf-8 -*-
'''*********************************************************************************
# TOPIC:              --- (Project's name)
# WRITTEN BY:         YASER GHAEDSHARAF
# DATE:               Fri Nov 22 22:52:04 2019
# DESCRIPTION:        This script is intended to
**********************************************************************************'''

import pandas as pd
import sqlite3




#% %
def sql_list_tables(db_conn=None):
    '''
    get a list of tables as a list by performing for example the following
    list_tables(db=conn)['name'].tolist()
    '''
    list_tables = pd.read_sql_query('''SELECT name FROM sqlite_master
                                     WHERE type='table' ''', db_conn)['name'].tolist()
    return list_tables


def sql_list_table_columns(db_conn=None, tb_name=None):
    '''
    get a list of table's columns as a python list
    '''
    cursor = db_conn.execute('SELECT * FROM ' + tb_name +' limit 0')
    list_columns = list(next(zip(*cursor.description)))
    return list_columns

def sql_column_dtypes(db_conn=None, tb_name=None):
    '''
    returns a data frame with sql tables column names and data types
    '''
    return pd.read_sql_query("SELECT * FROM pragma_table_info('{}')".format(tb_name), db_conn)


def pd_fix_date(df, inplace=False):
    dtypes = pd.DataFrame(df.dtypes, columns=['dtype']).reset_index()
    date_columns = dtypes[dtypes['dtype'] == '<M8[ns]']['index']
    if inplace:
        for col in date_columns:
#            df[col] = df[col].map(lambda x: x.date())
            df[col] = pd.to_datetime(df[col]).sub(pd.Timestamp('1960-01-01')).dt.days
    else:
        print(date_columns)
        df_out=df
        for col in date_columns:
#            df_out[col] = df_out[col]
            print(col)
            df_out[col] = pd.to_datetime(df[col]).sub(pd.Timestamp('1960-01-01')).dt.days
        return df_out


# %%



chunksize = 1*10**4
df_pde = pd.read_sas(sas_file, encoding='latin-1', chunksize=chunksize)

if_exists = 'replace'
read_so_far = 0
for df in df_pde:
    # print(df.info())
    df.to_sql('df_pde', conn, if_exists=if_exists, index=False)
    if_exists = 'append'
    read_so_far += df.shape[0]
    print('Has read {} records to sql table'.format(read_so_far))

# indexing the table
c = conn.cursor()
c.execute('''--begin-sql
    CREATE INDEX id_index
    ON df_pde(DESYNPUF_ID, PDE_ID);'''.\
    replace('--sql',''))





df.to_sql('new_table_name', conn, if_exists='replace', index=False)





# %%
import pandas as pd
import sqlite3


conn = sqlite3.connect('C:/Users/ghaedya1/Desktop/test.sqlite')
df2 = pd.read_sql_query('''
                           SELECT * FROM new_table_name where PROD_SRVC_ID= '58016024845'
                        '''
                           , conn)
df3 = pd.read_sql_query("SELECT * FROM pragma_table_info('new_table_name')", conn)


# %%
