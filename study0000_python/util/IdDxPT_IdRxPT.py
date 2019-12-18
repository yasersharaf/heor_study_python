'''********************************************************************************
TOPIC:              Collect datasets' names macro (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 20, 2019
DESCRIPTION:        Get name of specific datasets into a space separated macro!
**********************************************************************************'''
import pandas as pd
import datetime
import sqlite3
import traceback
import sys
import platform
from timeit import default_timer as timer



class Formatting:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
def fprint(*args, text_format = Formatting.BOLD+Formatting.UNDERLINE):
    print(text_format, end='')
    print(*args)
    print(Formatting.END, end='')
    

# Supply Days Cleaning using PROC SQL
def clean_supply_days(db_conn=None, inDsn=None, outDsn=None,
                         supply_days_var='daysupp', ndc_var='ndcnum',
                         id_var='enrolid', service_date_var='svcdate',
                         demoraphic_vars=''):
    demoraphic_vars = str_to_list(demoraphic_vars)
#     sql_statment_1_a = f'''--sql 
# CREATE TABLE _adjust_daysupp_step_1_a AS 
# SELECT *
# FROM {inDsn}
# ORDER BY {id_var}, {service_date_var}, {ndc_var} ASC, {supply_days_var} DESC;
# '''
#     execute_n_drop(db_conn=db_conn, sql_expr=sql_statment_1_a, if_exists='replace')
    
    sql_statement_1_b = f'''--sql
CREATE TABLE _adjust_daysupp_step_1_b AS 
SELECT * FROM (
    SELECT {id_var}, {service_date_var}, {ndc_var}, {supply_days_var}, 
            {', '.join(demoraphic_vars)},
            ROW_NUMBER() OVER (
                            PARTITION BY {id_var}, {service_date_var}, {ndc_var}
                            ORDER BY {id_var}, {service_date_var}, {ndc_var} ASC, {supply_days_var} DESC
                            )  AS N
    FROM {inDsn}
    )
WHERE N=1;
'''
    execute_n_drop(db_conn=db_conn, sql_expr=sql_statement_1_b, if_exists='replace')

    
    
    sql_statement_2_a = f'''--sql
CREATE TABLE _adjust_daysupp_step_2_a AS 
SELECT {ndc_var}, {supply_days_var}, COUNT(*) as freq
FROM _adjust_daysupp_step_1_b
WHERE {supply_days_var}>0
GROUP BY {ndc_var}, {supply_days_var}
ORDER BY {ndc_var}, freq DESC;
'''
    execute_n_drop(db_conn=db_conn, sql_expr=sql_statement_2_a, if_exists='replace')
       
    sql_statement_2_b = f'''--sql
CREATE TABLE _adjust_daysupp_step_2_b AS 
SELECT {ndc_var}, {supply_days_var} FROM (
    SELECT {ndc_var}, {supply_days_var}, freq, ROW_NUMBER() OVER(
                                PARTITION BY {ndc_var}
                                ORDER BY {ndc_var}, freq DESC
                                ) AS N
    FROM _adjust_daysupp_step_2_a
    ) AS a
WHERE N = 1;
''' 

    execute_n_drop(db_conn=db_conn, sql_expr=sql_statement_2_b, if_exists='replace')

    sql_statement_out = f'''--sql
CREATE TABLE {outDsn} AS 
SELECT a.{id_var}, a.{service_date_var}, a.{ndc_var}, 
                CASE
                    WHEN a.{supply_days_var} > 0 THEN a.{supply_days_var}
                    ELSE b.{supply_days_var}
            END AS {supply_days_var},
       {', '.join(demoraphic_vars)}            
        
FROM _adjust_daysupp_step_1_b AS a
INNER JOIN _adjust_daysupp_step_2_b AS b
ON a.{ndc_var} = b.{ndc_var};
'''
    execute_n_drop(db_conn=db_conn, sql_expr=sql_statement_out, if_exists='replace')

def low_case_str_list(l):
    assert  isinstance(l, list)
    return [item.lower() for item in l]

def str_to_list(str_or_list=[]):
    if isinstance(str_or_list, str):
        str_or_list = str_or_list.replace(",", " ")
        str_or_list = str_or_list.split()
    return str_or_list

def execute_n_drop(db_conn=None, sql_expr="", if_exists='replace', display=True):
    if display:
        print("Executing ",sql_expr)
        
    execution_time = timer()
    
    sql_expr_lines = [line for line in sql_expr.split("\n") if line.strip()[0:2] != '--']
    sql_expr = "\n".join(sql_expr_lines)
    sql_expr = " ".join(sql_expr.split())
    sql_expr_upper = sql_expr.upper()
    try:
        db_conn.execute(sql_expr)
    except sqlite3.OperationalError:
        # removing the comments
        if "CREATE TABLE IF NOT EXISTS" not in sql_expr_upper:
            if "CREATE TABLE" in sql_expr_upper:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                # traceback.print_exception(exc_type, exc_value, exc_traceback,
                #               limit=1, file=sys.stdout)
                # traceback.print_exc(file=sys.stderr)
                fprint(f'''Warning: Handling an exception of {str(exc_type)}\n because {str(exc_value)}''')
                if "already exists" in  str(exc_value):
                    new_table_name_upcase = sql_expr_upper.split("CREATE TABLE")[1].strip().split()[0]
                    tb_name_location = sql_expr_upper.find(new_table_name_upcase)
                    new_table_name = sql_expr[tb_name_location:].split()[0]
                    if f"DROP TABLE IF EXISTS {new_table_name_upcase}" not in sql_expr_upper:
                        print(f"""Note: Consider executing the expression \n "DROP TABLE IF EXISTS {new_table_name};"  """)
                        if if_exists == 'replace':
                            sql_expr_drop = f"DROP TABLE IF EXISTS {new_table_name};"
                            print(f"Ececuting \n{sql_expr_drop}\n first since if_exist={if_exists}.")
                            db_conn.execute(sql_expr_drop)
                            db_conn.execute(sql_expr)
                else:
                    traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stdout)
                    raise sqlite3.OperationalError
    if "CREATE TABLE" in sql_expr_upper:
        new_table_name_upcase = sql_expr_upper.split("CREATE TABLE")[1].strip().split()[0]
        tb_name_location = sql_expr_upper.find(new_table_name_upcase)
        new_table_name = sql_expr[tb_name_location:].split()[0]
        num_rows = pd.read_sql_query(f'SELECT count(*) FROM {new_table_name}',db_conn).iloc[0,0]
        print(f'Table {new_table_name} now containts {num_rows} observations'.center(90,'=').center(110) )
        execution_time = timer() - execution_time
        execution_message = f"The operation took {execution_time:.3g} seconds"
        print(f'{execution_message:>100}')
        return num_rows
    
    
        
               

def get_union_columns(db_conn=None, dbLib=None, table_list=None):
    all_columns = []
    for table_name in table_list:
        print(table_name)
        table_col = sql_list_table_columns(db_conn=db_conn, db_tb_name=f'{dbLib}.{table_name}')
        all_columns = all_columns + \
            [col.lower() for col in table_col if col.lower() not in all_columns]
    return all_columns

def sql_list_table_columns(db_conn=None, db_tb_name=None):
    '''
    get a list of table's columns as a python list
    '''
    cursor = db_conn.execute(f'SELECT * FROM {db_tb_name} LIMIT 0')
    list_columns = list(next(zip(*cursor.description)))
    return list_columns


def get_table_name(db_conn=None, dbLib='raw', dbList="ccae,mdcr", scope=None):

    # TODO: , stDt=None, edDt=None

    dbList_as_list = str_to_list(dbList)
    print("Lokking into following dbLists: ",dbList_as_list)
    scope = str_to_list(scope)
    
    df_tables = pd.read_sql_query(f"SELECT * FROM {dbLib}.sqlite_master WHERE type='table';", db_conn)
    all_tables = df_tables[['name']].applymap(lambda x: x.replace('\\','.').split('.')[-1])['name'].tolist()
    table_list = [ds for ds in all_tables if (ds[0:4] in dbList_as_list and ds[4] in scope)]
    return table_list

#IdDxPT record extraction *******************'''
def IdDxPT(db_conn=None ,dbLib = None, dbList = "ccae,mdcr", scope = "s,o",
           service_date_var='svcdate', stDt=None, edDt=None,
           dxVar = None, codes=None,  outDsn='outDsn'):

    table_list = get_table_name(db_conn=db_conn, dbLib=dbLib, scope=scope)
    
    print("codes:", codes)

    print(f"========= LISTING THE DATA SETS FROM",{dbLib},"LIBRARY FOR APPENDING ==========\n", *table_list)
    if stDt is None:
        stDt = pd.Timestamp(year=1900,month=1,day=1)
    if edDt is None:
        edDt = pd.Timestamp(year=2070,month=1,day=1)
    
    sql_select_list = []
    #Append the datasets*/
    if len(table_list) > 0:   
        sql_args = {'table_alias': '',
                    'column_prefix': '',
                    'on_clause': ''}
        all_columns = get_union_columns(db_conn=db_conn, dbLib=dbLib, table_list=table_list)
        if codes or dxVar:
            if not (codes and dxVar):
                raise ValueError('Either both dxVar and codes must be provided or none.')
            codes = str_to_list(codes)
            dxVar = str_to_list(dxVar)
            df_code = pd.DataFrame(codes, columns = ['dx'])
            df_code_ds = 'dx_list'
            df_code.to_sql(df_code_ds, db_conn, if_exists='replace')
            
        for table_name, table_num in zip(table_list, range(len(table_list))):
            if codes:
                table_col = sql_list_table_columns(db_conn=db_conn, db_tb_name=f'{dbLib}.{table_name}')
                sql_args['column_prefix'] = 'a' + str(table_num) + '.'
                sql_args['table_alias'] = 'AS ' + sql_args['column_prefix'][0:-1]
                sql_args['on_clause'] = 'ON ' + ' OR '.join([f"instr({sql_args['column_prefix']}{join_col},b{table_num}.dx) > 0" \
                                                             for join_col in [col.lower() for col in dxVar if col.lower() in [col2.lower() for col2 in table_col]]])
                sql_args['join clause'] = f'INNER JOIN {df_code_ds} AS b{table_num}' 
            sql_select_list.append(\
                f'''SELECT {', '.join([sql_args['column_prefix'] + col if col in [col2.lower() for col2 in table_col] 
                                                                       else '   NULL AS ' + col  for col in all_columns])},
'{dbLib}.{table_name}' as tb_name FROM {dbLib}.{table_name} {sql_args['table_alias']}
                {sql_args['join clause']}
                {sql_args['on_clause']}                                                       
                WHERE {service_date_var} >= '{stDt}'  AND {service_date_var} <= '{edDt}'
            ''')


        sql_id_dx = f'CREATE TABLE {outDsn} AS ' + 'UNION ALL\n'.join(sql_select_list)
        # print(sql_id_dx)

        execute_n_drop(db_conn=db_conn, sql_expr=sql_id_dx , if_exists='replace')
        total_rows = pd.read_sql_query(f"SELECT count(*) FROM {outDsn} ;", db_conn)



        # print(f'''Retrieved {list(total_rows.values)[0]} evaluated by {total_rows.columns[0]}
        #             in {execution_time:.3g} seconds''' )
        

        return total_rows

        



# #IdRxPT record extraction *******************'''
def IdRxPT(db_conn=None ,dbLib = None, dbList = "ccae,mdcr", scope = "d",
           service_date_var='svcdate', stDt=None, edDt=None,
           rxVar = None, codes=None,  outDsn='outDsn'):

    table_list = get_table_name(db_conn=db_conn, dbLib=dbLib, scope=scope)
    
    print("codes:", codes)

    print(f"========= LISTING THE DATA SETS FROM",{dbLib},"LIBRARY FOR APPENDING ==========\n", *table_list)
    if stDt is None:
        stDt = pd.Timestamp(year=1970,month=1,day=1)
    if edDt is None:
        edDt = pd.Timestamp(year=2070,month=1,day=1)
    
    sql_select_list = []
    all_columns = []
    table_col = []
    #Append the datasets*/
    if len(table_list) > 0:

        for table_name in table_list:
            print(table_name)
            table_col = sql_list_table_columns(db_conn=db_conn, db_tb_name=f'{dbLib}.{table_name}')
            all_columns = all_columns + \
                [col.lower() for col in table_col if col.lower() not in all_columns]     
        sql_args = {'table_alias': '',
                    'column_prefix': '',
                    'on_clause': ''}
    
        if codes or rxVar:
            if not (codes and rxVar):
                raise ValueError('Either both rxVar and codes must be provided or none.')
            codes = str_to_list(codes)
            rxVar = str_to_list(rxVar)
            df_code = pd.DataFrame(codes, columns = ['rx'])
            df_code_ds = 'rx_list'
            df_code.to_sql(df_code_ds, db_conn, if_exists='replace')

        for table_name, table_num in zip(table_list, range(len(table_list))):
            if codes:
                table_col = sql_list_table_columns(db_conn=db_conn, db_tb_name=f'{dbLib}.{table_name}')
                sql_args['column_prefix'] = 'a' + str(table_num) + '.'
                sql_args['table_alias'] = 'AS ' + sql_args['column_prefix'][0:-1]
                on_conditions = [f"{sql_args['column_prefix']}{join_col}=b{table_num}.rx" \
                                                             for join_col in [col.lower() for col in rxVar if col.lower() in [col2.lower() for col2 in table_col]]]
                sql_args['on_clause'] = 'ON ' + ' OR '.join(on_conditions)
                sql_args['join clause'] = f'INNER JOIN {df_code_ds} AS b{table_num}' 
            sql_select_list.append(\
                f'''SELECT {', '.join([sql_args['column_prefix'] + col if col in [col2.lower() for col2 in table_col] 
                                                                       else '   NULL AS ' + col  for col in all_columns])},
'{dbLib}.{table_name}' as tb_name FROM {dbLib}.{table_name} {sql_args['table_alias']}
                {sql_args['join clause']}
                {sql_args['on_clause']}                                                       
                WHERE SVCDATE >= '{stDt}'  AND SVCDATE <= '{edDt}'
            ''')


        sql_id_rx = f'CREATE TABLE {outDsn} AS ' + 'UNION ALL\n'.join(sql_select_list)
        print(sql_id_rx)

        from timeit import default_timer as timer
        execution_time = timer()
        # db_conn.execute(sql_id_rx)

        execute_n_drop(db_conn=db_conn, sql_expr=sql_id_rx , if_exists='replace')
        total_rows = pd.read_sql_query(f"SELECT count(*) FROM {outDsn} ;", db_conn)

        execution_time = timer() - execution_time
        assert isinstance(total_rows, pd.DataFrame)

        print(f'''Retrieved {list(total_rows.values)[0]} evaluated by {total_rows.columns[0]}
                    in {execution_time:.3g} seconds''' )
        

        return total_rows    


'''    PROC SQL NOPRINT;
    %put dblist is  %UPCASE(&dbList);
    SELECT "&dbLib.."||MEMNAME INTO: LIST_NAME SEPARATED BY ' ' FROM DICTIONARY.TABLES
    WHERE UPCASE(LIBNAME)=UPCASE("&dbLib")
        AND UPCASE(SUBSTR(MEMNAME,5,1)) IN &SCOPE
        %if &stDt ne %then %do; AND SUBSTR(MEMNAME,6,2) >= substr(strip(put(year(&stDt),4.)),3,2) %end;
        %if &edDt ne %then %do; AND SUBSTR(MEMNAME,6,2) <= substr(strip(put(year(&edDt),4.)),3,2) %end;
        %if &dbList ne %then %do; AND UPCASE(SUBSTR(MEMNAME,1,4)) IN %UPCASE(&dbList) %end;

    ORDER BY MEMNAME DESC
    ;;
    QUIT;
    %put ========= LISTING THE DATA SETS FROM &dbLib LIBRARY FOR APPENDING ==========&LIST_NAME;
    %put &LIST_NAME;
    #Append the datasets*/
    data &outDsn;
    set &LIST_NAME indsname = source; 
    where 1
    %if &stDt ne %then %do; and &stDt <= svcDate %end;
    %if &edDt ne %then %do; and svcDate <= &edDt %end;
    ;
#add a variable indicating table name and its scope */
    tbname = scan(source,2,'.');
    scope = substr(tbname,5,1);
    %if &code ne %then %do;
    if upcase(SCOPE) in ("O","S") then do;
        if proc1 in (&&&code) then output;
    end; sql_id_dx
    if upcase(SCOPE) in ("D") then do;
        if ndcnum in (&&&code) then output;
    end; 
    %end;
    run;
%MEND; '''



# #IdAdmPT record extraction *******************'''
# %MACRO IdAdmPT(dbLib = , dbList = ("CCAE","MDCR"), stDt =, edDt =, dxVar =, code=,  outDsn =);

#     %local SCOPE;
#     %let SCOPE = ("I");
#     PROC SQL NOPRINT;
#     %put dblist is  %UPCASE(&dbList);
#     SELECT "&dbLib.."||MEMNAME INTO: LIST_NAME SEPARATED BY ' ' FROM DICTIONARY.TABLES
#     WHERE UPCASE(LIBNAME)=UPCASE("&dbLib")
#         AND UPCASE(SUBSTR(MEMNAME,5,1)) IN &SCOPE
#         %if &stDt ne %then %do; AND SUBSTR(MEMNAME,6,2) >= substr(strip(put(year(&stDt),4.)),3,2) %end;
#         %if &edDt ne %then %do; AND SUBSTR(MEMNAME,6,2) <= substr(strip(put(year(&edDt),4.)),3,2) %end;
#         %if &dbList ne %then %do; AND UPCASE(SUBSTR(MEMNAME,1,4)) IN %UPCASE(&dbList) %end;

#     ORDER BY MEMNAME DESC
#     ;;
#     QUIT;
#     %put ========= LISTING THE DATA SETS FROM &dbLib LIBRARY FOR APPENDING ==========&LIST_NAME;
#     %put &LIST_NAME;
#     #Append the datasets*/
#     data &outDsn;
#     set &LIST_NAME indsname = source; 
#     %if &code ne %then %do;array dxvar_array &dxVar;%end;
#     where 1
#     %if &stDt ne %then %do; and &stDt <= coalesce(disdate,admdate) %end;
#     %if &edDt ne %then %do; and admDate <= &edDt %end;
#     ;
#     %if &code ne and &dxVar ne %then %do;
#     do i = 1 to dim(dxvar_array);
# #check ruleout?*/
#         if dxvar_array{i} in:  &code then output;
#         leave;
#     end;
#     %end;
# #add a variable indicating table name and its scope */
#     tbname = scan(source,2,'.');
#     scope = substr(tbname,5,1);
#     run;
# %MEND;



# #IdAdmPT record extraction *******************'''
# %MACRO IdCEPT(dbLib = , dbList = ("CCAE","MDCR"), stDt =, edDt =, outDsn =);

#     %local SCOPE;
#     %let SCOPE = ("T");
#     PROC SQL NOPRINT;
#     %put dblist is  %UPCASE(&dbList);
#     SELECT "&dbLib.."||MEMNAME INTO: LIST_NAME SEPARATED BY ' ' FROM DICTIONARY.TABLES
#     WHERE UPCASE(LIBNAME)=UPCASE("&dbLib")
#         AND UPCASE(SUBSTR(MEMNAME,5,1)) IN &SCOPE
#         %if &stDt ne %then %do; AND SUBSTR(MEMNAME,6,2) >= substr(strip(put(year(&stDt),4.)),3,2) %end;
#         %if &edDt ne %then %do; AND SUBSTR(MEMNAME,6,2) <= substr(strip(put(year(&edDt),4.)),3,2) %end;
#         %if &dbList ne %then %do; AND UPCASE(SUBSTR(MEMNAME,1,4)) IN %UPCASE(&dbList) %end;

#     ORDER BY MEMNAME DESC
#     ;;
#     QUIT;
#     %put ========= LISTING THE DATA SETS FROM &dbLib LIBRARY FOR APPENDING ==========&LIST_NAME;
#     %put &LIST_NAME;
#     #Append the datasets*/
#     data &outDsn;
#     set &LIST_NAME indsname = source; 
#     where 1
#     %if &stDt ne %then %do; and &stDt <= dtend %end;
#     %if &edDt ne %then %do; and dtstart <= &edDt %end;
#     ;
# #add a variable indicating table name and its scope */
#     tbname = scan(source,2,'.');
#     scope = substr(tbname,5,1);
#     run;
# %MEND;

