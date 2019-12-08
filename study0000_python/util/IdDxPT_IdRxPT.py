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


def execute_n_drop(conn_or_cur=None, sql_expr="", if_exists='replace'):
    try:
        conn_or_cur.execute(sql_expr)
    except sqlite3.OperationalError:
        traceback.print_exc(file=sys.stderr)
        sql_expr = " ".join(sql_expr.split())
        sql_expr_upper = sql_expr.upper()
        if "CREATE TABLE IF EXISTS" not in sql_expr_upper:
            if "CREATE TABLE" in sql_expr_upper:
                new_table_name_upcase = sql_expr_upper.split("CREATE TABLE")[1].strip().split()[0]
                tb_name_location = sql_expr_upper.find(new_table_name_upcase)
                new_table_name = sql_expr[tb_name_location:].split()[0]
                if f"DROP TABLE IF EXISTS {new_table_name_upcase}" not in sql_expr_upper:
                    print(f"""Note: Consider executing the expression \n "DROP TABLE IF EXISTS {new_table_name};"  """)
                    if if_exists == 'replace':
                        sql_expr_drop = f"DROP TABLE IF EXISTS {new_table_name};"
                        print(f"Ececuting: \n{sql_expr_drop}\n first.")
                        conn_or_cur.execute(sql_expr_drop)
                        conn_or_cur.execute(sql_expr)


def sql_list_table_columns(db_conn=None, db_tb_name=None):
    '''
    get a list of table's columns as a python list
    '''
    cursor = db_conn.execute(f'SELECT * FROM {db_tb_name} LIMIT 0')
    list_columns = list(next(zip(*cursor.description)))
    return list_columns


def get_table_name(db_conn=None, dbLib='raw', dbList="ccae,mdcr", scope=None):
    # TODO: , stDt=None, edDt=None


    if isinstance(dbList,str):
        dbList = dbList.split(',')
    if isinstance(scope,str):
        scope = scope.split(',')
    
    df_tables = pd.read_sql_query(f"SELECT * FROM {dbLib}.sqlite_master WHERE type='table';", db_conn)
    table_list = [ds for ds in df_tables['name'].tolist() if (ds[0:4] in dbList and ds[4] in scope)]
    return table_list

#IdDxPT record extraction *******************'''
def IdDxPT(db_conn=None ,dbLib = None, dbList = "ccae,mdcr", scope = "s,o", stDt=None, edDt=None,\
           dxVar = None, codes=None,  outDsn='outDsn'):
    # TODO: Add dxVar condition
    table_list = get_table_name(db_conn=db_conn, dbLib=dbLib, scope=scope)
    
    print("codes:", codes)

    # , stDt = stDt, edDt = edDt
    print(f"========= LISTING THE DATA SETS FROM",{dbLib},"LIBRARY FOR APPENDING ==========\n", *table_list)
    if stDt is None:
        stDt = pd.Timestamp(year=1970,month=1,day=1)
    if edDt is None:
        edDt = pd.Timestamp(year=2070,month=1,day=1)
    
    sql_select_list = []
    all_columns = []
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
    
        if codes:
            if not dxVar:
                raise ValueError('Either both dxVar and codes must be provided or none.')
            if isinstance(codes,str):
                codes = codes.split()
            if isinstance(dxVar,str):
                dxVar = dxVar.split()
            df_code = pd.DataFrame(codes, columns = ['dx'])
            df_code_ds = 'dx_list'
            df_code.to_sql(df_code_ds, db_conn, if_exists='replace')
            
        for table_name,table_num in zip(table_list,range(len(table_name))):
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
                WHERE SVCDATE >= '{stDt}'  AND SVCDATE <= '{edDt}'
            ''')

        

        sql_id_dx = f'CREATE TABLE {outDsn} AS ' + 'UNION ALL\n'.join(sql_select_list)
        print(sql_id_dx)

        from timeit import default_timer as timer
        execution_time = timer()
        # db_conn.execute(sql_id_dx)

        execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_id_dx , if_exists='replace')
        total_rows = pd.read_sql_query(f"SELECT count(*) FROM {outDsn} ;", db_conn)

        execution_time = timer() - execution_time
        assert isinstance(total_rows, pd.DataFrame)

        print(f'''Retrieved {list(total_rows.values)[0]} evaluated by {total_rows.columns[0]}
                    in {execution_time:.3g} seconds''' )
        

        return total_rows

        



# #IdRxPT record extraction *******************'''
# %MACRO IdRxPT(dbLib = , dbList = ("CCAE","MDCR"), SCOPE = , stDt =, edDt =, code = ,  outDsn =);
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
#     %if &stDt ne %then %do; and &stDt <= svcDate %end;
#     %if &edDt ne %then %do; and svcDate <= &edDt %end;
#     ;
# #add a variable indicating table name and its scope */
#     tbname = scan(source,2,'.');
#     scope = substr(tbname,5,1);
#     %if &code ne %then %do;
#     if upcase(SCOPE) in ("O","S") then do;
#         if proc1 in (&&&code) then output;
#     end; 
#     if upcase(SCOPE) in ("D") then do;
#         if ndcnum in (&&&code) then output;
#     end; 
#     %end;
#     run;
# %MEND;



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

