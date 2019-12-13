# importing standard python modules
import sqlite3
import pandas as pd
import sys
import os


#########################################################################################
#                                   define directory
#########################################################################################

# The project directory will be defined automatically


file_dir = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.realpath(file_dir+"/../../")
util_dir = os.path.join(proj_dir,'util')
sys.path.append(util_dir)
print(f"The study directory is {proj_dir}")


from studysetup import heor_study
from IdDxPT_IdRxPT import execute_n_drop, IdDxPT, IdRxPT


#Supply Days Cleaning using PROC SQL
def clean_daysupp_sql(db_conn=None, inDsn=None, outDsn=None,
                         supply_days='daysupp', ndcnum='ndcnum',
                         id_var='enrolid', service_date_var='svcdate'):
    sql_statment_1_a = f'''--sql 
CREATE TABLE _adjust_daysupp_step_1_a AS 
SELECT *
FROM {inDsn}
ORDER BY {id_var}, {service_date_var}, {ndcnum} ASC, {supply_days} DESC;
'''
    execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_statment_1_a, if_exists='replace')
    
    sql_statement_1_b = f'''--sql
CREATE TABLE _adjust_daysupp_step_1_b AS 
SELECT {id_var}, {service_date_var}, {ndcnum}, {supply_days}, ROW_NUMBER() OVER (
                        PARTITION BY {id_var}, {service_date_var}, {ndcnum}
                        ORDER BY {id_var}, {service_date_var}, {ndcnum} ASC, {supply_days} DESC
                        )  AS N
FROM _adjust_daysupp_step_1_a;
'''
    execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_statement_1_b, if_exists='replace')

    
    
    sql_statement_2_a = f'''--sql
CREATE TABLE _adjust_daysupp_step_2_a AS 
SELECT {ndcnum}, {supply_days}, COUNT(*) as freq
FROM _adjust_daysupp_step_1_b
WHERE {supply_days}>0
GROUP BY {ndcnum}, {supply_days}
ORDER BY {ndcnum}, freq DESC;
'''
    execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_statement_2_a, if_exists='replace')
       
    sql_statement_2_b = f'''--sql
CREATE TABLE _adjust_daysupp_step_2_b AS 
SELECT {ndcnum}, {supply_days} FROM (
    SELECT {ndcnum}, {supply_days}, freq, ROW_NUMBER() OVER(
                                PARTITION BY {ndcnum}
                                ORDER BY {ndcnum}, freq DESC
                                ) AS N
    FROM _adjust_daysupp_step_2_a
    ) AS a
WHERE N = 1;
''' 

    execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_statement_2_b, if_exists='replace')

    sql_statement_out = f'''--sql
CREATE TABLE {outDsn} AS 
SELECT a.{id_var}, a.{service_date_var}, a.{ndcnum}, 
                CASE
                    WHEN a.{supply_days} > 0 THEN a.{supply_days}
                    ELSE b.{supply_days}
            END AS {supply_days}
FROM _adjust_daysupp_step_1_b AS a
INNER JOIN _adjust_daysupp_step_2_b AS b
ON a.{ndcnum} = b.{ndcnum};
'''
    execute_n_drop(conn_or_cur=db_conn, sql_expr=sql_statement_out, if_exists='replace')

if __name__ == "__main__":
    # pass
    # from studysetup import  heor_study
    s = heor_study(proj_dir=proj_dir, import_raw_sas=False)
    c = s.db.cursor()
    
    execute_n_drop(conn_or_cur=c, sql_expr="""CREATE TABLE scd.mytable
                 (start, end, new_score_5)""")
    s.db.commit()
    
    from _01_import_codes import import_codes
    import_codes(study=s)
    s.db.commit()
    
    c.execute("SELECT * FROM scd.sqlite_master;")
    print("\n\n scd_fetchal:\n",c.fetchall())
    
    df = pd.read_sql_query("SELECT * FROM scd.sqlite_master;", s.db)
    
    from IPython.display import display, HTML
    display(HTML(df.to_html()))
    print(df)
    
    ##### 02_filter->idDxPt
    # icd9_codes = pd.read_sql_query("SELECT icd9 FROM scd.icd9", s.db).iloc[:,0].tolist()
    # dxVar = 'pdx dx1 dx2'
    # total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o",
    #                     codes=icd9_codes, dxVar=dxVar, stDt=s.study_start, edDt=s.study_end)
    
    
    procVar = 'proc1'
    hcpcs_codes = pd.read_sql_query("SELECT hcpcs FROM scd.hcpcs", s.db).iloc[:,0].tolist()
    total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s, o",
                        codes=hcpcs_codes, rxVar=procVar, stDt=s.study_start, edDt=s.study_end,
                        outDsn='interestTX_SO')
    #  739
    
    
    ndcVar = 'ndcnum'
    ndc_codes = pd.read_sql_query("SELECT ndc FROM scd.ndc", s.db).iloc[:,0].tolist()
    total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "d",
                        codes=ndc_codes, rxVar=ndcVar, stDt=s.study_start, edDt=s.study_end,
                        outDsn='interestTX_D')
    #  2784 -> fix sas
    # TODO: fix sas
    
    
    clean_daysupp_sql(db_conn=s.db, inDsn='interestTX_D', outDsn='outDsn',
                         supply_days='daysupp', ndcnum='ndcnum',
                         id_var='enrolid', service_date_var='svcdate')
    



