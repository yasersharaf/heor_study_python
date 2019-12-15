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
from IdDxPT_IdRxPT import execute_n_drop, str_to_list, IdDxPT, IdRxPT




#     sql_statement = '''--sql
# CREATE TABLE rds.interest_tx AS 
# SELECT enrolid, svcdate, Sex, DOBYR, AGE, planTyp, Region, Generic_name, daySupp 
# FROM (SELECT a.*, Generic_name from interest_tx_d_cleaned AS a
#                JOIN scd.ndc as b
#                ON a.ndcnum = b.ndc)
# UNION
# SELECT enrolid, svcdate, Sex, DOBYR, AGE, planTyp, Region, Generic_name, daySupp
# FROM (SELECT a.*, b.daysupp, Generic_name from interest_tx_so AS a
#                JOIN scd.hcpcs as b
#                ON a.proc1 = b.hcpcs);'''

#     execute_n_drop(db_conn=s.db, sql_expr=sql_statement, if_exists='replace')

if __name__ == "__main__":
    # pass
    # from studysetup import  heor_study
    s = heor_study(proj_dir=proj_dir, import_raw_sas=False)
    c = s.db.cursor()
    
    execute_n_drop(db_conn=s.db, sql_expr="""CREATE TABLE scd.mytable
                 (start, end, new_score_5)""")
    s.db.commit()
    
    from _01_import_codes import import_codes
    import_codes(study=s)
    
    
    supply_days_var='daysupp'
    ndc_var='ndcnum'
    id_var='enrolid'
    service_date_var='svcdate'
    demoraphic_vars = 'Sex, DOBYR, AGE, planTyp, Region'
    
    from _02_filter_patients import filter_patients
    filter_patients(study=s,
                    supply_days_var='daysupp',
                    ndc_var='ndcnum',
                    proc_var='proc1',
                    id_var='enrolid',
                    service_date_var='svcdate',
                    demoraphic_vars = 'Sex, DOBYR, AGE, planTyp, Region',
                    )
    # s.db.commit()
    
    # c.execute("SELECT * FROM scd.sqlite_master;")
    # print("\n\n scd_fetchal:\n",c.fetchall())
    
    # df = pd.read_sql_query("SELECT * FROM scd.sqlite_master;", s.db)
        
    ##### 02_filter->idDxPt
    # icd9_codes = pd.read_sql_query("SELECT icd9 FROM scd.icd9", s.db).iloc[:,0].tolist()
    # dxVar = 'pdx dx1 dx2'
    # total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o",
    #                     codes=icd9_codes, dxVar=dxVar, stDt=s.study_start, edDt=s.study_end)
    
    
#     proc_var = 'proc1'
#     hcpcs_codes = pd.read_sql_query("SELECT hcpcs FROM scd.hcpcs", s.db).iloc[:,0].tolist()
#     total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s, o",
#                         codes=hcpcs_codes, rxVar=proc_var, stDt=s.study_start, edDt=s.study_end,
#                         outDsn='interest_tx_so')
#     #  739
    
    
#     ndc_var = 'ndcnum'
#     ndc_codes = pd.read_sql_query("SELECT ndc FROM scd.ndc", s.db).iloc[:,0].tolist()
#     total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "d",
#                         codes=ndc_codes, rxVar=ndc_var, stDt=s.study_start, edDt=s.study_end,
#                         outDsn='interest_tx_d')
#     #  2784 -> fix sas
#     # TODO: fix sas
    
    
# sql_interest_tx = f'''
# --sql

# CREATE TABLE rds.interest_tx AS 
# SELECT a.{id_var}, {service_date_var}, {', '.join(str_to_list(demoraphic_vars))}, Generic_name, {service_date_var} 
# FROM interest_tx_d_cleaned AS a
# JOIN scd.ndc as b
# ON a.{ndc_var} = b.ndc

# UNION ALL

# SELECT a.{id_var}, {service_date_var}, {', '.join(str_to_list(demoraphic_vars))}, Generic_name, {service_date_var} 
# FROM interest_tx_so AS a
# JOIN scd.hcpcs as b
# ON a.{proc_var} = b.hcpcs;'''

# execute_n_drop(db_conn=s.db, sql_expr=sql_interest_tx, if_exists='replace')


# # create patient table, patient age, and index date, sex, and the index treatment from interest_tx*/
# sql_index_patient = f'''--sql
# create table rds.allpt as 
# select distinct a.*, DOBYR, AGE
#         , Sex
#         , planTyp
#         , region
#         , Generic_name as idxTreatment FROM (
#                SELECT {id_var}, MIN({service_date_var}) AS idxDate from rds.interest_tx
#                WHERE {service_date_var} BETWEEN '{s.index_start}' AND '{s.index_end}'
#                GROUP BY {id_var}) AS a
# join rds.interest_tx as b
# on b.enrolid = a.enrolid and a.idxDate = b.svcdate;
# '''
# execute_n_drop(db_conn=s.db, sql_expr=sql_index_patient, if_exists='replace')



# s.dbs