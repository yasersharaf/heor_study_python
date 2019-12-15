# '''********************************************************************************
# TOPIC:              FILTER PATIENTS (PHASE IV - HEOR)
# WRITTEN BY:         YASER GHAEDSHARAF
# DATE:               AUGUST 20, 2019
# DESCRIPTION:        The purpose of the script is to find the records of the patients with
#                     a record of biologics of interest. Then find the index date of patients
#                     And lastly, save the data sets.
# MODIFICATION:       Use the existing table extraction macros with suggested macro statements
# *********************************************************************************'''

import pandas as pd
import sqlite3
from studysetup import heor_study
from IdDxPT_IdRxPT import execute_n_drop, str_to_list, IdDxPT, IdRxPT, clean_supply_days



def filter_patients(study=None,
                    supply_days_var='daysupp',
                    ndc_var='ndcnum',
                    proc_var='proc1',
                    id_var='enrolid',
                    service_date_var='svcdate',
                    demoraphic_vars = '',
                    ):
    assert isinstance(study, heor_study) 

    # identify use of use of drugs of interest with HCPCS
    hcpcs_codes = pd.read_sql_query("SELECT hcpcs FROM scd.hcpcs", study.db).iloc[:,0].tolist()
    total_rows = IdRxPT(db_conn=study.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s, o",
                        codes=hcpcs_codes, rxVar=proc_var, 
                        service_date_var='svcdate', stDt=study.study_start, edDt=study.study_end,
                        outDsn='interest_tx_so')
    #  739
    
# identify use of use of drugs of interest with NDC codes    
    ndc_codes = pd.read_sql_query("SELECT ndc FROM scd.ndc", study.db).iloc[:,0].tolist()
    total_rows = IdRxPT(db_conn=study.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "d",
                        codes=ndc_codes, rxVar=ndc_var, 
                        service_date_var='svcdate', stDt=study.study_start, edDt=study.study_end,
                        outDsn='interest_tx_d')
    # 2784 -> fix sas
    # TODO: fix sas
    
    # clean supply dates     
    clean_supply_days(db_conn=study.db, inDsn='interest_tx_d', outDsn='interest_tx_d_cleaned',
                         supply_days_var=supply_days_var, ndc_var=ndc_var,
                         id_var=id_var, service_date_var=service_date_var,
                         demoraphic_vars=demoraphic_vars)


    # create interestTx by merging records of HCPCS ans NDC


    sql_interest_tx = f'''
--sql

CREATE TABLE rds.interest_tx AS 
SELECT a.{id_var}, {service_date_var}, {', '.join(str_to_list(demoraphic_vars))}, Generic_name, {service_date_var} 
FROM interest_tx_d_cleaned AS a
JOIN scd.ndc as b
ON a.{ndc_var} = b.ndc

UNION ALL

SELECT a.{id_var}, {service_date_var}, {', '.join(str_to_list(demoraphic_vars))}, Generic_name, {service_date_var} 
FROM interest_tx_so AS a
JOIN scd.hcpcs as b
ON a.{proc_var} = b.hcpcs;'''

    execute_n_drop(db_conn=study.db, sql_expr=sql_interest_tx, if_exists='replace')
    # 3473 




    # create patient table, patient age, and index date, sex, and the index treatment from interest_tx*/
    sql_index_patient = f'''--sql
create table rds.allpt as 
select distinct a.*, DOBYR, AGE
        , Sex
        , planTyp
        , region
        , Generic_name as idxTreatment FROM (
               SELECT {id_var}, MIN({service_date_var}) AS idxDate from rds.interest_tx
               WHERE {service_date_var} BETWEEN '{study.index_start}' AND '{study.index_end}'
               GROUP BY {id_var}) AS a
join rds.interest_tx as b
on b.enrolid = a.enrolid and a.idxDate = b.svcdate;
'''
    execute_n_drop(db_conn=study.db, sql_expr=sql_index_patient, if_exists='replace')
