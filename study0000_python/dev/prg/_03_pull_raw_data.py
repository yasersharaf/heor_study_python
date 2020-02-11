'''********************************************************************************
TOPIC:              PULL RAW DATA TABLES (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 20, 2019
DESCRIPTION:        The purpose of the script is to pull different tables of marketscan
                    data during the study period.
MODIFICATION:       Used suggested macro statements instead of previously used macro statements 
**********************************************************************************'''

import pandas as pd
from IdDxPT_IdRxPT import execute_n_drop, str_to_list, IdDxPT, IdRxPT, IdCEPT, clean_supply_days
from studysetup import heor_study

def pull_raw_data(study=None,
                  id_var='enrolid'
                  ):
    assert isinstance(study, heor_study)

    icd9_codes = pd.read_sql_query("SELECT icd9 FROM scd.icd9", study.db).iloc[:,0].tolist()
    dxVar = 'pdx dx1 dx2'
    
    
    # Pull the O and S table records within the study period*/
    total_rows = IdDxPT(db_conn=study.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o",
                        codes=icd9_codes, dxVar=dxVar, stDt=study.study_start, edDt=study.study_end,
                        outDsn='_alldx_')
    sql_all_dx = f'''
    --sql
    CREATE TABLE rds.alldx AS
    SELECT a.* FROM _alldx_  AS a WHERE {id_var}  IN (SELECT {id_var} FROM rds.allpt)
    '''
    
    execute_n_drop(db_conn=study.db, sql_expr=sql_all_dx, if_exists='replace')
    
    # Pull the I (admission) table records within the study period*/
    total_rows = IdDxPT(db_conn=study.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "i",
                        service_date_var='admdate', service_end_var='disdate',
                        stDt=study.study_start, edDt=study.study_end,
                        outDsn='_alladm_')
    
    sql_all_adm = f'''
    --sql
    CREATE TABLE rds.alladm AS
    SELECT * FROM _alladm_  WHERE {id_var}  IN (SELECT {id_var} FROM rds.allpt)
    '''
    execute_n_drop(db_conn=study.db, sql_expr=sql_all_adm, if_exists='replace')
    
    
    # Pull the T (continuous enrollment) table records within the study period*/
    IdCEPT(db_conn=study.db ,dbLib = 'raw', dbList = "ccae,mdcr",
               service_date_var='dtstart', service_end_var='dtend',
               stDt=study.study_start, edDt=study.study_end, outDsn='_allce_')
    
    
    sql_all_ce = f'''
    --sql
    CREATE TABLE rds.allce AS 
    SELECT * FROM _allce_ WHERE {id_var} IN (SELECT {id_var}  FROM rds.allpt);
    '''
    execute_n_drop(db_conn=study.db, sql_expr=sql_all_ce, if_exists='replace')