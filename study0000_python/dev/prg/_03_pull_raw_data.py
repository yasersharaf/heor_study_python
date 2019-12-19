/**********************************************************************************
TOPIC:              PULL RAW DATA TABLES (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 20, 2019
DESCRIPTION:        The purpose of the script is to pull different tables of marketscan
                    data during the study period.
MODIFICATION:       Used suggested macro statements instead of previously used macro statements 
************************************************************************************/

import pandas as pd
from IdDxPT_IdRxPT import execute_n_drop, str_to_list, IdDxPT, IdRxPT, clean_supply_days


icd9_codes = pd.read_sql_query("SELECT icd9 FROM scd.icd9", s.db).iloc[:,0].tolist()
dxVar = 'pdx dx1 dx2'


# Pull the O and S table records within the study period*/
id_var = 'enrolid'
total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o",
                    codes=icd9_codes, dxVar=dxVar, stDt=s.study_start, edDt=s.study_end,
                    outDsn='_alldx_')
sql_all_dx = f'''
--sql
CREATE TABLE rds.alldx AS
SELECT a.* FROM _alldx_  AS a WHERE {id_var}  IN (SELECT {id_var} FROM rds.allpt)
'''

execute_n_drop(db_conn=s.db, sql_expr=sql_all_dx, if_exists='replace')

# Pull the I (admission) table records within the study period*/
total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "i",
                    service_date_var='admdate', service_end_var='disdate',
                    stDt=s.study_start, edDt=s.study_end,
                    outDsn='_alladm_')

sql_all_adm = f'''
--sql
CREATE TABLE rds.alladm AS
SELECT a.* FROM _alladm_  AS a WHERE {id_var}  IN (SELECT {id_var} FROM rds.allpt)
'''
execute_n_drop(db_conn=s.db, sql_expr=sql_all_adm, if_exists='replace')




# Pull the T (continuous enrollment) table records within the study period*/
/*%COLLECT_TABLE(LIB_ARG = raw,SCOPE_ARG = ('T') , LIST_NAME = _a_list );
%put tables are &_a_list;
%let _additional_arg_so = where enrolid in (select enrolid from in_rds.allpt) and dtend >= &study_start and dtstart <= &study_end;
%CONCAT_MARKET_SCAN(LIB_N_TABLE_LIST = &_a_list, outDsn = in_rds.allce, additional_arg=_additional_arg_so);
*/
%IdCEPT(dbLib = raw, stDt = &study_start, edDt = &study_end, outDsn =_allce_);
proc sql;
create table in_rds.allce as select * from _allce_ where enrolid in  (select enrolid from in_rds.allpt);
quit;
