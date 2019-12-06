/**********************************************************************************
TOPIC:              PULL RAW DATA TABLES (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 20, 2019
DESCRIPTION:        The purpose of the script is to pull different tables of marketscan
                    data during the study period.
MODIFICATION:       Used suggested macro statements instead of previously used macro statements 
************************************************************************************/
/*Pull the O and S table records within the study period*/
/*%COLLECT_TABLE(LIB_ARG = raw,SCOPE_ARG = ('O','S') , LIST_NAME = _a_list );
%put tables are &_a_list;
%let _additional_arg_so = where &study_start <= svcdate <= &study_end. and enrolid in (select enrolid from in_rds.allpt);
%CONCAT_MARKET_SCAN(LIB_N_TABLE_LIST = &_a_list, outDsn = in_rds.alldx, additional_arg=_additional_arg_so);*/

%IdDxPT(dbLib = raw,SCOPE = ("S","O"), stDt = &study_start, edDt = &study_end, outDsn =_alldx_);
proc sql;
create table in_rds.alldx as select * from _alldx_ where enrolid in  (select enrolid from in_rds.allpt);
quit;


/*Pull the I (admission) table records within the study period*/
/*%COLLECT_TABLE(LIB_ARG = raw,SCOPE_ARG = ('I') , LIST_NAME = _a_list );
%put tables are &_a_list;
%let _additional_arg_so = where &study_start <= coalesce(disdate,admdate) and admdate <= &study_end. and enrolid in (select enrolid from in_rds.allpt);
%CONCAT_MARKET_SCAN(LIB_N_TABLE_LIST = &_a_list, outDsn = in_rds.alladm, additional_arg=_additional_arg_so);*/

%IdAdmPT(dbLib = raw, stDt = &study_start, edDt = &study_end, outDsn =_alladm_);
proc sql;
create table in_rds.alladm as select * from _alladm_ where enrolid in  (select enrolid from in_rds.allpt);
quit;


/*Pull the D (drug) table records within the study period*/
/*%COLLECT_TABLE(LIB_ARG = raw,SCOPE_ARG = ('D') , LIST_NAME = _a_list );
%put tables are &_a_list;
%let _additional_arg_so = where &study_start <= svcdate <= &study_end. and enrolid in (select enrolid from in_rds.allpt);
%CONCAT_MARKET_SCAN(LIB_N_TABLE_LIST = &_a_list, outDsn = in_rds.allrx, additional_arg=_additional_arg_so);
*/

%IdRxPT(dbLib = raw,SCOPE = ("D"), stDt = &study_start, edDt = &study_end, outDsn =_allrx_);
proc sql;
create table in_rds.allrx as select * from _allrx_ where enrolid in  (select enrolid from in_rds.allpt);
quit;


/*Pull the T (continuous enrollment) table records within the study period*/
/*%COLLECT_TABLE(LIB_ARG = raw,SCOPE_ARG = ('T') , LIST_NAME = _a_list );
%put tables are &_a_list;
%let _additional_arg_so = where enrolid in (select enrolid from in_rds.allpt) and dtend >= &study_start and dtstart <= &study_end;
%CONCAT_MARKET_SCAN(LIB_N_TABLE_LIST = &_a_list, outDsn = in_rds.allce, additional_arg=_additional_arg_so);
*/
%IdCEPT(dbLib = raw, stDt = &study_start, edDt = &study_end, outDsn =_allce_);
proc sql;
create table in_rds.allce as select * from _allce_ where enrolid in  (select enrolid from in_rds.allpt);
quit;
