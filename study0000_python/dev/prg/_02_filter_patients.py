# '''********************************************************************************
# TOPIC:              FILTER PATIENTS (PHASE IV - HEOR)
# WRITTEN BY:         YASER GHAEDSHARAF
# DATE:               AUGUST 20, 2019
# DESCRIPTION:        The purpose of the script is to find the records of the patients with
#                     a record of biologics of interest. Then find the index date of patients
#                     And lastly, save the data sets.
# MODIFICATION:       Use the existing table extraction macros with suggested macro statements
# *********************************************************************************'''

study_ndc = pd.read_sql_query("SELECT icd9 FROM scd.icd9", s.db).iloc[:,0].tolist()
study_hcpcs = pd.read_sql_query("SELECT icd9 FROM scd.hcpcs", s.db).iloc[:,0].tolist()



# identify use of biologics of interest with HCPCS
    procVar = 'proc1'
    hcpcs_codes = pd.read_sql_query("SELECT hcpcs FROM scd.hcpcs", s.db).iloc[:,0].tolist()

    print("study hcpcs: ", *hcpcs_codes)

    total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s, o",
                        codes=hcpcs_codes, rxVar=procVar, stDt=s.study_start, edDt=s.study_end,
                        outDsn='interestTX_SO')
    #  739
    


# identify use of biologics of interest with NDC*/
    ndcVar = 'ndcnum'
    ndc_codes = pd.read_sql_query("SELECT ndc FROM scd.ndc", s.db).iloc[:,0].tolist()
    
    print("study ndc: "  , *ndc_codes)

    total_rows = IdRxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "d",
                        codes=ndc_codes, rxVar=ndcVar, stDt=s.study_start, edDt=s.study_end,
                        outDsn='interestTX_D')
    #  2784 -> fix sas
    # TODO: fix sas

# TODO: Clean days of supply
# clean dayssupp
%clean_daysupp_sas(indsn = interestDS_D, outDsn = interestDS_D, DAYSUPP = DAYSUPP, ndcnum = ndcnum);


    # create interestTx by merging records of HCPCS ans NDC
    sql_statement = \
'''CREATE TABLE rds.interestTx AS 
SELECT enrolid, svcdate, Sex, DOBYR, AGE, planTyp, Region, Generic_name, daySupp 
FROM (SELECT a.*, Generic_name from interestTX_D AS a
               JOIN scd.ndc as b
               ON a.ndcnum = b.ndc)
UNION
SELECT enrolid, svcdate, Sex, DOBYR, AGE, planTyp, Region, Generic_name, daySupp
FROM (SELECT a.*, b.daysupp, Generic_name from interestTX_SO AS a
               JOIN scd.hcpcs as b
               ON a.proc1 = b.hcpcs);'''

    execute_n_drop(conn_or_cur=s.db, sql_expr=sql_statement, if_exists='replace')
    # 3473 




/*create patient table, patient age, and index date, sex, and the index treatment from interestedTx*/
PROC SQL;
create table in_rds.allpt as 
select distinct a.*, DOBYR, AGE
        , put(Sex,$sex.) as Sex label = "Gender of Patient"
        , put(planTyp,plantyp.) as planTyp label = "Plan Indicator"
        , put(region, $region.) as Region label = "Region"
        , Generic_name as idxTreatment from (select enrolid, min(svcdate) as idxDate format = mmddyy10. from in_rds.interestedTx
               where &index_start <= svcdate <= &index_end
               group by enrolid) as a
join in_rds.interestedTx as b
on b.enrolid = a.enrolid and a.idxDate = b.svcdate
;
Quit;

