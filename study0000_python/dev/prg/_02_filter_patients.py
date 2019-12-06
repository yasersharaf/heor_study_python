# '''********************************************************************************
# TOPIC:              FILTER PATIENTS (PHASE IV - HEOR)
# WRITTEN BY:         YASER GHAEDSHARAF
# DATE:               AUGUST 20, 2019
# DESCRIPTION:        The purpose of the script is to find the records of the patients with
#                     a record of biologics of interest. Then find the index date of patients
#                     And lastly, save the data sets.
# MODIFICATION:       Use the existing table extraction macros with suggested macro statements
# *********************************************************************************'''

study_HCPCS = scd_HCPCS['HCPCS'].to_list()
study_NDC = scd_NDC['NDC'].to_list()

print("study hcpcs: ", *study_HCPCS)
print("study ndc: "  , *study_NDC)


/*identify use of biologics of interest with HCPCS*/
%IdRxPT(dbLib = raw,SCOPE = ("S","O"), stDt = &study_start, edDt = &study_end, code = study_hcpcs , outDsn = interestDS_SO);


/*identify use of biologics of interest with NDC*/
%IdRxPT(dbLib = raw,SCOPE = ("D"), stDt = &study_start, edDt = &study_end, code = study_ndc , outDsn = interestDS_D);

/*clean dayssupp*/
%clean_daysupp_sas(indsn = interestDS_D, outDsn = interestDS_D, DAYSUPP = DAYSUPP, ndcnum = ndcnum);


/*create interestedTx by merging records of HCPCS ans NDC*/
proc sql;
create table in_rds.interestedTx as 
select * from (select a.*, Generic_name from interestDS_D as a
               join in_scd.Study_code_ndc as b
               on a.ndcnum = b.ndc)
outer union corr
select * from (select a.*, b.daysupp, Generic_name from interestDS_SO as a
               join in_scd.Study_code_hcpcs as b
               on a.proc1 = b.hcpcs);
;
quit;

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

