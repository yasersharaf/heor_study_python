/**********************************************************************************
TOPIC:              CREATE COHORT (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 21, 2019
DESCRIPTION:        The purpose of the script is to identify the cohort group
                    who satisfy the inclusion/exclusion criteria. Then classify
                    the patients based on their medical consditions during the study
                    period.
MODIFICATION:       1- Create age flag (inc_age) in the final cohort
************************************************************************************/
/*creating a macro indicating the baseline condition*/

proc sql noprint;
select '"' || icd9 || '"' into: baseline_cond separated by ',' from in_scd.study_code_icd9 where upcase(TA) ="PSORIASIS";
quit;

%put Baseline required conditions codes list is (&baseline_cond);

/*find patients with baseline condition (psoriasis)*/

proc sql;
create table allpt_psoriasis as
select enrolid, max(psor_dx) as inc_psoriasis from (
                select a.enrolid, case when b.enrolid ne . then 1 else 0 end as psor_dx from in_rds.allpt as a
                left join in_rds.alldx as b
                on a.enrolid = b.enrolid and (not %Ruleout_)  and idxDate - &preeval_length <= svcdate <= idxDate 
                                         and (DX1 in (&baseline_cond) or DX2 in (&baseline_cond))
                )
group by enrolid
;
quit;


/*creating a table indicating records of medical and pharmaceutical enrollment for patients in the initial cohort*/

proc sql;
create table allpt_enroll as
select a.*, DTSTART, DTEND from in_rds.allpt as a
join in_rds.allce(where=(RX = '1')) as b
on a.enrolid = b.enrolid;
quit;


/*Create continuous enrollment table for the data enrollment data*/
%CONT_ENROLL_PERIODS(ENROLL_DATA = allpt_enroll, ID_VAR = ENROLID, GRACE_PERIOD = &CE_gap,
                     PRIOR_DAYS = &preeval_length,POST_DAYS =  &eval_length ,IDXDATE_VAR = IDXDATE);

/*adding an inclusion flag indicating continuous enrollment for the patients within the initial cohort*/

proc sql;
create table allpt_ce as
select a.enrolid, case when pr_start >= 1 then 1 else 0 end as inc_ce
from in_rds.allpt  as a
left join Allpt_enroll_step2 as b
on a.enrolid = b.enrolid
;
quit;

/*finding records of biologics use in baseline*/
proc sql;
create table baseline_usage as
select enrolid, case when ndcnum ne '' or proc1 ne '' then 1 else 0 end as exc_baseline from
        (select c.*, d.ndcnum, d.svcDate as ndc_svc from 
            (select a.enrolid, a.idxDate format = mmddyy10., b.proc1, b.svcDate as hcpcs_svc from in_rds.allpt as a
            left join (select * from in_rds.alldx where proc1 in (select hcpcs from in_scd.Study_code_hcpcs))as b
            on a.enrolid = b.enrolid and idxDate - &preeval_length <= b.svcdate < idxDate) as c
        left join (select * from in_rds.allrx where ndcnum in (select ndc from in_scd.Study_code_ndc))as d
        on c.enrolid = d.enrolid and idxDate - &preeval_length <= d.svcdate < idxDate);
quit;

proc sql;
create table baseline_usage as
select a.enrolid, ifn(strip(b.ndcnum||b.proc1) = "",0,1) as exc_baseline from in_rds.allpt as a
left join in_rds.interestedTx as b 
on a.enrolid = b.enrolid and a.idxDate - &preeval_length <= b.svcdate < a.idxDate;
quit;

/*aggregating patients records (by patient) to find the list of patients who used biologics of interest*/

proc sql;
create table allpt_basline_use as
select enrolid, max(exc_baseline) as exc_baseline from baseline_usage
group by enrolid;
quit;


/*creating cohort flag with columns indicating exclusion/inclusion flags*/

proc sql;
create table sds.cohort as 
select a.*, b.inc_psoriasis, 
            ifn(c.enrolid ne ., 1, 0) as inc_ce,
            ifn(a.age>=&min_age,1,0) as inc_age,
            e.exc_baseline from in_rds.allpt as a
join allpt_psoriasis as b
on a.enrolid = b.enrolid
left join Allpt_enroll_step2 as c
on a.enrolid = c.enrolid
join allpt_basline_use as e
on a.enrolid = e.enrolid;
quit;

/*creating patient flow table*/

proc sql;
create table _t1 as
select count(*) label =  "Patients who used biologics In Identification period 
                            (i.e. between %format(&index_start,mmddyy10.) and %format(&index_end,mmddyy10.))",
sum(inc_psoriasis) label =  "Patients with a non-rule-out diagnosis of psoriasis in baseline period
                            (i.e., &preeval_length days before patient's index date and up to one day before the index date)", 
sum(inc_ce and inc_psoriasis) label = "Patients with medical and pharmaceutical continuous enrollment &preeval_length days before the index date and 
                    up to &eval_length of the index date (with &ce_gap days gap allowance)",
sum(inc_age and inc_ce and inc_psoriasis) label = "Patients that are at least &min_age years old at their index date",
sum((not exc_baseline) and inc_age and inc_ce and inc_psoriasis) label = "Patients who are not excluded due to use of biologics of interest during the baseline period
                            (i.e., &preeval_length days before patient's index date and up to one day before the index date)" from sds.cohort;
quit;

/*transposing the patient flow table*/
proc transpose data = _t1
                out = __t1;
run;

/*modifying the patient flow table columns name*/
proc sql;
create table sds.t1 as 
select monotonic() as Step, _LABEL_ as Criteria label = "Criteria",
       Col1 as SampleSize label = "Sample size" from __t1;
quit;


/*creating list of patients satisfying inclusion/exclusion criterias*/

proc sql;
create table sddm_preprocess as
select a.*, proc1,dx1,dx2 from sds.cohort as a
join in_rds.alldx as b
on a.enrolid = b.enrolid
where inc_psoriasis+inc_ce+inc_age -  exc_baseline = 3;
quit;

/*creating macros indicating medical conditions*/
proc sql noprint;
select icd9 into: Psoriatic_Arthritis_list separated by '","' from in_scd.study_code_icd9 where upcase(TA) ="PSORIATIC ARTHRITIS";
quit;
%put ("&Psoriatic_Arthritis_list");

proc sql noprint;
select icd9 into: Rheumatoid_Arthritis_list separated by '","' from in_scd.study_code_icd9 where upcase(TA) ="RHEUMATOID ARTHRITIS";
quit;
%put ("&Rheumatoid_Arthritis_list");

proc sql noprint;
select icd9 into: Ankylosing_Spondylosis_list separated by '","' from in_scd.study_code_icd9 where upcase(TA) ="ANKYLOSING SPONDYLOSIS";
quit;
%put ("&Ankylosing_Spondylosis_list");

proc sql noprint;
select icd9 into: Crohns_Disease_list separated by '","' from in_scd.study_code_icd9 where upcase(TA) ="CROHN'S DISEASE";
quit;
%put ("&Crohns_Disease_list");


/*finding flags indicating the diagnosis records for the conditions of interest in the patients*/
data sddm_preprocess_2;
set sddm_preprocess;
if (not %Ruleout_) and (dx1 in: ("&Psoriatic_Arthritis_list") or dx2 in: ("&Psoriatic_Arthritis_list")) 
            then p_flag = 1; else p_flag = 0;
if (not %Ruleout_) and (dx1 in: ("&Rheumatoid_Arthritis_list") or dx2 in: ("&Rheumatoid_Arthritis_list"))
            then r_flag = 1; else r_flag = 0;
if (not %Ruleout_) and (dx1 in: ("&Ankylosing_Spondylosis_list") or dx2 in: ("&Ankylosing_Spondylosis_list"))
            then a_flag = 1; else a_flag = 0;
if (not %Ruleout_) and (dx1 in: ("&Crohns_Disease_list") or dx2 in: ("&Crohns_Disease_list"))
            then c_flag = 1; else c_flag = 0;
/*if p_flag + r_flag + a_flag + c_flag >= 1 then output;*/
run;

/*creating unique flag for patients indicating any diagnosis of the conditions of interest*/
proc sql;
create table sddm_preprocess3 as
select enrolid, max(p_flag) as p_flag ,max(r_flag) as r_flag, max(a_flag) as a_flag, max(c_flag) as c_flag
from sddm_preprocess_2
group by enrolid;
quit;

/*classifying the patients based on their diagnosis*/
data sddm_preprocess4;
set sddm_preprocess3;
if p_flag and not (r_flag or a_flag or c_flag) then group = 'Grp1';
else if (r_flag or a_flag or c_flag) and not p_flag then group = 'Grp2';
else if p_flag and (r_flag or a_flag or c_flag) then group = 'Grp3';
else if not (p_flag or r_flag or a_flag or c_flag) then group = 'Grp4';
else group = 'Grp5';
run;

/*creating the final cohort data set*/
proc sql;
create table sds.sddm as 
select a.*, b.group from sds.cohort(drop = inc_psoriasis inc_ce inc_age exc_baseline) as a
right join sddm_preprocess4 as b
on a.enrolid = b.enrolid;
quit;


/*==================Pull the standard datasets for the final cohort group========================*/
/*a. inpatient and outpatient services, save data as sddx.sas7bdat in dev/drv/sds folder*/
proc sql;
create table sds.sddx as
select a.*,b.idxDate from in_rds.alldx as a
join sds.sddm as b
on a.enrolid = b.enrolid and b.idxDate - &preeval_length <= a.svcdate <= b.idxDate + &eval_length;
;
quit;

/*b. pharmacy claims, save data as sdrx.sas7bdat in dev/drv/sds folder*/
proc sql;
create table sds.sdrx as
select a.*,b.idxDate from in_rds.allrx as a
join sds.sddm as b
on a.enrolid = b.enrolid and b.idxDate - &preeval_length <= a.svcdate <= b.idxDate + &eval_length;
;
quit;

/*c. inpatient admissions, save data as sdadm.sas7bdat in dev/drv/sds folder*/
proc sql;
create table sds.sdadm as
select a.*,b.idxDate from in_rds.alladm as a
join sds.sddm as b
on a.enrolid = b.enrolid and b.idxDate - &preeval_length <= coalesce(disdate,admdate) and admdate <= b.idxDate + &eval_length
order by enrolid, admdate
;
quit;
