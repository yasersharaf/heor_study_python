/**********************************************************************************
TOPIC:              OUTCOMES RESOURCE UTILIZATION (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 26, 2019
DESCRIPTION:        The purpose of the script is to find the number of all hospitalization, 
                    emergency room visits, office visits, or other types of visits for 
                    cohort patients in the outcomes period. Also, create a label indicating
                    the existence of each type of visit. Moreover, create the same columns 
                    for the case that the visit/hospitalization included a non-ruleout psoriasis
                    diagnosis.
MODIFICATION:       1- Do not consider the non-ER diagnosis in the other (outpatient) diagnosis count.
                    2- Consider the unique service dates for counting the frequency
                    3- Include the observations from outcomes period only
************************************************************************************/

/*find psoriasis diagnosis codes*/
proc sql noprint;
select "'" || strip(icd9) || "'" into: baseline_cond separated by ',' from in_scd.study_code_icd9 where upcase(TA) ="PSORIASIS";quit;
quit;

/*find patients hospitalization counts (both all or with disease diagnosis)*/
data outcomes_hospital;
    set sds.sdadm
    (where=%is_in_outcome(date = admdate, date2 = disDate, idxDate = idxDate))
    ;
    by enrolid admdate;
    retain All_Hospitalization Dis_Hospitalization obs_Dis_Hospital;
    array procs proc1-proc15;
    
    if first.enrolid then do;
/* initiallizae number of visits by zero  */
        All_Hospitalization = 0;
        Dis_Hospitalization = 0;
    end;
/*count a general hospitalization only once for each disticnt date
  also starting a new data, set the existence of a hospitalization with diagnosis
  to zero*/
    if first.admdate then do;
        All_Hospitalization = All_Hospitalization + 1;
        obs_Dis_Hospital = 0;
    end;
/*add a flag indicating a non-ruleout diagnosis of the disease in the hospitalization
  if true, set the proc number of the non-rule out as the flags value*/
    if pdx in: (&baseline_cond) then do;
        do i = 1 to dim(procs);
            proc1 = procs{i};
            if ((not %ruleout_) and strip(proc1) ne "") then do;
                obs_Dis_Hospital = i;
                leave;
            end;
        end;
    end;
/*    if it is the last observation of the admission date for the patient,
    and the patient has had a diagnosis of the disease, increament number of
    hospitalizations with the disease diagnosis by one*/
    if last.admdate and obs_Dis_Hospital>0 then do;
        Dis_Hospitalization = Dis_Hospitalization + 1;
    end;
    if last.enrolid then output;
    keep enrolid admdate idxDate disDate All_Hospitalization Dis_Hospitalization;
run;

/*create a flag indicating type of the visit for the claim (ER, office, others) using data step*/
/*It is also possible to denote all three types of visit with 1 flag since they are
mutually exclusive.*/
data visit_type;
    set sds.sddx(where=%is_in_outcome());
/*initiallize the claim visit type as none*/
    ER = 0; 
    Office = 0;
    Others = 0;
/*flag indicating the existence of the disease*/
    disease_flag = 0;
/*checking whether it is an ER visit according to marketscan guide and the data dictionary*/
    if  %EmergencyRoom_
        then ER = 1;
    else if scope = "O" and %OPVisit_ 
        then Office = 1;
    else if scope = "O" then Others  = 1;
    if (not %ruleout_) and (
        (scope = "S" and pdx in: (&baseline_cond))
        or 
        (scope = "O" and (dx1 in:(&baseline_cond) or dx2 in:(&baseline_cond)))
        )
        then disease_flag = 1;
run;
 



/*find the frequency of each type of visit (both all and given the diagnosis)*/
proc sql;
create table freq_ru as
select a.enrolid 
, All_ER, All_office, All_others
, Dis_ER, Dis_office, Dis_others 
from sds.sddm as a
left join %uniqueDateVisit(visit_type,ER    ) as b
on a.enrolid = b.enrolid
left join %uniqueDateVisit(visit_type,Office) as c
on a.enrolid = c.enrolid
left join %uniqueDateVisit(visit_type,Others) as d
on a.enrolid = d.enrolid

left join %uniqueDateVisit(visit_type,ER    ,disease_flag = 1) as e
on a.enrolid = e.enrolid
left join %uniqueDateVisit(visit_type,Office,disease_flag = 1) as f
on a.enrolid = f.enrolid
left join %uniqueDateVisit(visit_type,Others,disease_flag = 1) as g
on a.enrolid = g.enrolid
;
quit;

%let Dis = Psoriasis;
proc print; 
/*Integrate visit type and hospitalization info for the patients, along with 
a flag indicating the existence of that purticular type of visit/hospitalization
into one table*/
proc sql;
create table ads.outcomes_RU as
select   a.enrolid format = 12.0
/*counts of hospitalization or visits */
        ,ifn(b.All_Hospitalization, b.All_Hospitalization   ,0) as All_Hospitalization          label = "Frequency of Any Hospitalization"
        ,ifn(b.Dis_Hospitalization, b.Dis_Hospitalization   ,0) as Dis_Hospitalization          label = "Frequency of &Dis Hospitalization"
        ,ifn(c.all_er             , c.all_er                ,0) as All_ER                       label = "Frequency of Any ER Visits"
        ,ifn(c.all_office         , c.all_office            ,0) as All_Office                   label = "Frequency of Any Office Visits"
        ,ifn(c.all_others         , c.all_others            ,0) as All_Others                   label = "Frequency of Any Others Visits"
        ,ifn(c.dis_er             , c.dis_er                ,0) as Dis_ER                       label = "Frequency of &Dis ER Visits"
        ,ifn(c.dis_office         , c.dis_office            ,0) as Dis_Office                   label = "Frequency of &Dis Office Visits"
        ,ifn(c.dis_others         , c.dis_others            ,0) as Dis_Others                   label = "Frequency of &Dis Other Visits"

/*flags indicating hospitalization or visits of each type*/
        ,ifc(b.All_Hospitalization, '1',  '0')                  as with_All_Hospitalization     label = "Had any Hospitalization"    
        ,ifc(b.Dis_Hospitalization, '1',  '0')                  as with_Dis_Hospitalization     label = "Had &Dis Hospitalization"
        ,ifc(c.all_er,              '1',  '0')                  as with_All_ER                  label = "Had Any ER Visits"
        ,ifc(c.all_office,          '1',  '0')                  as with_All_Office              label = "Had Any Office Visits"
        ,ifc(c.all_others,          '1',  '0')                  as with_All_Others              label = "Had Any Other Visits"
        ,ifc(c.dis_er,              '1',  '0')                  as with_Dis_ER                  label = "Had &Dis ER Visits"
        ,ifc(c.dis_office,          '1',  '0')                  as with_Dis_Office              label = "Had &Dis Office Visits"
        ,ifc(c.dis_others,          '1',  '0')                  as with_Dis_Others              label = "Had &Dis Other Visits"
from sds.sddm as a
left join outcomes_hospital as b
on a.enrolid = b.enrolid
/*frequency of each visit type for each patient (all visits vs those with disease diagnosis)*/
left join freq_ru as c
on a.enrolid = c.enrolid
;quit;


/*=================================== Appendix ===================================*/


*** create a flag indicating type of the visit (ER, office, others) using sql;
/*
proc sql;
create table er_visits as
select *, 
        case when substr(SVCSCAT,4,2) = '20'
                    or
                    STDPLAC = 23
                    or 
                    (STDPLAC in (21,22,28)   and   ((STDPROV = 220 and STDSVC ne 104) or STDSVC = 77)  )
                    or 
                    PROCGRP in (110,111,114) then 1 else 0
                end as er_flag
        , case when %OPVisit_ and calculated er_flag = 0 then 1 else 0
                end as office_flag

        , case when scope = "O"
                    and
                    calculated er_flag = 0 
                    and
                    calculated office_flag = 0 then 1 else 0
                end as others_flag
        , case when (not %ruleout_) and (
        (scope = "S" and pdx in (&baseline_cond))
        or 
        (scope = "O" and (dx1 in(&baseline_cond) or dx2 in(&baseline_cond)))
        ) then 1 else 0
                end as disease_flag
from sds.sddx
;
quit;
proc sql; 
select sum(er_flag), sum(office_flag), sum(others_flag) from er_visits
where disease_flag;
quit;


proc sql; 
select sum(er_flag), sum(office_flag), sum(office_flag) from visit_type
where disease_flag;
quit;

*/
