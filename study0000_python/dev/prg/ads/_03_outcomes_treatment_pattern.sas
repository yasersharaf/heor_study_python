/**********************************************************************************
TOPIC:              OUTCOMES TREATMENT PATTERN (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 28, 2019
DESCRIPTION:        The purpose of the script is to analyze cohort's adherence, persistence.
                    Lastly, merge all the the outcomes tables with the baseline table to 
                    create Variables table which will be used for the statistical analysis.
MODIFICATION        1- KM analysis for time to discontinuation.
                    2- Remove patients with multiple index treatments.
************************************************************************************/
/*list of patients with multiple index treatment*/
proc sql;
create table multi_idx_Tx_pt as
select enrolid, count(distinct idxTreatment) as indexFreq from sds.sddm
group by enrolid
having indexFreq>1;
quit;


/*find the treatments of interest claims in the follow-up period*/
proc sql;
create table cohort_treatment(rename = (Generic_name = idxTreatment)) as
select b.idxDate,a.*  from in_rds.InterestedTx as a
join sds.sddm as b
on a.enrolid = b.enrolid and b.idxTreatment = a.Generic_name and %is_in_outcome(date = a.svcDate, idxDate = b.idxDate)
/*exclude patients with multiple index treatments*/
where b.enrolid not in (select enrolid from multi_idx_Tx_pt)

;quit;

/*analyze patients' treatment adherence */
%adherence(drugdsn= cohort_treatment, followUpDays = &eval_length, outdsn=cohort_adhere,
           id = enrolid, drugdt = svcdate, idxdt = idxDate, drugdays = daysupp);

/*analyze patients' treatment persistence */
%persistence(indsn = cohort_treatment, DiscTH = &Rx_gap, followUpDays = &drug_followup, withLookforward = Y, 
             id = enrolid, drugdt = svcDate, drugdays = daysupp, outdsn = cohort_persist);


/*perform KM-analysis for time to discontinuation*/
proc lifetest data = cohort_persist ;
    time time*disc_flag(0);
run; 

/*adherence sas dataset*/
proc sql;
create table ads.outcomes_TP as
select a.enrolid, a.pdc_df2, put(1- b.disc_flag,1.0) as persistence_flag label = "Persistence Flag", b.time label = "Time to discontinuation" from cohort_adhere as a
join cohort_persist as b
on a.enrolid = b.enrolid;
quit;

/*create variables table (the final data set required for the analysis)*/
proc sql;
create table ads.variables(drop = benrolid cenrolid denrolid) as
select * from sds.sddm as a
join ads.baseline (rename = (enrolid = benrolid)) as b
on a.enrolid = b.benrolid
join ads.outcomes_ru(rename = (enrolid = cenrolid)) as c
on a.enrolid = c.cenrolid
join ads.outcomes_tp(rename = (enrolid = denrolid)) as d
on a.enrolid = d.denrolid
;
quit;
