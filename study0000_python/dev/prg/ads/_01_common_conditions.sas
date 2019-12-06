/**********************************************************************************
TOPIC:              COMMON BASELINE CONDITIONS AND CHARLSON COMORBIDITY INDEX
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 26, 2019
DESCRIPTION:        This script finds the common 3 digit diagnosis and 
                    Charlson comorbidity index for cohort patients in
                    their baseline period.
************************************************************************************/


/*create a table for baseline diagnosis of the final cohort*/
proc sql;
create table sddx_baseline(drop = benrolid proc1) as
select a.enrolid, b.* from sds.sddm(keep = enrolid) as a
left join sds.sddx
(keep = enrolid dx1-dx2 proc1 svcdate idxDate rename = (enrolid = benrolid) 
where = (not %ruleout_)
) as b
on a.enrolid = b.benrolid
where idxDate-&preeval_length <= svcdate < idxDate
;quit;

/*find the unique list of diagnosis for each patient*/
proc sql ;
create table icd9_3digit as
select enrolid, substr(strip(dx1),1,3) as icd9_3 from sddx_baseline
union
select enrolid, substr(strip(dx2),1,3) as icd9_3 from sddx_baseline;
quit;

/*rank the 3-char icd9 according to their commonnes*/
proc sql ;
create table top_icd9_3 as
select icd9_3, count(*) as freq from icd9_3digit
where icd9_3 ne "" and icd9_3 not in (select substr(icd9,1,3) from in_scd.study_code_icd9 where upcase(TA) = "PSORIASIS")
group by icd9_3
order by freq desc;
quit;

/*create a tabele of 10 most common 3-char icd9 diagnosis*/
data top_icd9_3;
set top_icd9_3;
retain freq_10th;
icd9_3_rank = "ICD93d_Top_"||strip(put(_n_,2.));
if _n_ = 10 or freq = freq_10th then freq_10th = freq;
else freq_10th = 0;
if _n_<= 10 or freq = freq_10th then output;
run;

/*create flags for cohort patients indicating whether they have
had a diagnosis for top (possibly) 10 common diagnosis during the baseline*/
%vec2mat(inDsn = icd9_3digit, id = enrolid,dicDsn = top_icd9_3,
         var_name = icd9_3, col_names=icd9_3_rank , outDsn = common_cond, flagNum = N);

/*find the comorbidity index of the */
options mprint;
%COMB_Charlson(setin =sddx_baseline ,id = enrolid,dxvarstr =dx1-dx2,
               outdata = cci ,src =work);

/*create the final baseline analysis data set by merging the common conditions and comorbidity index of patients */
proc sql;
create table ads.baseline(drop = benrolid) as 
select * from cci(drop = i) as a
join common_cond(rename = (enrolid = benrolid)) as b
on a.enrolid = b.benrolid;
quit;
