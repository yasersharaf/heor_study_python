/**********************************************************************************
TOPIC:              DESCRIPTIVE STATISTICAL ANALYSIS FOR CONTINUOUS VARIABLES MACRO
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               August 08, 2019
DESCRIPTION:        The purpose of the macro is to do descriptive statistical analysis 
                    on the continuous variables of a provided data set.
************************************************************************************/

/*%include "&stat_desc_dir\simplifyVarRangeList.sas";*/
%macro desc_stat_n(nInDsn = , nOutDsn = , varlist = ); 
 
ods output Summary = _Summary;
proc means data = &nInDsn(keep = group &varlist)
stackodsoutput Mean Median Std Q1 Q3 MAXDEC = 3; 
var &varlist;
class group; 
run;

/* sort to be able to use proc transpose by statement */
proc sort data = _Summary(drop = Nobs);
    by group Label;
run;


proc transpose data = work._Summary 
                out = _nOutDsn(
                drop = _LABEL_ 
                rename = (_NAME_ = Stat Label = varName col1 = Value)
                );
by group Label;
run;
data &nOutDsn;
    length varName $50 varLvl $50 Stat $58;
    varLvl = 'Overall';
    set _nOutDsn
/*(drop = group)*/
;
run;

%mend;

/*%desc_stat_n(nInDsn = desc, nOutDsn = _continuous_desc, varlist = age cost); */
