/**********************************************************************************
TOPIC:              DESCRIPTIVE STATISTICAL ANALYSIS FOR CATEGORICAL VARIABLES MACRO
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               August 08, 2019
DESCRIPTION:        The purpose of the macro is to do descriptive statistical analysis 
                    on the categorical variables of a provided data set.
MODIFICATION:       Added the group variable in the macro.
************************************************************************************/


%macro desc_stat_c(InDsn = , OutDsn = _categorical_test, varList = , groupVar = group); 
/*simplify variable list and replace the range with individual variable name*/
%simplifyVarRangeList(_dataSet = &InDsn, _varList = varList);


/* initiallize the iterating variable i with 1 */
%let i=1;
    /* iterate over the list of variables and write their count/percentage/summary to the output dataset*/
%do %while (%scan(&varList, &i) ne );
    %let next_var = %scan(&varList, &i);
    ods output  CrossTabFreqs   = _CrossTabFreqs ;
    proc freq data = &InDsn;
    tables &groupVar.*&next_var;
    run;
    data _CrossTabFreqs(rename = (frequency = N RowPercent = Perc));
    length varLvl varName &groupVar. $ 50;
    set _CrossTabFreqs(keep = &groupVar. &next_var Frequency RowPercent
                       where = (strip(&next_var) ne ""));
    varLvl = &next_var;
    call label (&next_var,varName);
    if strip(&groupVar.) = "" then &groupVar. =  "Overall";
    drop &next_var;
    run;
    
    
    proc transpose data = _crosstabfreqs 
                    out = _crosstabfreqs_T
                    (rename = (COL1 = Value _NAME_ = Stat)
                     drop = _LABEL_);
        by &groupVar. varLvl varName;
    run;
    /* merge the result for all variables */

    proc sql;
    %if &i = 1 %then %do;
        create table &OutDsn as
    %end;
    %else %do;
        insert into &OutDsn
    %end;
    select * from _CrossTabFreqs_T;
    quit;

    %let i = %eval(&i + 1);
%end;

%mend;

