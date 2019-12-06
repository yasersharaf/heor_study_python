
%let cat_char_list = "SEX","PLANTYP","REGION","idxTreatment";
%let other_list = "enrolid","group","DOBYR", "idxDate";

proc sql;
select name into: _num_vars separated by " " from DICTIONARY.columns
where   UPCASE(LIBNAME)= "ADS"
        and 
        UPCASE(MEMNAME)= "VARIABLES"
        and
        upcase(type) = "NUM"
        and upcase(name) not in (%upcase(&cat_char_list, &other_list));
quit;
%put &_num_vars;


proc sql;
select name into: _dich_vars separated by " " from DICTIONARY.columns
where   UPCASE(LIBNAME)= "ADS"
        and 
        UPCASE(MEMNAME)= "VARIABLES"
        and
        upcase(type) = "CHAR"
        and upcase(name) not in (%upcase(&cat_char_list, &other_list));
quit;
%put &_dich_vars;

proc sql;
select name into: _cat_vars separated by " " from DICTIONARY.columns
where   UPCASE(LIBNAME)= "ADS"
        and 
        UPCASE(MEMNAME)= "VARIABLES"
        and
        upcase(name) in (%upcase(&cat_char_list));
quit;
%put &_cat_vars;


%desc_stat_n(nInDsn = copyx2, nOutDsn = _continuous_desc, varlist = &_num_vars); 

%desc_stat_c(InDsn = copyx2, OutDsn = _categorical_desc, varlist = &_cat_vars, groupVar = group);

%desc_stat_d(InDsn = copyx2, OutDsn = _dichotomy_desc, varlist = &_dich_vars, groupVar = group); 


%desc_stat(inDsn = ads.variables, outDsn = _stat_desc ,DVarList = &_dich_vars ,CVarList = &_cat_vars
            ,NVarList = &_num_vars, Strata = Group);


data this;

set copyx2(keep = sex obs = 1);
length a $ 100;
call label(sex,a);
run;

proc sql;
create table copyx2 as
select *  from ads.variables
outer union corr 
select *, "Grp1" as group from ads.variables(drop = group);
run;


    proc summary data = copyx2;
        class group;
        var SEX;
        output out = _dummy_summary_cat;
/*(rename = (_FREQ_ = Value));*/
    run;

%macro this_macro;
/* initiallize the iterating variable i with 1 */
%let i=1;
    /* iterate over the list of variables and write their count/percentage/summary to the output dataset*/
    %do %while (%scan(&_cat_vars, &i) ne );
        %let next_var = %scan(&_cat_vars, &i);
ods output  CrossTabFreqs   = _CrossTabFreqs ;
    proc freq data = copyx2 ;
    tables group*&next_var / out = _freq
/*(where = 1)*/
 sparse
    ;
    run;
proc sql;
select 
data _CrossTabFreqs(rename = (frequency = N RowPercent = Perc));
length varLvl varName group $ 50;
set _CrossTabFreqs(keep = group &next_var Frequency RowPercent
                   where = (strip(&next_var) ne ""));
varLvl = &next_var;
call label(varName ,&next_var);
if strip(group) = "" then group =  "Overall";
drop &next_var;

run;

/* merge the result for all variables */

    proc sql;
    %if &i = 1 %then %do;
    create table cOutDsn as
    %end;
    %else %do;
    insert into cOutDsn
    %end;
    select * from _CrossTabFreqs;
    quit;

    %let i = %eval(&i + 1);
    %end;
%mend;
%this_macro;
