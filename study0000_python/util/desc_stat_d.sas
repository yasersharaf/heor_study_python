/**********************************************************************************
TOPIC:              DESCRIPTIVE STATISTICAL ANALYSIS FOR DICHOTOMOUS VARIABLES MACRO
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               August 08, 2019
DESCRIPTION:        The purpose of the macro is to do descriptive statistical analysis 
                    on the dichotomous variables of a provided data set.
************************************************************************************/

%macro desc_stat_d(InDsn = , OutDsn = _dichotomoy_test, varList = , groupVar = group); 
/*simplify variable list and replace the range with individual variable name*/
/*%simplifyVarRangeList(_dataSet = &InDsn, _varList = varList);*/
proc sql;
    create table &OutDsn
    (varName char(50),
    group char(50),
    varLvl char(50),
    Stat char(60),
    Value num);
quit;

proc sql;
select count(*) into: _total_ from &InDsn;
quit;


%local i j;
%let i = 1;

proc sql;
select &groupVar, count(*) into: _group_list separated by ' ', :_group_population separated by ' ' 
from &InDsn. group by &groupVar;
quit;

%put &_group_list;
%put &_group_population;


/* iterate over the list of variables and write their count/percentage/summary to the output dataset*/
%do %while (%scan(&varlist, &i) ne );
    %let j = 0;
    %let next_var = %scan(&varlist, &i);
    data __dummy;
        set &InDsn(keep = &next_var obs = 1);
        length next_label $ 100;
        call label(&next_var,next_label);
        call symput('next_label', strip(next_label));
    run;
    %put ======== analyzing &next_var(&next_label) dichotomy variable =============;
    %do j = 0 %to %sysfunc(countw(&_group_list));
        %if &j = 0 %then %do;
            %let next_group = Overall;
            %let next_group_pop = &_total_;
        %end;
        %else %do;
            %let next_group = %scan(&_group_list, &j);
            %let next_group_pop = %scan(&_group_population, &j);
        %end;
        proc sql noprint;
        select count(*) as cnt, calculated cnt/&next_group_pop. into:_next_var_ones, :_next_var_ones_perc from &InDsn
        where &next_var = '1' 
        %if &j >= 1 %then %do; and &groupVar. = "&next_group." %end;
        ;
        quit;;
        %put number of ones in &next_var of group &next_group is &_next_var_ones which is &_next_var_ones/&next_group_pop = _next_var_ones_perc;
        proc sql;
        insert into &OutDsn
            values("&next_label","&next_group","Overall","N",&_next_var_ones)
            values("&next_label","&next_group","Overall","Perc",&_next_var_ones_perc);

        quit;
    %end;
    %let i = %eval(&i + 1);


%end;


%mend;
