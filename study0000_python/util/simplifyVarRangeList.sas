/**********************************************************************************
TOPIC:              SIMPLIFY A LIST WITH RANGES
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               August 08, 2019
DESCRIPTION:        The purpose of the macro is to simplify a list with ranges
                    and convert it to a full list
************************************************************************************/

%macro simplifyVarRangeList(_libname = work, _dataSet = , _varList = );
%if %scan(&_dataSet, 2, %str(.)) ne %then %do;
    %put updating dataset name for simplifying variables'' range list;
    %let _libname = %scan(&_dataSet, 1, %str(.));
    %let _dataSet = %scan(&_dataSet, 2, %str(.));
%end;
%if &_libname = %then %do; %let _libname = work;%end;
%put ---------- simplifying &_dataSet  from library &_libname;
data _dummy_&_dataSet;
set &_libname..&_dataSet(obs = 1 keep = &&&_varList);
run;
proc sql noprint;
select name into:&_varList separated by ' ' from dictionary.columns 
where upcase(libname) = "WORK"
and     upcase(memname)  = upcase("_dummy_&_dataSet");
quit;
%mend;

