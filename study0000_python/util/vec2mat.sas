/**********************************************************************************
TOPIC:              Vector to Matrix MACRO
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 24, 2019
DESCRIPTION:        This script is a macro for conveting an input table with 
                    (inDsn) with id and parameters (within var_name) to a table
                    where each parameter in var_name has become a flag. The name of
                    the flag is derived from dicDsn with var_name and col_names variables.
                    The final result is saved in outDsn.
************************************************************************************/


%macro vec2mat (inDsn = , id = ,
                dicDsn = , var_name = ,
                col_names= , outDsn = , flagNum = Y);

proc sql noprint;
select &col_names, &var_name into :_col_list separated by " ",
                                :_var_list separated by " "
                                from &dicDsn
;quit;
%local if_func one_value zero_value;
%if %upcase(&flagNum) ne N %then %do;
    %let if_func = ifn;
    %let one_value = 1;
    %let zero_value = 0; 
    %put  numeric flags (&flagNum), and thus if_func = &if_func; %end;
%else %do;
    %let if_func = ifc;
    %let one_value = '1';
    %let zero_value = '0';
    %put char flags (&flagNum), and thus if_func = &if_func; %end;


%put &_var_list;
%put &_col_list;
%local i;
data &outDsn;

set &inDsn;
by &id;
retain &_col_list;
array col_array 
                  %if %upcase(&flagNum) = N %then %do; $%end;
                  &_col_list;
if first.&id then do;
    if _n_ = 1 then do;  
        %let i=1;
        %do %while (%scan(&_var_list, &i) ne );
            %let next_var = %scan(&_var_list, &i);
            %let next_col = %scan(&_col_list, &i);
            %put &i &next_var &next_col;
            label &next_col =  "&next_col (&next_var)";
            %let i = %eval(&i+1);
        %end;
    end;
    do i = 1 to dim(col_array);
        col_array{i} = &zero_value;
    end;
end;
%let i=1;
%do %while (%scan(&_var_list, &i) ne );
    %let next_var = %scan(&_var_list, &i);
    %let next_col = %scan(&_col_list, &i);
    %put &i &next_var &next_col;
    x = &zero_value;
    if &next_col > x then do; x = &next_col; end;
    &next_col = &if_func("&next_var" = &var_name,&one_value,x);
    %let i = %eval(&i+1);
%end;
drop i icd9_3 x;
if last.&id then output;

run;
%mend;
