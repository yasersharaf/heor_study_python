
/*=======================================MISCELLANEOUS MACROS==========================================*/
/*author: Yaser Ghaedsharaf*/
/*date: August 29*/

/*a macro for choosing unique service dates of a specifict type for each patient subject to a disease_flag*/
%macro uniqueDateVisit(inDsn , type, id = enrolid, date = svcDate, disease_flag = 0);
(select &id, count(distinct &date) as 
%if &disease_flag ne 1 %then %do; All_&type%end;
%else %do; Dis_&type%end;
 from &inDsn
where &type = 1
%if &disease_flag = 1 %then %do; and disease_flag = 1%end;
group by &id)
%mend;

/*for checking the outcome period*/
%macro is_in_outcome(date = svcDate, date2 = , idxDate = idxDate );
%if &date2 ne %then %do; 
(&idxDate <= coalesce(&date2,&date) and &date <= &idxDate + &eval_length)
%end;
%else %do; 
(&idxDate <= &date <= &idxDate + &eval_length)
%end;
%mend;
