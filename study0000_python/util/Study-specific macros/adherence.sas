%macro adherence(drugdsn=, followUpDays =, outdsn=, id = enrolid, drugdt = svcdate, idxdt = , drugdays = daysupp);
***If patients have the same kinds of drug dispensing on the same day, ***;
***it need to sum those daysupp of the claims into one drug dispensing claim***;
proc sql;
    create table ad_raw as
    select &id, &idxdt as idxdt, &idxdt + &followUpDays as enddt format = mmddyy10.,
           &drugdt as svcdt, sum(&drugdays) as daysupp
    from &drugdsn.
    where &idxdt <= svcdt <= &idxdt + &followUpDays
    group by &id, svcdt, idxdt, enddt
    ;
quit;

*********For MPR measures, we will restrict the study sample to patients who have at least 2 fillings**********;
***MPR definition 1: MPR=Total Rx days of supply/[(Last Rx date - First Rx date) + Last Rx days of supply]***;
***MPR definition 2: MPR=(Total Rx days of supply - Last Rx days of supply)/(Last Rx date - First Rx date)***;
***MPR definition 3: MPR=[Total Rx days of supply - Last Rx days of supply - max(0, [estimated last Rx date-actual last Rx date])]/(Last Rx date - First Rx date)***;
data ad_mpr_df;
    set ad_raw;
    retain total_daysupp est_svcdt pdaysupp;
    by &id svcdt;

    if first.&id then do;
        total_daysupp = 0;
        est_svcdt = svcdt;
    end;
    else if svcdt >= (est_svcdt + pdaysupp) then do;
        est_svcdt = svcdt;
    end;
    else do;
        est_svcdt = est_svcdt + pdaysupp;
    end;

    pdaysupp = daysupp;
    total_daysupp + daysupp;

    if last.&id;
    format est_svcdt mmddyy10.;

    if first.&id = last.&id then do;
        mpr_df1 = .; mpr_df2 = .; mpr_df3 = .; mpr_flag = 0;
    end;
    else do;
        mpr_df1 = min((total_daysupp) / ((svcdt - idxdt) + daysupp), 1);
        mpr_df2 = min((total_daysupp - daysupp)/(svcdt - idxdt), 1);
        mpr_df3 = (total_daysupp - daysupp - max(0, (est_svcdt - svcdt)))/(svcdt - idxdt);
        mpr_flag = 1;
    end;

run;

********For PDC measures, we will restrict the study sample to patients who have at least 1 filling***********;
***PDC definition 1: PDC=Number of medication possession days/Fixed interval***;
data ad_pdc_df1;
    set ad_raw;
    retain total_daysupp est_svcdt pdaysupp;
    by &id svcdt;
    if first.&id then do;
        est_svcdt = svcdt;
        pdaysupp = daysupp;
        total_daysupp = 0;
    end;
    else if svcdt >= (est_svcdt + pdaysupp) then do;
        est_svcdt = svcdt;
        pdaysupp = daysupp;
    end;
    else do;
        est_svcdt = est_svcdt + pdaysupp;
        pdaysupp = max(svcdt + daysupp - est_svcdt, 0);
    end;
    
    pdaysupp = max(min(pdaysupp, enddt - est_svcdt), 0);
    total_daysupp + pdaysupp;

    if last.&id;
    format est_svcdt mmddyy10.;

    pdc_df1 = total_daysupp / &followUpDays;
run;

***PDC definition 2: PDC=[Total Rx days of supply - max(0, [estimated next Rx date- study end date])]/Fixed interval***;
***PDC definition 3: PDC=Total days supply in the study period/Fixed interval***;
data ad_pdc_df2;
    set ad_raw;
    retain total_daysupp est_svcdt pdaysupp;
    by &id svcdt;
    if first.&id then do;
        total_daysupp = 0;
        est_svcdt = svcdt;
    end;
    else if svcdt >= (est_svcdt + pdaysupp) then do;
        est_svcdt = svcdt;
    end;
    else do;
        est_svcdt = est_svcdt + pdaysupp;
    end;

    pdaysupp = daysupp;
    total_daysupp + daysupp;

    if last.&id;
    format est_svcdt mmddyy10.;

    pdc_df2 = (total_daysupp - max(0, (est_svcdt + pdaysupp - enddt))) / &followUpDays;
    pdc_df3 = min(total_daysupp / &followUpDays, 1);
run;

********Integrate MPR and PDC***********;
data &outdsn.(index = (&id) keep=&id mpr_flag mpr_df1 mpr_df2 mpr_df3 pdc_df1 pdc_df2 pdc_df3);
    merge ad_mpr_df ad_pdc_df1 ad_pdc_df2;
    by &id;
    label mpr_df1="MPR(definition 1)"
           mpr_df2="MPR(definition 2)"
           mpr_df3="MPR(definition 3)"
           pdc_df1="PDC(definition 1)"
           pdc_df2="PDC(definition 2)"
           pdc_df3="PDC(definition 3)"
           ;
run;
%mend;
