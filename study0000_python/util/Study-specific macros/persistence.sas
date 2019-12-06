%macro persistence(
        indsn =, 
        DiscTH = ,
        followUpDays = ,
        withLookforward = Y, 
        id = , 
        drugdt = ,
        drugdays = , 
        outdsn =);

    %if %upcase(&withLookforward) = Y %then %do;
        %put Note: Please make sure input data include at least &followUpDays + &DiscTH days data followed the date of first drug filled;
    %end;
    %else %do;
        %put Note: Please make sure input data include at least &followUpDays days data followed the date of first drug filled;
    %end;

    proc sort data = &indsn out = persistence_step1(keep = &id &drugdt &drugdays); by &id &drugdt; run;
    
    Data persistence_step2;
        set persistence_step1; 
        retain disc end_drugdt st_drugdt;
        by &id &drugdt;

        if first.&id then do;
            st_drugdt = &drugdt;
            end_drugdt = &drugdt + &drugdays;
            disc = '';
        end;
        else if disc = '' then do;
            if &drugdt - end_drugdt < 0 then end_drugdt + &drugdays; 
            else if &drugdt - end_drugdt <= &discth then end_drugdt = &drugdt + &drugdays; 
            else disc = 'Y';
        end; 
        format end_drugdt mmddyy10. st_drugdt mmddyy10.;

        if last.&id;
        keep &id st_drugdt end_drugdt;
    run;

    Data &outdsn(index = (&id) keep = &id time disc_flag);
        set persistence_step2;
        if end_drugdt >= st_drugdt + &followUpDays then do; disc_flag = 0; time = &followUpDays; end;
        %if %upcase(&withLookforward) ne Y %then %do;
        else if st_drugdt + &followUpDays - end_drugdt <= &DiscTH then do; disc_flag = 0; time = &followUpDays; end;
        %end;
        else do; disc_flag = 1; time = end_drugdt - st_drugdt; end;
    run;
%mend;

