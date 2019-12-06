/**********************************************************************************
TOPIC:              TREATMENT EXERCISE (PHASE II - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 01, 2019
DESCRIPTION:        The purpose of this script is to clean the supply days and choose the
                    largest number of supply days for each patient and specific drug and
                    furthermore, replace the missing or negative supply days with the mode
                    of the positive supply days for the existing population taking the specific
                    drug.
************************************************************************************/

/*Supply Days Cleaning using PROC SQL*/
%MACRO clean_daysupp_sql(indsn = , outDsn = , DAYSUPP = DAYSUPP, ndcnum = ndcnum);
    PROC SQL;
    CREATE TABLE _ADJUST_DAYSUPP_STEP1_A AS 
    SELECT *
    FROM &INDSN
    ORDER BY ENROLID, SVCDATE, &NDCNUM. ASC, &DAYSUPP. DESC
    ;
    QUIT;

    PROC SQL;
    CREATE TABLE _ADJUST_DAYSUPP_STEP1_B(DROP = N) AS 
    SELECT *, MIN(MONOTONIC()) AS N
    FROM _ADJUST_DAYSUPP_STEP1_A
    GROUP BY ENROLID, SVCDATE, &NDCNUM.
    HAVING N = MONOTONIC();
    QUIT;

    /*/*SQL shortcut*/;*/
    /*PROC SQL;*/
    /*CREATE TABLE _ADJUST_DAYSUPP_STEP1_B_SHORTCUT AS */
    /*SELECT DISTINCT ENROLID, SVCDATE, &NDCNUM., MAX(&DAYSUPP.) AS &DAYSUPP.*/
    /*FROM &INDSN*/
    /*GROUP BY ENROLID, SVCDATE, &NDCNUM.*/
    /*;*/
    /*QUIT;*/

    ;
    PROC SQL;
    CREATE TABLE _ADJUST_DAYSUPP_STEP2_A AS 
    SELECT &NDCNUM., &DAYSUPP., COUNT(*) as COUNT
    FROM _ADJUST_DAYSUPP_STEP1_B
    WHERE &DAYSUPP.>0
    GROUP BY &NDCNUM., &DAYSUPP.
    ORDER BY  &NDCNUM., COUNT DESC;
    QUIT;

    PROC SQL;
    CREATE TABLE _ADJUST_DAYSUPP_STEP2_B(DROP = FIRST_OBS_ROW) AS 
    SELECT &NDCNUM., &DAYSUPP., MIN(MONOTONIC()) AS FIRST_OBS_ROW
    FROM _ADJUST_DAYSUPP_STEP2_A
    GROUP BY &NDCNUM.
    HAVING  FIRST_OBS_ROW = MONOTONIC();
    QUIT;

    PROC SQL;
    CREATE TABLE &OUTDSN AS 
    SELECT X.*, CASE
                    WHEN X.&DAYSUPP._DUMMY LE 0 THEN COALESCE(Y.&DAYSUPP._DUMMY,0)
                    ELSE X.&DAYSUPP._DUMMY
                END AS &DAYSUPP
    FROM _ADJUST_DAYSUPP_STEP1_B(RENAME = (&DAYSUPP = &DAYSUPP._DUMMY)) AS X
    LEFT JOIN _ADJUST_DAYSUPP_STEP2_B(RENAME = (&DAYSUPP = &DAYSUPP._DUMMY)) AS Y
    ON Y.&NDCNUM = X.&NDCNUM
    WHERE CALCULATED &DAYSUPP > 0
    ;QUIT;
%MEND clean_daysupp_sql;

/*Supply Days Cleaning using DATA STEP*/
%MACRO clean_daysupp_sas(indsn = , outDsn = , DAYSUPP = DAYSUPP, ndcnum = ndcnum);
    proc sort data = &indsn out = _adjust_daysupp_step1_a;
        by enrolid svcdate &ndcnum. descending &daysupp.;
    run;

    data _adjust_daysupp_step1_b;
        set _adjust_daysupp_step1_a;
            by enrolid svcdate &ndcnum. descending &daysupp.;
            row_num = _N_;
            if first.&ndcnum. then output;
    run;

    data _adjust_daysupp_step1_b;
        set _adjust_daysupp_step1_a;
            by enrolid svcdate &ndcnum. descending &daysupp.;
            if first.&ndcnum. then output;
    run;

    proc freq data = _adjust_daysupp_step1_b (where = (&daysupp. > 0)) order = freq noprint; 
        tables &ndcnum. * &daysupp. / out = _adjust_daysupp_step2_a; 
    run;

    proc sort data = _Adjust_DaySupp_step2_a; by &ndcnum. descending count &daysupp.; run;

    data _adjust_daysupp_step2_b(index = (&ndcnum.));
        set _Adjust_DaySupp_step2_a; 
            by &ndcnum. descending count &daysupp.;
            if first.&ndcnum. then output;
            keep &ndcnum. &daysupp.;
    run;


    data &outDsn;
          set _adjust_daysupp_step1_b;
                if &daysupp. <= 0 then do; 
                      set _adjust_daysupp_step2_b key = &ndcnum./unique;
                      if _iorc_ ne 0 then do; _error_ = 0; &daysupp. = 0; end;
                end;
                if &daysupp. > 0 then output;
    run;
%mend clean_daysupp_sas;
