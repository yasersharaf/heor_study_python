/**********************************************************************************
TOPIC:              CONTINUOUS ENROLLMENT PERIOD MACRO
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               JULY 29, 2019
DESCRIPTION:        This script is a macro to find the continuous enrollment periods 
                    within a data set and then separate the keep the index_dates that
                    are within an interval of the continuous enrollment.
MODIFICATION:       1- Modified PR_END for the case that it is the period of the ID to use max function
                    2- added index var
************************************************************************************/



%MACRO CONT_ENROLL_PERIODS(ENROLL_DATA = , ID_VAR = ENROLID, GRACE_PERIOD = ,PRIOR_DAYS = ,POST_DAYS = ,IDXDATE_VAR = IDXDATE );

    %PUT Sorting &ENROLL_DATA ...;
    PROC SORT DATA = &ENROLL_DATA;
    BY &ID_VAR DTSTART DTEND;
    RUN;

    %PUT Performing Step 1 to create &ENROLL_DATA._STEP1 dataset ...;
    DATA &ENROLL_DATA._STEP1(DROP = DTSTART DTEND DUMMY_DTSTART DUMMY_DTEND DIFF);
    SET &ENROLL_DATA;
    BY &ID_VAR;
    FORMAT DUMMY_DTSTART MMDDYY10. DUMMY_DTEND MMDDYY10. PR_START MMDDYY10. PR_END MMDDYY10.;
    RETAIN DUMMY_DTSTART DUMMY_DTEND;
    DIFF = DTSTART - DUMMY_DTEND;
    IF FIRST.&ID_VAR THEN DO;
        DUMMY_DTSTART = DTSTART;
        DUMMY_DTEND = DTEND;
    END;
    IF (NOT FIRST.&ID_VAR) AND  (DIFF > &GRACE_PERIOD) THEN DO;
        PR_START = DUMMY_DTSTART;
        PR_END = DUMMY_DTEND;
        OUTPUT;
    END;

    IF DTSTART - LAG(DTEND) > &GRACE_PERIOD THEN DO;
        DUMMY_DTSTART = DTSTART;
    END;

    IF LAST.&ID_VAR THEN DO;
        PR_START = DUMMY_DTSTART;
        PR_END = MAX(DTEND,DUMMY_DTEND);
        OUTPUT;
    END;
    DUMMY_DTEND = MAX(DTEND,DUMMY_DTEND);
    RUN;
    %PUT Performing Step 2 to create &ENROLL_DATA._STEP2 dataset ...;
    DATA &ENROLL_DATA._STEP2;
    SET &ENROLL_DATA._STEP1;
        IF (PR_START LE &IDXDATE_VAR - &PRIOR_DAYS) AND (PR_END GE &IDXDATE_VAR + &POST_DAYS) THEN 
        OUTPUT;
    RUN;

%MEND;
