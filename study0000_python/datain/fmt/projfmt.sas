proc format;
    
    value plantyp
        .=".-Missing/Unknown    "
        1="1-Basic/Major Medical"
        2="2-Comprehensive      "
        3="3-EPO                "
        4="4-HMO                "
        5="5-POS                "
        6="6-PPO                "
        7="7-POS with Capitation"
        8="8-CDHP               "
        9="9-HDHP               "
    ;
    
    value plan
        1,2,3,5,6,8,9="1-Fee for service"
        4,7="3-HMO and POS capitation"
        other="2-Unknown"
    ;
        
    value $region
        "1"="1-Northeast Region           "
        "2"="2-North Central Region       "
        "3"="3-South Region               "
        "4"="4-West Region                "
        "5"="5-Unknown Region             "
    ;
    
    value $sex
        " "="  Missing/Unknown"
        "1"="1-Male           "
        "2"="2-Female         "
    ;
run;

/*****************************************************************************************************************
***** Types of service utilization;
*****************************************************************************************************************/
*** Emergency room;
%Macro EmergencyRoom_ ;
    (Year <=2010 and (
     (StdPlac=23) OR
     (StdPlac in (21,22,28) and ((StdProv=220 and StdSvc ne 104) or (StdSvc=77))) OR
     (ProcGrp in (110,111,114)) OR
     (length(left(revcode))=3 AND revcode in: ('45')) OR
     (length(left(revcode))=4 AND revcode in: ('045')) 
     )
    )
    OR
    (Year >= 2011 and 
      substr(left(Svcscat), 4, 2) in ('20')
    )
%Mend EmergencyRoom_ ;

*** Office visit;
Proc Format;
    Value $OFVSRV 
     "99201"-"99215",
     "99241"-"99245",
     "99381"-"99387",
     "99391"-"99397",
     "99401"-"99404",
     "99411"-"99412",
     "99420"-"99429" = "Y"
     Other           = "N"
    ;
Run;
%Macro OfficeVisit_ ;
    (
    scope = 'O' AND
        ( 
        (StdPlac in (11, 22, 95) and ProcGrp in (101,104,109)) OR
        (put(Proc1,$OFVSRV.)='Y' and length(proc1)=5)
        )
    ) 
%Mend OfficeVisit_;

%Macro OPVisit_ ;
    ( 
    %OfficeVisit_ OR
    REVCODE in ("0450","0510","0515","0517","0520","0521","0523","0526","0760","0761","0762","0769","0770","0779","0982","0983","0988")
    ) 
%Mend OPVisit_;

*** Lab Test;
%macro LabTest_ ;
    (
    scope = 's' AND
        (
        (ProcTyp IN ('1') AND Substr(Proc1,1,1) = '8') OR
        (ProcGrp in (301,302,303,331,332,334,335,336,338,339)) OR
        (length(left(RevCode))=3 AND RevCode IN: ('30','31')) OR
        (length(left(RevCode))=4 AND RevCode IN: ('030','031'))
        )
    )
%mend LabTest_ ;

*** Radiology;
%macro Radiology_ ;
    (
    (PROCTYP IN ('1') AND Substr(Proc1,1,1) = '7') OR
    (Proc1 IN ('0073T','0082T','0083T')) OR
    (201 <= ProcGrp <= 299) OR
    (length(left(RevCode))=3 AND RevCode IN: ('32','33')) OR
    (length(left(RevCode))=4 AND RevCode IN: ('032','033'))  
    )
%mend Radiology_ ;

/*****************************************************************************************************************
***** Rule out diagnosis;
*****************************************************************************************************************/
Proc Format;
    Value $RULEOUT 
    '70010'-'76999',
    '78000'-'78799',
    '80000'-'89999',
    '36400'-'36425',
    'S9529','G0001' = "Y"
    Other           = "N";
Run;
%Macro Ruleout_;
  (put(proc1,$ruleout.) eq 'Y'  and length(proc1)=5)
%Mend Ruleout_;

