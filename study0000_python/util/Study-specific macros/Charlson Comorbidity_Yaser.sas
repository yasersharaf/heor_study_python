%macro  COMB_Charlson(setin = ,id = ,dxvarstr = ,outdata = ,src =);

/******************************************************************************************************
SETIN:      Input Dataset name
ID:         Variable name: Unique ID for each patient. There may be more than 1 record per patient.
DXVARSTR:   Variable names: the diagnosis codes in ICD-9, ie 'DX1-DX10'
OUTDATA:    Dataset name : output dataset that contains Charlon comorbidity index
SRC:        Libname which could be define by author
Ref:        Coding Algorithms for Defining Comorbidities in ICD-9-CM and ICD-10 Administrative Data
******************************************************************************************************/

%put initial var list is &dxvarstr;
%simplifyVarRangeList(_libname = , _dataSet = &setin, _varList = dxvarstr);
%put expanded var list is &dxvarstr;

proc sql;
create table mydata as 
select &id, %scan(&dxvarstr, 1) as dx1 from &setin
%local i;
%let i = 2;
%do %while (%scan(&dxvarstr, &i) ne );
    %let next_dx = %scan(&dxvarstr, &i);
    %put &i: &next_dx;
    union select &id, &next_dx as dx1 from &setin;
    %let i = %eval(&i+1);
%end;
quit;

/*proc sort data = &setin(keep = &id &dxvarstr)*/
/*          out = mydata;*/
/*          by &id;*/
/*run;*/

    data cci;
        set mydata;
        by &id;
        /* apply requested conditions to be defined as comorbidity:*/
        retain cc_grp_1 - cc_grp_17;
        array cc_grp {17} $ cc_grp_1 - cc_grp_17;
        /* set an array for the diagnosis */
        array dx {*} $ &dxvarstr;

        /* initialize comorbidities and comorb medication to 0 */
        if first.&id then do;
            charlson_index = 0;
            do m = 1 to 17;
                cc_grp[m] = '0';
            end;
        end;

        /*---------------------------------- diagnosis code loop ----------------------------------*/

        length icd3 $3. icd4 $4. icd5 $5.;
        /*  check each patient record for the diagnosis codes in each elx group. */
        do i = 1 to dim(dx);   /* for each set of diagnoses codes */
            if dx[i] ne '' then do;  
                icd3 = upcase(substr(left(dx[i]),1,3));
                icd4 = upcase(substr(left(dx[i]),1,4));
                icd5 = upcase(substr(left(dx[i]),1,5));

                /*************** Myocardial Infarction *********** weight = 1 ****/
                if cc_grp_1 = '0' then do;
                   if  icd3 in ('410','412') then cc_grp_1 = '1';
                end;
                label cc_grp_1 = 'Myocardial Infarction';

                 /********** Congestive Heart Failure ************ weight = 1 ********/
                if cc_grp_2 = '0' then do;
                   if icd5 in ('39891','40201','40211','40291','40401','40403','40411','40413','40491','40493')
                       or '4254' <= icd4 <= '4259' 
                       or icd3 in ('428') 
                    then cc_grp_2 = '1';
                end;
                label cc_grp_2 = 'Congestive Heart Failure';

                 /********** Periphral Vascular Disease ************* weight = 1 ****/
                if cc_grp_3 = '0' then do;
                   if icd4 in ('0930','4373','4471','5571','5579','V434')
                        or '4431' <= icd4 <= '4439' 
                        or icd3 in ('440','441')  
                   then cc_grp_3 = '1';
                end;
                label cc_grp_3 = 'Periphral Vascular Disease';

                 /************ Cerebrovascular Disease ****** weight = 1 ***********/
                 if cc_grp_4 = '0' then do;
                    if icd5 in ('36234')
                        or '430' <= icd3 <= '438' 
                    then cc_grp_4 = '1';
                 end;
                 label cc_grp_4 = 'Cerebrovascular Disease';

                /************ Dementia *************** weight = 1 ************/
                if cc_grp_5 = '0' then do;
                    if icd4 in ('2941','3312') 
                        or icd3 in ('290') 
                    then cc_grp_5 = '1';
                end;
                label cc_grp_5 = 'Dementia';

                /************ Chronic Pulmonary Disease(copd)**** weight = 1 **********/
                if cc_grp_6 = '0' then do;
                    if icd4 in ('4168','4169','5064','5081','5088') 
                        or '490' <= icd3 <= '505'
                    then cc_grp_6 = '1';
                end;
                label cc_grp_6 = 'Chronic Pulmonary Disease';

                /********* Rheumatic Disease ******* weight = 1******/
                if  cc_grp_7 = '0' then do;
                    if icd4 in ('4465', '7148') 
                        or '7100' <= icd4 <='7104' 
                        or '7140' <= icd4 <= '7142'
                        or icd3 in ('725')  
                    then cc_grp_7 = '1';
                end;
                label cc_grp_7 = 'Rheumatic Disease';

                /*************** Peptic Ulcer Disease *************** weight = 1*******/
                if cc_grp_8 = '0' then do;
                    if '531' <= icd3 <= '534' 
                    then cc_grp_8 = '1';
                end;
                label cc_grp_8 = 'Peptic Ulcer Disease';

                /************** Mild Liver Disease ******** weight = 1 *****/
                if  cc_grp_9 = '0' then do;
                    if icd5 in ('07022','07023','07032','07033','07044','07054') 
                        or icd4 in ('0706','0709','5733','5734','5738','5739','V427') 
                        or icd3 in ('570','571') 
                    then cc_grp_9 = '1';
                end;
                label cc_grp_9 = 'Mild Liver Disease';

                /********** Diabetes Without Chronic Complications *************** weight = 1*******/
                if cc_grp_10 = '0' then do;
                    if icd4 in ('2508','2509') 
                        or '2500' <= icd4 <= '2503'
                    then cc_grp_10 = '1';
                end;     
                label cc_grp_10 = 'Diabetes Without Chronic Complications';

                /********** Diabetes With Chronic Complications**************** weight = 2*******/
                if cc_grp_11 = '0' then do;
                    if '2504' <= icd4 <= '2507' 
                    then cc_grp_11= '1';
                end;
                label cc_grp_11 = 'Diabetes With Chronic Complications';

                /********** Hemiplegia or Paraplegia *************** weight = 2********/
                if cc_grp_12 = '0' then do;
                    if icd4 in ('3341','3449') 
                        or '3440' <= icd4 <= '3446'
                        or icd3 in ('342','343') 
                    then cc_grp_12 = '1';
                end;
                label cc_grp_12 = 'Hemiplegia or Paraplegia';

                /********* Renal Disease *************** weight = 2***************/
                if cc_grp_13 = '0' then do;
                    if icd5 in('40301','40311','40391','40402','40403','40412','40413','40492','40493') 
                        or icd4 in ('5880','V420','V451') 
                        or '5830' <= icd4 <= '5837' 
                        or icd3 in ('582','585','586','V56','V56')
                    then cc_grp_13 = '1';
                end;
                label cc_grp_13 = 'Renal Disease';

                /**************** Any Malignancy *************** weight = 2********/
                if cc_grp_14 = '0' then do;
                    if icd4 = '2386' 
                        or '1740'<= icd4 <= '1958' 
                        or '140' <= icd3 <= '172'  
                        or '200' <= icd3 <= '208' 
                    then cc_grp_14 = '1';
                end;
                label cc_grp_14 = 'Any Malignancy';

                /*************** Moderate or Severe Liver Disease *************** weight = 3********/
                if cc_grp_15 = '0' then do;
                    if '4560' <= icd4 <= '4562' 
                        or '5722' <= icd4 <= '5728'
                    then cc_grp_15= '1';
                end;
                label cc_grp_15 = 'Moderate or Severe Liver Disease';

                /*************** Metastatic Solid Tumor **************** weight = 6*******/
                if cc_grp_16 = '0' then do;
                    if '196' <= icd3 <= '199' 
                    then cc_grp_16 = '1';
                end;
                label cc_grp_16 = 'Metastatic Solid Tumor';

                /************** AIDS/HIV **************** weight = 6*******/
                if cc_grp_17 = '0' then do;
                    if '042' <= icd3 <= '044'  
                    then cc_grp_17 = '1';
                end;
                label cc_grp_17= 'AIDS/HIV';
            end;
        end;  
        drop &dxvarstr icd3 icd4 icd5 m; 
        if last.&id then do;
            do m = 1 to 17;
                if m <= 10 then charlson_index + input(cc_grp[m],1.0);
                else if m <= 14 then charlson_index + input(cc_grp[m],1.0)*2;
                else if m <= 15 then charlson_index + input(cc_grp[m],1.0)*3;
                else if m <= 17 then charlson_index + input(cc_grp[m],1.0)*6;
            end;
            label charlson_index = 'Charlson Comorbidity Index';
            output;
        end;
     run;


%Mend COMB_Charlson;
