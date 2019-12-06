'''********************************************************************************
TOPIC:              IMPORT MEDICAL CODES (PHASE IV - HEOR)
WRITTEN BY:         YASER GHAEDSHARAF
DATE:               AUGUST 20, 2019
DESCRIPTION:        The purpose of the script is to import the medical codes that will be used during
                    the study (ICD9-CM, HCPCS, and NDC) and save them as a sas data set.

**********************************************************************************'''
import pandas as pd
import sqlite3
from studysetup import heor_study

def ndc_marketscan_format(ndc):
    ndc_split = ndc.split("-")
    if len(ndc_split) == 3:
        ndc_split[0] = ndc_split[0].zfill(5)
        ndc_split[1] = ndc_split[1].zfill(4)
        ndc_split[2] = ndc_split[2].zfill(2)
        return ''.join(ndc_split)
    else:
        return ''.join(ndc_split).zfill(11)

def import_codes(study=None):
    assert isinstance(study, heor_study) 

    in_scd_loc = study.proj_dir + "/datain/scd/"

    #Saving ICD9-CM Codes
    scd_ICD9 = pd.read_excel(in_scd_loc+"study_codes_excel.xlsx", sheet_name='Study Code_ICD9', dtype = str)

    #Saving NDC Codes
    scd_NDC = pd.read_excel(in_scd_loc+"study_codes_excel.xlsx", sheet_name='Study Code_NDC', dtype = str)

    #Saving HCPCS Codes
    scd_HCPCS = pd.read_excel(in_scd_loc+"study_codes_excel.xlsx", sheet_name='Study Code_HCPCS', dtype = str)

    #populate missing TA with previous TA 
    scd_ICD9["TA"].fillna(method='ffill', inplace=True)

    #Remove x and . from ICD9-CM
    scd_ICD9["ICD9"] = scd_ICD9["ICD9"].apply(lambda x: x.replace("x","").replace(".",""))

    #remove "-" (hyphen) from NDC codes and convert them to the market scan format (length = 11)
    scd_NDC["NDC"] = scd_NDC["NDC"].apply(ndc_marketscan_format)

    # write study dataframes to a study dataframe
    with sqlite3.connect(study.scd_db) as c:
        scd_ICD9.to_sql('icd9', c, if_exists='replace', index=False)
        scd_NDC.to_sql('ndc', c, if_exists='replace', index=False)
        scd_HCPCS.to_sql('hcpcs', c, if_exists='replace', index=False)

