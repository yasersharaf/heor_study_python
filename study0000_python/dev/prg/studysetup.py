# '''********************************************************************************
# TOPIC:              Study Setup (PHASE IV - HEOR)
# WRITTEN BY:         YASER GHAEDSHARAF
# DATE:               AUGUST 20, 2019
# DESCRIPTION:        Setup study tables and variables
# **********************************************************************************'''
import sqlite3
import pandas as pd
import sys
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.realpath(file_dir+"/../../")
util_dir = os.path.join(proj_dir,'util')
sys.path.append(util_dir)
print(f"The study directory is {proj_dir}")

from IdDxPT_IdRxPT import *



chunksize = 1*10**3
def sas7bdat2sql(sas_file, db_conn=None, chunksize=10**4, if_exists='replace'):
    '''# Should be in try catch block'''
    df = pd.read_sas(sas_file, encoding='latin-1', chunksize=chunksize)
    ds_name = sas_file.split('/')[-1].split('.')[0]
    print(f"\n\nReading {df.row_count} records from {sas_file.split('/')[-1]} table ...")
    read_so_far = 0
    for df_chunk in df:
        # print(df.info())
        # print(f"if_exists: {if_exists}")
        df_chunk.to_sql(ds_name, db_conn, if_exists=if_exists, index=False)
        # if it did not fail, it will append the rest. 
        if_exists = 'append'
        read_so_far += df_chunk.shape[0]
        sys.stdout.write('\r')
        sys.stdout.write(f"\t Read {read_so_far/df.row_count*100:.2f}% records to sql table {ds_name}")



class heor_study():
    def __init__(self, proj_dir=None, import_raw_sas=True, refresh_raw_sas=False):
    
#########################################################################################
#                                 define work environment
#########################################################################################
# TODO: use os.path.join instead of concatenation

        self.proj_dir = proj_dir
        self.WorkEnvir = self.proj_dir+'/dev'
        


#########################################################################################
#                                  define data libarary
#########################################################################################

        self.db = sqlite3.connect(self.proj_dir+'/study.db', isolation_level=None)       

        self.raw_db = self.proj_dir+'/datain/raw.db'
        self.rds_db = self.proj_dir+'/datain/rds.db'
        self.scd_db = self.proj_dir+'/datain/scd.db'
        self.sds_db = self.WorkEnvir+'/drv/sds.db'
        self.ads_db = self.WorkEnvir+'/drv/ads.db'
        self.fds_db = self.WorkEnvir+'/drv/scd.db'


        with self.db as c:
            c.execute(f"ATTACH DATABASE ':memory:' AS memory")
            c.execute(f"ATTACH DATABASE '{self.raw_db}' AS raw")
            c.execute(f"ATTACH DATABASE '{self.rds_db}' AS rds")
            c.execute(f"ATTACH DATABASE '{self.scd_db}' AS scd")
            c.execute(f"ATTACH DATABASE '{self.sds_db}' AS sds")
            c.execute(f"ATTACH DATABASE '{self.ads_db}' AS ads")
            c.execute(f"ATTACH DATABASE '{self.fds_db}' AS fds")

                        
        print('\nImporting the following sas files to the sql database.')
        # print('\n'.join(self.sas_dataset_list))

        if import_raw_sas:
            self.sas_dataset_list = []
            self.raw_sas_dir = self.proj_dir+'/datain/raw'
            for file in os.listdir(self.raw_sas_dir):
                if file.endswith(".sas7bdat"):
                    self.sas_dataset_list.append(os.path.join(self.raw_sas_dir, file))
                    with sqlite3.connect(self.raw_db) as c:
                        try:
                            sas7bdat2sql(self.sas_dataset_list[-1], db_conn=c, chunksize=chunksize, if_exists='fail')
                        except ValueError as identifier:
                            print("Warning:", identifier)
                            print("Assign True to refresh_raw_sas to overwrite an existing sql raw dataset")
                            if refresh_raw_sas:
                                sas7bdat2sql(self.sas_dataset_list[-1], db_conn=c, chunksize=chunksize, if_exists='replace')
                            # c.commit()
                            # curr = c.cursor()
                            # curr.execute("SELECT name FROM sqlite_master WHERE type='table';")
                            # print(curr.fetchall())
                                




        print(type(c))
        # assert isinstance(self.db, sqlite3.Connection) and \
        #        isinstance(self.raw_db, str) and\
        #        isinstance(self.rds_db, str) and\
        #        isinstance(self.scd_db, str) and\
        #        isinstance(self.sds_db, str) and\
        #        isinstance(self.ads_db, str) and\
        #        isinstance(self.fds_db, str)



    
# load formats;
#%inc "&Dir/datain/fmt/projfmt.sas";
#%inc "&Dir/datain/fmt/projfmt_new.sas";

# load study macros;
#%inc "&Dir/util/Study-specific macros/Charlson Comorbidity_Yaser.sas";
#%inc "&Dir/util/Study-specific macros/adherence.sas";
#%inc "&Dir/util/Study-specific macros/persistence.sas";

# other macros;
#%include "&Dir/util/IdDxPT_IdRxPT.sas"; #pull tables*/
#%include "&Dir/util/suppdays_cleaning.sas"; #daysupp cleaning macros*/
#%include "&Dir/util/MACRO_CONT_ENROLL.sas"; #Continuous enrollment*/
#%include "&Dir/util/vec2mat.sas"; #Used in Common conditions*/
#%include "&Dir/util/misc_macros.sas"; #Used in Outcomes analysis*/
#%include "&Dir/util/simplifyVarRangeList.sas";
#%include "&Dir/util/kmplot_format.sas";#For treatment plan*/

#Statitical analysis macros
#%include "&Dir/util/desc_stat_n.sas";
#%include "&Dir/util/desc_stat_d.sas";
#%include "&Dir/util/desc_stat_c.sas";
#%include "&Dir/util/desc_stat.sas";

# '''************************************************************************************
#                                 define macro variables
# ************************************************************************************'''
        # define data extraction period;
        self.index_start = pd.Timestamp(year=2007,month=1,day=1)
        self.index_end = pd.Timestamp(year=2007,month=12,day=31)

# define study period;
        self.study_start = pd.Timestamp(year=2006,month=1,day=1)
        self.study_end = pd.Timestamp(year=2008,month=12,day=31)

# define baseline in days;
        self.preeval_length = 365

# define look-foward in days;
        self.eval_length = 364

# define look-foward in days;
        self.drug_followup = 300

# define age range;
        self.min_age = 18

# define continuous enrollment gap;
        self.CE_gap = 45

# define prescription gap;
        self.Rx_gap = 60

    def attach_dbs(self):
        self.db.execute()
        

# dtype_O = {'SEQNUM': int,  'VERSION': int,\
# 'ENROLID': int, 'PROC1': str,\
# 'PROVID': float, 'REVCODE': float,\
# 'SVCDATE': str, 'PATID': int,\
# 'DOBYR': int, 'YEAR': int,\
# 'AGE': int, 'CAP_SVC': str,\
# 'COB': float, 'COINS': float,\
# 'COPAY': float, 'DEDUCT': float,\
# 'DX1': str, 'DX2': str,\
# 'EMPZIP': float, 'FACHDID': float,\
# 'FACPROF': str, 'MHSACOVG': float,\
# 'NETPAY': float, 'NTWKPROV': str,\
# 'PAIDNTWK': str, 'PAY': float,\
# 'PDDATE': str, 'PLANTYP': float,\
# 'PROCGRP': float, 'PROCMOD': str,\
# 'PROCTYP': str, 'PROVZIP': float,\
# 'QTY': int, 'SVCSCAT': int,\
# 'TSVCDAT': str, 'MDC': int,\
# 'REGION': int, 'MSA': float,\
# 'STDPLAC': float, 'STDPROV': float,\
# 'STDSVC': float, 'DATATYP': int,\
# 'PLANKEY': float, 'WGTKEY': float,\
# 'AGEGRP': int, 'EECLASS': int,\
# 'EESTATU': int, 'EGEOLOC': int,\
# 'EIDFLAG': int, 'EMPREL': int,\
# 'ENRFLAG': int, 'INDSTRY': float,\
# 'PATFLAG': int, 'PHYFLAG': int,\
# 'RX': int, 'SEX': int,\
# 'HLTHPLAN': int, 'EMPCTY': float,\
# 'PROVCTY': float}

# dtype_S = {'SEQNUM': int, 'VERSION': int, 'DOBYR': int, 'YEAR': int, 'ADMDATE': str, 'AGE': int, 'CAP_SVC': str,\
# 'CASEID': int, 'COB': float, 'COINS': float, 'COPAY': float, 'DEDUCT': float, 'DISDATE': str, 'DRG': int,\
# 'DX1': str, 'DX2': str, 'FACHDID': float, 'FACPROF': str, 'HOSPZIP': float, 'MHSACOVG': float, 'NETPAY': float,\
# 'NTWKPROV': str, 'PAIDNTWK': str, 'PAY': float, 'PDDATE': str, 'PDX': str, 'PPROC': float, 'PROC1': str,\
# 'PROCMOD': str, 'PROCTYP': str, 'PROVID': float, 'PROVZIP': float, 'QTY': int, 'REVCODE': float, 'SVCDATE': str,\
# 'SVCSCAT': int, 'TSVCDAT': str, 'UNIHOSP': float, 'ADMTYP': str, 'MDC': int, 'DSTATUS': float, 'STDPLAC': int,\
# 'STDPROV': float, 'STDSVC': float, 'WGTKEY': float, 'ENROLID': int, 'EMPZIP': float, 'PLANTYP': float, 'REGION': int,\
# 'MSA': float, 'DATATYP': int, 'PATID': int, 'PLANKEY': float, 'AGEGRP': int, 'EECLASS': int, 'EESTATU': int, 'EGEOLOC': int,\
# 'EIDFLAG': int, 'EMPREL': int, 'ENRFLAG': int, 'INDSTRY': float, 'PATFLAG': int, 'PHYFLAG': int, 'RX': int, 'SEX': int,\
# 'HLTHPLAN': int, 'EMPCTY': float, 'HOSPCTY': float, 'PROVCTY': float}

# dtype_T = {'SEQNUM': int, 'VERSION': int, 'ENROLID': int, 'DTEND': str, 'DTSTART': str, 'EMPZIP': float, 'MEMDAYS': int,\
# 'MHSACOVG': float, 'PLANTYP': float, 'AGE': int, 'DOBYR': int, 'REGION': int, 'MSA': float, 'DATATYP': int, 'PLANKEY': float,\
# 'WGTKEY': float, 'AGEGRP': int, 'EECLASS': int, 'EESTATU': int, 'EGEOLOC': int, 'EMPREL': int, 'INDSTRY': float, 'PHYFLAG': int,\
# 'RX': int, 'SEX': int, 'HLTHPLAN': int, 'EMPCTY': float}

# dtype_D = {'SEQNUM': int, 'VERSION': int, 'ENROLID': int, 'NDCNUM': int, 'PHARMID': float, 'SVCDATE': str, 'PATID': int,\
# 'DOBYR': int, 'YEAR': int, 'AGE': int, 'AWP': float, 'CAP_SVC': str, 'COB': float, 'COINS': float, 'COPAY': float,\
# 'DAYSUPP': int, 'DEDUCT': float, 'DISPFEE': float, 'EMPZIP': float, 'GENERID': float, 'INGCOST': float, 'METQTY': float,\
# 'MHSACOVG': float, 'NETPAY': float, 'NTWKPROV': str, 'PAIDNTWK': str, 'PAY': float, 'PDDATE': str, 'PHRMZIP': float,\
# 'PLANTYP': float, 'QTY': int, 'REFILL': float, 'RXMR': float, 'SALETAX': float, 'THERCLS': float, 'DAWIND': float, 'DEACLAS': float,\
# 'GENIND': float, 'MAINTIN': float, 'THERGRP': float, 'REGION': int, 'MSA': float, 'DATATYP': int, 'PLANKEY': float, 'WGTKEY': float,\
# 'AGEGRP': int, 'EECLASS': int, 'EESTATU': int, 'EGEOLOC': int, 'EIDFLAG': int, 'EMPREL': int, 'ENRFLAG': int, 'INDSTRY': float,\
# 'PATFLAG': int, 'PHYFLAG': int, 'SEX': int, 'HLTHPLAN': int, 'EMPCTY': float, 'PHRMCTY': float}


# table_dtype = {'D':dtype_D, 'O':dtype_O, 'S':dtype_S, 'T':dtype_T}

if __name__ == "__main__":
    pass
    import os
    from studysetup import  heor_study
    s = heor_study(proj_dir=proj_dir)
    c = s.db.cursor()
    
#     try:
#         scd = sqlite3.connect(s.scd_db)
#         cscds = scd.cursor()    
#         cscds.execute("""CREATE TABLE mytable
#                  (start, end, score)""")

#         scd.commit()
#         scd.close()
#     except Exception as ex:
#         pass
#         print('passing')
#         scd = sqlite3.connect(s.scd_db)
#         cscds = scd.cursor()    
#         cscds.execute("SELECT name FROM sqlite_master WHERE type='table';")
#         print(cscds.fetchall())
        
#         cscds.execute("""INSERT INTO mytable (start, end, score)
#               values(1, 99, 123);""")
#         cscds.execute("SELECT * FROM mytable;")
#         print(cscds.fetchall())
#         scd.commit()
        
#         scd.commit()
#         scd.close()
#         print(ex)
    


#     c.execute("SELECT * FROM scd.mytable")
#     print(c.fetchall())
#     s.db.commit()
#     c.fetchall()
    execute_n_drop(conn_or_cur=c, sql_expr="""CREATE TABLE scd.mytable
                 (start, end, new_score_5)""")
    s.db.commit()

    from _01_import_codes import import_codes
    import_codes(study=s)
    s.db.commit()
    
    c.execute("SELECT * FROM scd.sqlite_master;")
    print("\n\n scd_fetchal:\n",c.fetchall())

    df = pd.read_sql_query("SELECT * FROM scd.sqlite_master;", s.db)

    from IPython.display import display, HTML
    display(HTML(df.to_html()))
    print(df)

    # 02_filter->idDxPt

    total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o")

