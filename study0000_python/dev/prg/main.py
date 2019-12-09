# importing standard python modules
import sqlite3
import pandas as pd
import sys
import os


#########################################################################################
#                                   define directory
#########################################################################################

# The project directory will be defined automatically


file_dir = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.realpath(file_dir+"/../../")
util_dir = os.path.join(proj_dir,'util')
sys.path.append(util_dir)
print(f"The study directory is {proj_dir}")


from studysetup import heor_study
from IdDxPT_IdRxPT import execute_n_drop, IdDxPT, IdRxPT

if __name__ == "__main__":
    pass
    import os
    # from studysetup import  heor_study
    s = heor_study(proj_dir=proj_dir, import_raw_sas=True)
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
    codes = pd.read_sql_query("SELECT icd9 FROM scd.icd9", s.db).iloc[:,0].tolist()
    dxVar = 'pdx dx1 dx2'
    total_rows = IdDxPT(db_conn=s.db ,dbLib='raw', dbList = "ccae,mdcr", scope = "s,o",
                        codes=codes, dxVar=dxVar, stDt=s.study_start, edDt=s.study_end)
    
    from timeit import default_timer as timer
    execution_time = timer()

    execute_n_drop(conn_or_cur=c, 
                   sql_expr='''create table dummy11 as select a.* from outDsn as a
                                inner join scd.icd9 as b
                                on instr(a.dx1,b.icd9) > 0
                                ''', if_exists='replace')
    # compare it with on a.DX1 like '%' || b.icd9 || '%'
    print(pd.read_sql_query('select count(*) from dummy11',s.db))

    
    #s.db.commit()
    execution_time = timer() - execution_time
    print(execution_time)


'''
# *** basic filtering and indexing;
%include "&dir/dev/prg/rds/_01_import_codes.py";
%include "&dir/dev/prg/rds/_02_filter_patients.sas";
%include "&dir/dev/prg/rds/_03_pull_raw_data.sas";

# *** creating cohort;
%include "&dir/dev/prg/sds/_01_create_cohort.sas";

# *** analysis of cohorts condition and visits;
%include "&dir/dev/prg/ads/_01_common_conditions.sas";
%include "&dir/dev/prg/ads/_02_outcomes_resources.sas";
%include "&dir/dev/prg/ads/_03_outcomes_treatment_pattern.sas";
'''

