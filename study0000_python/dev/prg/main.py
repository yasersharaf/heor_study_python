import os

#########################################################################################
#                                   define directory
#########################################################################################
# The project directory will be defined automatically

file_dir = os.path.dirname(os.path.abspath(__file__))
proj_dir = os.path.realpath(file_dir+"/../../")
print(f"The study directory is {proj_dir}")


# *** study setup;
from studysetup import heor_study
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



if __name__ == "__main__":
    s = heor_study(proj_dir=proj_dir)
    s.db.