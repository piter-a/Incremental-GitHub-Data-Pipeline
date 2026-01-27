import os   

BASE_DIR = 'data'
RAW_DIR = os.path.join(BASE_DIR, 'raw_data')
CLEAN_DIR = os.path.join(BASE_DIR, 'clean_data')
ISSUES_DIR = os.path.join(BASE_DIR, 'issue_log')

for directory in [RAW_DIR, CLEAN_DIR, ISSUES_DIR]:
    os.makedirs(directory, exist_ok = True)