import pandas as pd
import json
import os
import datetime
import uuid
from pathlib import Path
from dotenv import load_dotenv
from config import RAW_DIR, CLEAN_DIR, ISSUES_DIR
from utils.guid_gen import (
    generate_guid,
    NAMESPACE_REPO,
    NAMESPACE_BRANCH,
    NAMESPACE_ISSUE,
    NAMESPACE_OWNER
)

class CleanData:
    def __init__(self):
        self.repos_df = pd.DataFrame()
        self.issues_df = pd.DataFrame()
        self.branches_df = pd.DataFrame()
        self.owners_df = pd.DataFrame(
            columns = [
                'owner_id',
                'owner_login'
            ]
        )

    def _log_issue(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(os.path.join(ISSUES_DIR, 'pipeline_error_log.txt'), 'a', encoding = 'UTF-8') as issue_log:
            issue_log.write(f'{timestamp}: {message}\n')

    def _write_to_file(self, file_name, df):
        curr_path = Path(CLEAN_DIR) / f'{file_name}.csv'
        backup_path = Path(CLEAN_DIR) / f'{file_name}_backup.csv'

        if backup_path.exists():
            backup_path.unlink()
        
        if curr_path.exists():
            curr_path.rename(backup_path)

        df.to_csv(
            curr_path,
            index = False,
            encoding = 'UTF-8'
        )
    
    def _validate_raw_file(self, file_name):
        file_path = Path(RAW_DIR) / f'{file_name}.json'

        if not file_path.exists():
            self._log_issue(f'{file_name}.json does not exist!')
            raise FileNotFoundError

        if os.path.getsize(file_path) == 0:
            self._log_issue(f'{file_name}.json is empty!')
            raise ValueError
        
        try:
            with open(file_path, 'r', encoding = 'UTF-8') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            self._log_issue(f'{file_name}.json contains invalid JSON: {e}')
            raise ValueError
        
        if not isinstance(data, list):
            self._log_issue(f'Incorrect top-level type in {file_name}.json')
            raise TypeError
        
        return data
    
    def clean_repos(self):
        raw_data = self._validate_raw_file('repos_raw')
        self.repos_df = pd.json_normalize(raw_data)

        self.repos_df = self.repos_df[[
            'id',
            'name',
            'full_name',
            'description',
            'topics',
            'language',
            'owner.id',
            'owner.login',
            'visibility',
            'private',
            'disabled',
            'fork',
            'archived',
            'default_branch',
            'language',
            'stargazers_count',
            'watchers',
            'watchers_count',
            'forks_count',
            'forks',
            'open_issues_count',
            'created_at',
            'updated_at',
            'pushed_at'
        ]]

        # Rename cols to match SQL format

        self.repos_df = self.repos_df.rename(
            columns = {
                'id' : 'github_repo_id',
                'name' : 'repo_name',
                'owner.id' : 'github_owner_id',
                'owner.login' : 'owner_login',  
            }
        )

        # Drop nulls

        self.repos_df = self.repos_df.dropna(
            subset = [
                'github_repo_id',
                'github_owner_id',
                'owner_login'
            ]
        )

        # Dedupe on repo_id, keep latest one if more than one exist

        self.repos_df = self.repos_df.drop_duplicates(
            subset = ['github_repo_id'],
            keep = 'last'
        )

        # Generate GUIDs for repo_id

        self.repos_df['repo_id'] = self.repos_df.apply(
            lambda r: generate_guid(
                NAMESPACE_REPO,
                f"{r['owner_login']}|{r['repo_name']}"
            ),
            axis = 1
        )

        self.repos_df['owner_id'] = self.repos_df.apply(
            lambda r: generate_guid(
                NAMESPACE_OWNER,
                r['owner_login']
            ),
            axis = 1
        )

        # Data type casting

        self.repos_df = self.repos_df.astype({
            'repo_id': 'string',
            'repo_name': 'string',
            'full_name': 'string',
            'description': 'string',
            'language': 'string',
            'owner_id': 'string',
            'owner_login': 'string',
            'visibility': 'string',
            'default_branch': 'string',
            'stargazers_count': 'int64',
            'watchers': 'int64',
            'watchers_count': 'int64',
            'forks': 'int64',
            'forks_count': 'int64',
            'open_issues_count': 'int64',
        })

        self.repos_df['topics'] = self.repos_df['topics'].apply(
            lambda x: ','.join(x) if isinstance(x, list) else None
        )

        date_cols = ['created_at', 'updated_at', 'pushed_at']
        
        for col in date_cols:
            self.repos_df[col] = pd.to_datetime(
                self.repos_df[col], 
                errors='coerce', 
                utc=True)
            
        bool_cols = ['private', 'disabled', 'fork', 'archived']

        for col in bool_cols:
            self.repos_df[col] = self.repos_df[col].astype('Int64')

        new_owners = self.repos_df[['owner_id', 'owner_login']]

        self.owners_df = pd.concat(
            [self.owners_df, new_owners],
            ignore_index = True
        )

        self._write_to_file('repos_clean', self.repos_df)

    def clean_owners(self):
        self.owners_df = (
            self.owners_df
            .dropna(subset = ['owner_id', 'owner_login'])
            .drop_duplicates(subset = ['owner_id'])
            .reset_index(drop = True)
        )
        
        self._write_to_file('owners_clean', self.owners_df)


data_cleaner = CleanData()
data_cleaner.clean_repos()
data_cleaner.clean_owners()