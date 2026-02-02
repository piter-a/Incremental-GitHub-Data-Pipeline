import pandas as pd
import json
import os
import datetime
from pathlib import Path
from dotenv import load_dotenv
from config import RAW_DIR, CLEAN_DIR, ISSUES_DIR
from utils.guid_gen import (
    generate_guid,
    NAMESPACE_REPO,
    NAMESPACE_BRANCH,
    NAMESPACE_ISSUE,
    NAMESPACE_OWNER,
    NAMESPACE_USER
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
        self.users_df = pd.DataFrame(
            columns = [
                'user_id',
                'user_login'
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

        og_rows = len(self.repos_df)

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

        new_rows = len(self.repos_df)

        if og_rows != new_rows:
            self._log_issues(f'REPOS | {og_rows - new_rows} dropped during cleaning.')

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

        self.repos_df = self.repos_df[[
            'repo_id', 'github_repo_id', 'repo_name','full_name', 
            'description', 'topics', 'language', 'owner_id', 'github_owner_id',
            'owner_login', 'visibility', 'private', 'disabled', 'fork', 'archived',
            'default_branch', 'language', 'stargazers_count', 'watchers',
            'watchers_count', 'forks_count', 'forks', 'open_issues_count',
            'created_at', 'updated_at', 'pushed_at'
            ]]

        self._write_to_file('repos_clean', self.repos_df)
        self._log_issue(f'REPOS - Complete | {len(self.repos_df)} rows loaded.')

    def clean_issues(self):
        raw_data = self._validate_raw_file('issues_raw')
        self.issues_df = pd.json_normalize(raw_data)

        self.issues_df = self.issues_df[[
            'id',
            'repo_name',
            'number',
            'user.login',
            'user.id',
            'title',
            'state',
            'locked',
            'comments',
            'pull_request.merged_at',
            'created_at',
            'updated_at',
            'closed_at',
            'labels',
            'assignee.login',
            'assignee.id'
        ]]

        self.issues_df = self.issues_df.rename(
            columns = {
                'id' : 'github_issue_id',
                'user.login' : 'author_login',
                'user.id' : 'github_author_id',
                'pull_request.merged_at' : 'pr_merged_at',
                'assignee.login' : 'assignee_login',
                'assignee.id' : 'assignee_id'
            }
        )

        og_rows = len(self.issues_df)

        self.issues_df = self.issues_df.dropna(
            subset = [
                'github_issue_id',
                'repo_name',
                'author_login',
                'github_author_id'
            ]
        )

        self.issues_df = self.issues_df.drop_duplicates(
            subset = ['github_issue_id'],
            keep = 'last'
        )

        new_rows = len(self.issues_df)

        if og_rows != new_rows:
            self._log_issues(f'ISSUES | {og_rows - new_rows} dropped during cleaning.')

        self.issues_df['issue_id'] = self.issues_df.apply(
            lambda r: generate_guid(
                NAMESPACE_ISSUE,
                f"{r['repo_name']}|{r['number']}"
            ),
            axis = 1
        )

        self.issues_df['author_id'] = self.issues_df.apply(
            lambda r: generate_guid(
                NAMESPACE_USER,
                r['author_login']
            ),
            axis = 1
        )

        self.issues_df['assignee_id'] = self.issues_df.apply(
            lambda r: generate_guid(
                NAMESPACE_USER,
                r['assignee_login']
            ) if pd.notna(r['assignee_login']) else None,
            axis = 1
        )

        self.issues_df = self.issues_df.merge(
            self.repos_df[['repo_id', 'repo_name']],
            on = 'repo_name',
            how = 'left',
            validate = 'many_to_one'
        )

        self.issues_df = self.issues_df.drop(
            columns = ['repo_name']
        )

        missing_repos = self.issues_df['repo_id'].isna().sum()

        if missing_repos > 0:
            self._log_issue(f'ISSUES | {missing_repos} rows with missing repo_id (FK Enforcement).')

        self.issues_df = self.issues_df.dropna(
            subset = ['repo_id']
        )

        self.issues_df = self.issues_df.astype({
            'issue_id' : 'string',
            'github_issue_id' : 'Int64',
            'repo_id' : 'string',
            'author_id' : 'string',
            'assignee_id' : 'string',
            'author_login' : 'string',
            'assignee_login' : 'string',
            'number' : 'Int64',
            'title' : 'string',
            'state' : 'string',
            'comments' : 'Int64',
        })

        date_cols = ['created_at', 'updated_at', 'closed_at', 'pr_merged_at']

        for col in date_cols:
            self.issues_df[col] = pd.to_datetime(
                self.issues_df[col],
                errors = 'coerce',
                utc = True
            )

        self.issues_df['locked'] = self.issues_df['locked'].astype('Int64')

        self.issues_df['labels'] = self.issues_df['labels'].apply(
            lambda labels: ','.join(
                label['name'] for label in labels
            ) if isinstance(labels, list) and labels else None
        )

        self.issues_df = self.issues_df[[
            'issue_id', 'github_issue_id', 'number', 'author_id', 'github_author_id',
            'author_login', 'title', 'state', 'locked', 'comments', 'pr_merged_at', 'created_at',
            'updated_at', 'closed_at', 'labels', 'assignee_id', 'assignee_login', 'repo_id'
        ]]

        self._write_to_file('issues_clean', self.issues_df)
        self._log_issue(f'ISSUES - Complete | {len(self.issues_df)} rows loaded.')

    def clean_branches(self):
        raw_data = self._validate_raw_file('branches_raw')
        self.branches_df = pd.json_normalize(raw_data)

        self.branches_df = self.branches_df[[
            'name',
            'protected',
            'repo_name',
            'commit.sha'
        ]]

        self.branches_df = self.branches_df.rename(
            columns = {'commit.sha' : 'commit_sha'}
        )

        og_rows = len(self.branches_df)

        self.branches_df = self.branches_df.dropna(
            subset = ['name']
        )

        self.branches_df = self.branches_df.drop_duplicates(
            subset = ['repo_name', 'name'],
            keep = 'last'
        )

        new_rows = len(self.branches_df)

        if og_rows != new_rows:
            self._log_issues(f'BRANCHES | {og_rows - new_rows} dropped during cleaning.')

        self.branches_df['branch_id'] = self.branches_df.apply(
            lambda r: generate_guid(
                NAMESPACE_BRANCH,
                f"{r['repo_name']}|{r['name']}"
            ),
            axis = 1
        )

        self.branches_df = self.branches_df.merge(
            self.repos_df[['repo_id', 'repo_name']],
            on = 'repo_name',
            how = 'left',
            validate = 'many_to_one'
        )

        self.branches_df = self.branches_df.drop(
            columns = ['repo_name']
        )

        self.branches_df = self.branches_df.astype({
            'branch_id' : 'string',
            'repo_id' : 'string',
            'protected' : 'Int64',
            'commit_sha' : 'string'
        })

        self.branches_df['ingested_at'] = pd.Timestamp.utcnow()

        self.branches_df = self.branches_df[['branch_id', 'name', 'protected', 'commit_sha', 'repo_id', 'ingested_at']]

        self._write_to_file('branches_clean', self.branches_df)
        self._log_issue(f'BRANCHES - Complete | {len(self.branches_df)} rows loaded.')

    def clean_users(self):
        new_authors = self.issues_df[['author_id', 'author_login']].rename(
            columns = {
                'author_id' : 'user_id',
                'author_login' : 'user_login'
            }
        )

        new_assignees = self.issues_df[['assignee_id', 'assignee_login']].rename(
            columns = {
                'assignee_id' : 'user_id',
                'assignee_login' : 'user_login'
            }
        )

        new_users = pd.concat(
            [new_authors, new_assignees],
            ignore_index = True
        )

        new_users = (
            new_users
            .drop_duplicates(subset = ['user_id'])
            .dropna(subset = ['user_id', 'user_login'])
        )

        self.users_df = pd.concat(
            [self.users_df, new_users],
            ignore_index = True
        )

        og_rows = len(self.users_df)

        self.users_df = self.users_df.drop_duplicates(
            subset = ['user_id']
        )

        new_rows = len(self.users_df)

        if og_rows != new_rows:
            self._log_issues(f'USERS | {og_rows - new_rows} dropped during cleaning.')

        self.users_df = self.users_df.astype({
            'user_id' : 'string',
            'user_id' : 'string'
        })

        self._write_to_file('users_clean', self.users_df)
        self._log_issue(f'USERS - Complete | {len(self.users_df)} rows loaded.')

    def clean_owners(self):
        new_owners = self.repos_df[['owner_id', 'owner_login']]

        self.owners_df = pd.concat(
            [self.owners_df, new_owners],
            ignore_index = True
        )

        og_rows = len(self.owners_df)

        self.owners_df = (
            self.owners_df
            .dropna(subset = ['owner_id', 'owner_login'])
            .drop_duplicates(subset = ['owner_id'])
            .reset_index(drop = True)
        )
        
        new_rows = len(self.owners_df)

        if og_rows != new_rows:
            self._log_issue(f'OWNERS | {og_rows - new_rows} dropped during cleaning.')

        self._write_to_file('owners_clean', self.owners_df)
        self._log_issue(f'OWNERS - Complete | {len(self.owners_df)} rows loaded.')


data_cleaner = CleanData()
data_cleaner.clean_repos()
data_cleaner.clean_owners()
data_cleaner.clean_branches()
data_cleaner.clean_issues()
data_cleaner.clean_users()