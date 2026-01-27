import json
import requests
import os
import datetime
from pathlib import Path
from dotenv import load_dotenv
from config import RAW_DIR, ISSUES_DIR

class ExtractData:
    def __init__ (self):
        load_dotenv('pipeline.env')
        self.pat = os.getenv('GITHUB_TOKEN')
        self.base_url = 'https://api.github.com/'
        self.headers = {
           'Authorization' : f'Bearer {self.pat}',
           'Accept' : 'application/vnd.github+json',
           'User-Agent' : 'github-issues-pipeline'
        }
        self.max_pages = 3
        self.per_page = 100
        self.owner = 'microsoft'

    def _write_to_file(self, file_name, file_contents):
        curr_path = Path(RAW_DIR) / f'{file_name}.json'
        backup_path = Path(RAW_DIR) / f'{file_name}_backup.json'

        if backup_path.exists():
            backup_path.unlink()
        
        if curr_path.exists():
            curr_path.rename(backup_path)

        with open(os.path.join(RAW_DIR, f'{file_name}.json'), 'w', encoding = 'UTF-8') as file:
            json.dump(file_contents, file, ensure_ascii = False, indent = 4)

    def _log_issue(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(os.path.join(ISSUES_DIR, 'pipeline_error_log.txt'), 'a', encoding = 'UTF-8') as issue_log:
            issue_log.write(f'{timestamp}: {message}\n')


    def fetch_repos(self):
        page = 1
        repo_names = []
        repos_data = []

        while page <= self.max_pages:
            try:
                resp = requests.get(
                    f'{self.base_url}/users/{self.owner}/repos',
                    headers = self.headers,
                    params = {
                        'per_page' : self.per_page,
                        'page' : page
                    }
                )

                resp.raise_for_status()
                repos = resp.json()

                if repos == []:
                    break

                repos_data.extend(repos)

                for repo in repos:
                    if not repo['visibility'] == 'private' and not repo['archived'] == True and not repo['fork'] == True:
                        repo_names.append(repo['name'])

                page += 1
            
            except requests.exceptions.RequestException as e:
                self._log_issue(e)
                raise

            except Exception as e:
                self._log_issue(e)
                raise

        self._write_to_file('repos_raw', repos_data)

        return repo_names

    def fetch_issues(self, repo_names):
        all_issues = []
        
        for repo in repo_names:
            page = 1

            while page <= self.max_pages:
                try:
                    resp = requests.get(
                            f'{self.base_url}/repos/{self.owner}/{repo}/issues',
                            headers = self.headers,
                            params = {
                                'per_page' : self.per_page,
                                'page' : page
                            }
                        )
                    
                    resp.raise_for_status()
                    issues = resp.json()

                    if issues == []:
                        break

                    for issue in issues:
                        issue['repo_name'] = repo
                        all_issues.append(issue)

                    page += 1
                
                except requests.exceptions.RequestException as e:
                    self._log_issue(e)
                    raise

                except Exception as e:
                    self._log_issue(e)
                    raise
        
        self._write_to_file('issues_raw', all_issues)
        
    
    def fetch_branches(self, repo_names):
        all_branches = []

        for repo in repo_names:
            page = 1

            while page <= self.max_pages:
                try:
                    resp = requests.get(
                        f'{self.base_url}/repos/{self.owner}/{repo}/branches',
                        headers = self.headers,
                        params = {
                            'per_page' : self.per_page,
                            'page' : page
                        }
                    )

                    resp.raise_for_status()
                    branches = resp.json()

                    if branches == []:
                        break

                    for branch in branches:
                        branch['repo_name'] = repo
                        all_branches.append(branch)

                    page += 1

                except requests.exceptions.RequestException as e:
                    self._log_issue(e)
                    raise
                
                except Exception as e:
                    self._log_issue(e)
                    raise

        self._write_to_file('branches_raw', all_branches)
        
extractor = ExtractData()
repo_names = extractor.fetch_repos()
# extractor.fetch_issues(repo_names)
# extractor.fetch_branches(repo_names)
# print(len(repo_names))
