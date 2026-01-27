import requests
import os
import datetime
from dotenv import load_dotenv
from config import ISSUES_DIR

class GithubAuth:
    def __init__(self):
        load_dotenv('pipeline.env')
        self.pat = os.getenv('GITHUB_TOKEN')
        self.auth_url = 'https://api.github.com/user'
        self.headers = {
            'Authorization' : f'Bearer {self.pat}',
            'Accept' : 'application/vnd.github+json',
            'User-Agent': 'github-issues-pipeline'
        }

    def _log_issue(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(os.path.join(ISSUES_DIR, 'auth_error_log.txt'), 'a', encoding = 'UTF-8') as issue_log:
            issue_log.write(f'{timestamp}: {message}\n')

    def validate_token(self):
        if not self.pat:
            self._log_issue('Github token not found in environment variables.')
            raise Exception('Github token not found in environment variables.')

        resp = requests.get(self.auth_url, headers = self.headers)

        if resp.status_code == 401:
            self._log_issue('Github token invalid or expired.')
            raise Exception('Github token invalid or expired.')
        
        if not resp.ok:
            self._log_issue(f'Authentication failed: {resp.status_code}.')
            raise Exception(f'Authentication failed: {resp.status_code}.')

        return True
    
# authoriser = GithubAuth()
# authoriser.validate_token()





