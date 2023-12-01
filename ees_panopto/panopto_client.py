import time
from urllib.parse import urlparse

import requests
from requests.exceptions import RequestException

oauth_url = 'Panopto/oauth2/connect/token'
login_url = 'Panopto/api/v1/auth/legacyLogin'
session_url = 'Panopto/api/v1/sessions'

class Panopto:
    def __init__(self, config, logger):
        self.logger = logger
        self.retry_count = int(config.get_value("retry_count"))
        self.host = config.get_value("panopto.host_url")
        self.client_id = config.get_value("panopto.client_id")
        self.client_secret = config.get_value("panopto.client_secret")
        self.username = config.get_value("panopto.username")
        self.password = config.get_value("panopto.password")
        # self.secure_connection = config.get_value("panopto.secure_connection")
        # self.certificate_path = config.get_value("panopto.certificate_path")

        self.session = requests.Session()
        self.session.trust_env = False

        self.access_token = None
        self.expires_at = None

    def refresh_token(self):
        request_headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }
        url = f'{self.host}/{oauth_url}'
        response = self.session.post(
            url,
            headers=request_headers,
            data={
                'grant_type': 'password',
                'username': self.username,
                'password': self.password,
                'scope': 'api',
                'client_id': self.client_id,
                'client_secret': self.client_secret
            }
        )
        if response.ok:
            response_data = response.json()
            access_token = response_data.get('access_token')
            expires_in = response_data.get('expires_in')

            self.access_token = access_token
            self.expires_at = time.time() + expires_in

        if response.status_code >= 400 and response.status_code < 500:
            self.logger.exception(
                f"Error: {response.reason}. Error while refreshing token."
            )
    
    def is_token_expired(self):
        current_time = time.time()
        return current_time >= self.expires_at


    def dowload_video_by_session_id(self, session_id):
        response = self.get(f'{session_url}/{session_id}')

        if response.ok:
            response_data = response.json()
            
            download_url = response_data['Urls']['DownloadUrl']
            response = self.get(
                download_url,
            )

            if response.ok:
                parsed_download_url = urlparse(download_url)._replace(query=None).geturl()
                filename = parsed_download_url.rsplit("/", 1)[1]
                self.logger.info(f"Successfully downloaded {session_id} video from {parsed_download_url} as {filename}.")
                with open(f'./videos/{filename}', "wb") as f:
                    f.write(response.content)
            
            if response.status_code >= 400 and response.status_code < 500:
                self.logger.exception(f"Error while downloading {session_id} video from {download_url}.")
        if response.status_code >= 400 and response.status_code < 500:
            self.logger.exception(f"Error while downloading {session_id} video.")


    def login(self):
        url = f'{self.host}/{login_url}'
        response = self.session.get(
            url,
            headers={
                'Authorization': f'Bearer {self.access_token}'
            },
        )
        # if response.ok:
        #     self.logger.info(f"Successfully login with {url}.")

        if response.status_code >= 400 and response.status_code < 500:
            self.logger.exception(
                f"Error: {response.reason}. Error while logging in the panopto."
            )

    def get(self, rel_url, request_headers=None):
        retry = 0
        while retry <= self.retry_count:
            try:
                if not self.access_token or self.is_token_expired():
                    # Refresh the token
                    self.refresh_token()

                self.login()

                if not request_headers:
                    request_headers = {
                        'Authorization': f'Bearer {self.access_token}'
                    }
                else:
                    request_headers['Authorization'] = f'Bearer {self.access_token}'

                # Check if the url contains the host part
                parsed_url = urlparse(rel_url)
                if not parsed_url.netloc:
                    # Add the host part to the url
                    url = f"{self.host}/{rel_url}"
                else:
                    url = rel_url

                response = self.session.get(
                    url,
                    headers=request_headers,
                )
                if response.ok:
                    return response
                if response.status_code >= 400 and response.status_code < 500:
                    self.logger.exception(
                        f"Error: {response.reason}. Error while fetching from the panopto, url: {url}."
                    )
                    return response
                
                # This condition is to avoid sleeping for the last time
                if retry < self.retry_count:
                    time.sleep(2 ** retry)
                retry += 1
            except RequestException as exception:
                if retry < self.retry_count:
                    time.sleep(2 ** retry)
                else:
                    return False
                retry += 1

        if retry > self.retry_count:
            return response
        return response