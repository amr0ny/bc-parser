from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from settings import settings, logger
from gspread.auth import authorize
import gspread

class ServiceAuth:
    def __init__(self, service_account_file, scopes, sheet_title, worksheet_name):
        self.account_file = service_account_file
        self.scopes = scopes
        self.sheet_title = sheet_title
        self.worksheet_name = worksheet_name

    def __authenticate_service_account(self):
        try:
            logger.info(f'[Authentication]: Started')
            logger.debug(f'[Authentication]: Service account file: {self.account_file}\tScopes: {self.scopes}')
            creds = Credentials.from_service_account_file(self.account_file, scopes=self.scopes)
            logger.debug(f'[Authentication]: Credentials accepted: {creds}')
            return creds
        except Exception as e:
            logger.error(f'[Authentication]: Failed: {e}')
            raise

    def get_google_sheet(self):
        try:
            # Authorize gspread with the credentials
            credentials = self.__authenticate_service_account()
            client = authorize(credentials=credentials)
            # Open the Google Sheet by its title
            logger.debug(f'[Opening sheet]: Sheet: {self.sheet_title}\t{self.worksheet_name}')
            sheet = client.open(self.sheet_title).worksheet(self.worksheet_name)

            return sheet
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f'[Opening sheet]: Not found')
            raise
        except Exception as e:
            logger.error('f[Opening sheet]: Failed: {e}')
            raise