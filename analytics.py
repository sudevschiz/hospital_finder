import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

class Analytics():
    """
    All analytics related functionalities
    """
    KEY_FILE = "./credentials/service-account.json"
    AUTH_SCOPE = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    # TODO : Move to config
    SPREADSHEET_ID = "1IWjEQGUAQpQfT_wWVDiQqUoK457bE_MnTbpgnBPzTiE"
    SHEET_NAME = "Usage_Log"

    def __init__(self):
        # Initialize
        self.authenticate()
        self.get_sheet()
        
    
    def authenticate(self):
        """
        Oauth
        """
        # add credentials to the account
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.KEY_FILE, self.AUTH_SCOPE)

        # authorize the clientsheet 
        self.client = gspread.authorize(creds)

    def get_sheet(self):
        """
        Access the right sheet
        """
        # get the instance of the Spreadsheet
        sh = self.client.open_by_key(self.SPREADSHEET_ID)
        self.sheet = sh.worksheet(self.SHEET_NAME)

    def append_rows(self,rows):
        """
        Insert a row of data to the usage_log
        rows is a dataframe
        """
        r = self.sheet.append_rows(rows)
        logging.info(f"{r['updates']['updatedRows']} row(s) updated to usage logs!")

