import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread

class GoogleSheetApi:
    def __init__(self) -> None:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        # JSON Key File Path
        json_key_path = os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))) + "/google_keys/key.json"

        credential = ServiceAccountCredentials.from_json_keyfile_name(json_key_path, scope)
        self.gc = gspread.authorize(credential)

        pass

    def get_doc(self, spreadsheet_url):
        doc = self.gc.open_by_url(spreadsheet_url)
        return doc
