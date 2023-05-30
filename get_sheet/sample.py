################################
#
#  cp sample.py name.py
#
################################

import os
from dotenv import load_dotenv
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from _lib.google_sheet import GoogleSheetApi
from _lib.mysql_connector import MysqlConnector

#load .env
startDirectory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
load_dotenv(startDirectory+"/.env")
# mysql Connector
mysql_connector = MysqlConnector()
# google Sheet Connector
gs_api = GoogleSheetApi()


## Variables
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1_0Z7S2NlIXZPFeG3mEZEoz4LOjVNaZEIuQ82GX6V3OY/edit#gid=0'
sheet_name = '시트1';
table_name = 'table_samples';


doc = gs_api.get_doc(spreadsheet_url)
worksheet = doc.worksheet(sheet_name)

if __name__ == "__main__":
    # example source
    #cell_data = worksheet.acell('A1').value
    #col_data = worksheet.col_values(1)
    #row_data = worksheet.row_values(1)

    # all rows
    rows = worksheet.get_all_values()
    rows_count = len(rows)
    data_results = []

    for i, row in enumerate(rows):
        if i == 0:
            continue
        data_results.append(row)

    # 데이터 넣기
    mysql_connector.upsertTableData(table_name=table_name, data_results=data_results)
