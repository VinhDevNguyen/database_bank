from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from json import loads, dumps
import numpy as np

import json
import pandas as pd


def read_sheet(sheet_id, sheet_range, creds):
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    try:
        result = sheet.values().get(spreadsheetId=sheet_id,
                                        range=sheet_range).execute()
        result = result.get('values', [])
        column = result[0]
        values = result[1:]
        df = pd.DataFrame(values, columns=column)
        return df
    except HttpError as err:
        print(json.loads(err.content)['error'])

def get_config(sheet_config_id:str, range_config:str, cred:str):
    """Get config from sheet

    Args:
        sheet_config_id (str): Sheet id
        range_config (str): Range of config sheet
        cred (str): Credential of google sheet

    Returns:
        pandas.dataframe: Dataframe of config
    """
    config_df = read_sheet(sheet_config_id, range_config, cred)
    return config_df

def get_valid_config(config_df):
    """Only get config that is not copy

    Args:
        config_df (df): config_df

    Returns:
        tuple: Tuple that contain source and dest
    """
    valid_config = config_df[config_df['is_copy'] == '1']
    valid_config = valid_config.drop('is_copy', axis=1)
    config_json = valid_config.to_json(orient="records")
    config_json = json.loads(config_json)
    sourceID = []
    destID = []
    source = []
    dest = []
    report_daily = []
    for config in config_json:
        source.append(f"{config['Name']}!{config['Range']}")
        # dest.append(f"{config['Name_dest']}!{config['Range_dest']}")
        sourceID.append(config['Sheet ID'])
        # destID.append(config['Sheet ID_dest'])
        # report_daily.append(f"{config['Name_report']}!{config['Range_report']}")
    return source, sourceID

def write_df(value, sheet_id, sheet_range, creds):
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    value = pd.DataFrame(value).astype(str).values.tolist()
    
    body = {
        'values': value
    }
    try:
        result = sheet.values().update(
            spreadsheetId=sheet_id, range=sheet_range,
            valueInputOption='RAW', body=body).execute()
        print('{0} cells updated.'.format(result.get('updatedCells')))
    except HttpError as err:
        print(json.loads(err.content)['error'])

def get_source_data(sheet_source_id: str, range_source: str, creds: str):
    source_df = read_sheet(sheet_source_id, range_source, creds)

    source_df_keep = ['Ngày', 'Sever', 'Người bán', 'Số lượng', 'Số tiền', 'Loại tiền', 'Ví tiền']
    source_df = source_df.drop(source_df.columns.difference(source_df_keep), axis=1)
    source_df.replace(r'^\s*$', None, inplace=True)
    source_df.replace('', None, inplace=True)
    source_df = source_df.dropna(thresh=source_df.shape[1] - 4)
    name_sheet = range_source.split("!")[0]
    source_df = source_df.assign(**{"name sheet": name_sheet})

    source_list = source_df.values.tolist()
    header = source_df.columns.tolist()
    source_list.insert(0, header)
    result = source_df.copy()
    return result

    # write_df(source_list, sheet_dest_id, range_dest, creds)
    
    # source_df['Ngày'] = pd.to_datetime(source_df['Ngày'], format='%d/%m/%Y %H:%M:%S')
    # source_df['Ngày'] = source_df['Ngày'].dt.strftime('%d/%m/%Y')
    # source_df['Số lượng'] = source_df['Số lượng'].astype(float)
    # source_df['Số tiền'] = source_df['Số tiền'].astype(float)
    # grouped_df = source_df.groupby(['Ngày', 'Ví tiền', 'Loại tiền'], as_index=False)['Số tiền'].sum()
    # grouped_df.replace(['', ' '], None, inplace=True)
    # grouped_list = grouped_df.values.tolist()
    # header_group = ['Ngày', 'Ví tiền', 'Loại tiền','Tổng']
    # grouped_list.insert(0, header_group)
    # write_df(grouped_list, sheet_dest_id, range_report, creds)


