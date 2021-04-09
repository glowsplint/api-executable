'''
# BigSchedules Production API Caller
This script will call the BigSchedules API using our production credentials. Ensure that the number of seconds per iterations do not exceed 1 per second.
For the production environment, the API call limit is maximum 2,000 calls per day and 150 calls per minute, with a maximum of 30,000 calls per month.
The script requires an input file 'BigSchedules Port Pairs - *.csv'.

--------
Features
--------
1. If calls have already been done today (i.e. the json files exist in the <today_path> directory), it will not rerun those calls.
2. Will raise an exception if PulseSecure is on
'''

import pkg_resources.py2_warn
import requests
import json
import os
import pandas as pd
import numpy as np
import joblib

from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
from secrets import PRODUCTION_KEY

'''
Complete all API calls and save the responses by date and batch into respective folders.
'''

# Check if directory already exists - if exist then calculate outstanding, if not use df
today_path = Path('responses/' + datetime.now().strftime('%Y-%m-%d'))
port_pairs_files = list(Path('.').glob('BigSchedules Port Pairs - *.csv'))
if len(port_pairs_files) != 1:
    print('Exception: There is more than one file matching the pattern "BigSchedules Port Pairs - *.csv". Please ensure that there is only one file matching this pattern.')
    input()
    raise Exception(
        'There is more than one file matching the pattern "BigSchedules Port Pairs - *.csv". Please ensure that there is only one file matching this pattern.')
try:
    df = pd.read_csv(port_pairs_files[0])
    os.makedirs(today_path)
except FileExistsError:
    path = Path(os.getcwd()) / today_path / 'batch one'
    completed_df = pd.DataFrame([item.name for item in sorted(
        path.glob('*.json'))]).rename({0: 'pol_pod'}, axis=1)
    df['pol_pod'] = df.port_of_loading + '-' + df.port_of_discharge

    if len(completed_df):
        completed_df['pol_pod'] = completed_df.pol_pod.str[:11]
        print(f'Detected {len(completed_df)} existing API calls today.')
        outstanding = set(df.pol_pod) - set(completed_df.pol_pod)
    else:
        outstanding = set(df.pol_pod)

    if len(outstanding) == 0:
        df = pd.DataFrame({'pol_pod': []})
    else:
        df = pd.DataFrame(outstanding)[0].str.split(
            '-', expand=True).rename({0: 'port_of_loading', 1: 'port_of_discharge'}, axis=1)

os.chdir(today_path)

batch_paths = ['batch one', 'batch two']
for batch in batch_paths:
    try:
        os.makedirs(batch)
    except FileExistsError:
        pass


url = 'https://apis.cargosmart.com/openapi/schedules/routeschedules'
weeks = 6


def set_params(df, index, departureFrom):
    credentials = {
        'appKey': PRODUCTION_KEY,
        'departureFrom': departureFrom,
        'searchDuration': weeks
    }

    parameters = {
        'porID': df.iloc[index]['port_of_loading'],
        'fndID': df.iloc[index]['port_of_discharge']
    }

    credentials.update(parameters)
    return credentials


def write_json(response, output_file):
    with open(output_file, 'w') as w:
        json.dump(response, w, indent=2)


print('Running first batch of API calls...')
os.chdir(batch_paths[0])
for i in tqdm(range(len(df))):
    departureFrom = datetime.now().strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    try:
        response = requests.get(url, params=set_params(df, i, departureFrom))
        write_json(response.json(),
                   f'{set_params(df, i, departureFrom)["porID"]}-{set_params(df, i, departureFrom)["fndID"]}.json')
    except requests.exceptions.SSLError:
        raise Exception(
            'You are connected on PulseSecure! You will need to turn off PulseSecure to run this script.')
    except Exception as e:
        pass

print('Running second batch of API calls...')
os.chdir('../' + batch_paths[1])
for i in tqdm(range(len(df))):
    departureFrom = (datetime.now() + timedelta(weeks=weeks)
                     ).strftime('%Y-%m-%d') + 'T00:00:00.000Z'
    try:
        response = requests.get(url, params=set_params(df, i, departureFrom))
        write_json(response.json(),
                   f'{set_params(df, i, departureFrom)["porID"]}-{set_params(df, i, departureFrom)["fndID"]}.json')
    except Exception as e:
        pass


'''
Assemble the file from the saved API responses.
'''


class Hasher(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


def replaceHasherWithBlank(variable, blank):
    if isinstance(variable, Hasher):
        return blank
    return variable


def find_cy(data, i, j, total_legs, port_of_loading, blank):
    '''
    Find the first leg where transportMode == "VESSEL", take the defaultCutoff in the same leg,
        checking that the fromPoint of that leg is the POL.
    '''
    for k in range(total_legs):
        if data['routeGroupsList'][i]['route'][j]['leg'][k]['transportMode'] == 'VESSEL':
            if port_of_loading == data['routeGroupsList'][i]['route'][j]['leg'][k]['fromPoint']['location']['unlocode']:
                return Hasher(data['routeGroupsList'][i]['route'][j]['leg'][k]['fromPoint'])['defaultCutoff']
    return blank


def find_routing(data, i, j, total_legs, port_of_discharge, blank):
    '''
    Find the first leg where transportMode == "VESSEL", take the toPoint in the same leg,
        checking that the toPoint of that leg is not the POD.
    '''
    for k in range(total_legs):
        if data['routeGroupsList'][i]['route'][j]['leg'][k]['transportMode'] == 'VESSEL':
            routing = data['routeGroupsList'][i]['route'][j]['leg'][k]['toPoint']['location']['unlocode']
            if port_of_discharge != routing:
                return routing
    return blank


def find_vsv(data, i, j, total_legs, blank):
    '''
    Find the first leg where transportMode == "VESSEL",
        take the vessel name in the same leg,
        take the service name in the same leg,
        take the externalVoyageNumber.
    '''
    for k in range(total_legs):
        if data['routeGroupsList'][i]['route'][j]['leg'][k]['transportMode'] == 'VESSEL':
            vessel = Hasher(Hasher(data['routeGroupsList'][i]['route'][j]['leg'][k])[
                            'vessel'])['name']
            service = Hasher(Hasher(data['routeGroupsList'][i]['route'][j]['leg'][k])[
                             'service'])['name']
            voyage = Hasher(data['routeGroupsList'][i]['route'][j]['leg'][k])[
                'externalVoyageNumber']
            return vessel, service, voyage
    return blank, blank, blank


def get_relevant_fields(data, i, j):
    '''Every route should be a single row in the spreadsheet'''
    blank = np.nan

    # Fields that don't crash the script
    port_of_loading = data['routeGroupsList'][i]['por']['location']['unlocode']
    port_of_discharge = data['routeGroupsList'][i]['fnd']['location']['unlocode']
    departure_date = data['routeGroupsList'][i]['route'][j]['por']['etd']
    arrival_date = data['routeGroupsList'][i]['route'][j]['fnd']['eta']
    transit = data['routeGroupsList'][i]['route'][j]['transitTime']
    carrier = data['routeGroupsList'][i]['carrier']['name']
    update_date = data['routeGroupsList'][i]['route'][j]['touchTime']
    # created_date = pd.Timestamp.today()

    total_legs = len(data['routeGroupsList'][i]['route'][j]['leg'])
    cy_cutoff_date = find_cy(data, i, j, total_legs, port_of_loading, blank)
    routing = find_routing(data, i, j, total_legs, port_of_discharge, blank)

    '''
    Loading terminal is currently:
    loading_terminal = Hasher(
        data['routeGroupsList'][i]['route'][j]['por']['location'])['facility']['name']
    Loading terminal in a future release could be:
        Within the route, take the first leg, get the fromPoint.location.facility.name
    '''
    loading_terminal = Hasher(
        data['routeGroupsList'][i]['route'][j]['por']['location'])['facility']['name']

    '''
    Discharge terminal is currently:
    discharge_terminal = Hasher(
        data['routeGroupsList'][i]['route'][j]['fnd']['location'])['facility']['name']
    Discharge terminal in a future release could be:
        Within the route, take the last leg, get the toPoint.location.facility.name
    '''
    discharge_terminal = Hasher(
        data['routeGroupsList'][i]['route'][j]['fnd']['location'])['facility']['name']

    cy_cutoff_date = replaceHasherWithBlank(cy_cutoff_date, blank)
    loading_terminal = replaceHasherWithBlank(loading_terminal, blank)
    discharge_terminal = replaceHasherWithBlank(discharge_terminal, blank)

    vessel, service, voyage = find_vsv(data, i, j, total_legs, blank)
    vessel = replaceHasherWithBlank(vessel, blank)
    service = replaceHasherWithBlank(service, blank)
    voyage = replaceHasherWithBlank(voyage, blank)

    return {
        "port_of_loading": port_of_loading,
        "port_of_discharge": port_of_discharge,
        "cy_cutoff_date": cy_cutoff_date,
        "departure_date": departure_date,
        "arrival_date": arrival_date,
        "transit": transit,
        "service": service,
        "vessel": vessel,
        "voyage": voyage,
        "carrier": carrier,
        "routing": routing,
        "loading_terminal": loading_terminal,
        "discharge_terminal": discharge_terminal,
        "update_date": update_date,
        # "created_date": created_date,
        "source": "BigSchedules API"
    }


def extract_data(data):
    if len(data['routeGroupsList']):
        df = pd.DataFrame(([get_relevant_fields(data, i, j)
                            for i in range(len(data['routeGroupsList']))
                            for j in range(len(data['routeGroupsList'][i]['route']))]))
        df.sort_values('departure_date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    else:
        return


def create_data_list(current_path):
    data_list = []
    for _, file in enumerate(sorted(current_path.glob('*.json'))):
        with open(file, 'r') as jsonfile:
            data = json.load(jsonfile)
        data_list.append(data)
    return data_list


def create_df(data_list):
    df = pd.concat([extract_data(Hasher(data))
                    for data in data_list], ignore_index=True)
    df.cy_cutoff_date = pd.to_datetime(
        df.cy_cutoff_date, utc=True).dt.strftime('%d/%m/%Y')
    df.departure_date = pd.to_datetime(
        df.departure_date).dt.strftime('%d/%m/%Y')
    df.arrival_date = pd.to_datetime(df.arrival_date).dt.strftime('%d/%m/%Y')
    df.update_date = pd.to_datetime(df.update_date).dt.strftime('%d/%m/%Y')
    # df.created_date = pd.to_datetime(df.created_date).dt.strftime('%d/%m/%Y')
    return df


list_of_dfs = []
for batch in batch_paths:
    os.chdir('../' + batch)
    path = Path(os.getcwd())
    data_list = create_data_list(path)
    list_of_dfs.append(create_df(data_list))

full_df = pd.concat(list_of_dfs).reset_index(drop=True)

# # Replace Unicode characters
# full_df.carrier = full_df.carrier.str.replace('Ü', 'U')
# full_df.carrier = full_df.carrier.str.replace('Ç', 'C')

# # Prepare APAC version with US port pairs removed
# apac_df = full_df.loc[full_df.port_of_loading.str[:2]
#                       != 'US'].reset_index(drop=True).copy()

# Determine number of failed (timeout) API Calls


def isAPICallError(path_to_file, error_msg):
    with open(path_to_file) as jsonfile:
        data = json.load(jsonfile)
        try:
            if data['message'][:len(error_msg)] == error_msg:
                return path_to_file.name
        except KeyError:
            pass


files_in_folder = sorted(path.glob('*.json'))
timeout_list = [isAPICallError(item, "Call ssm2014 timeout")
                for item in files_in_folder if isAPICallError(item, "Call ssm2014 timeout") is not None]
timeout_list = timeout_list + [isAPICallError(item, "Call ssm2014 Request failed with status code 500")
                               for item in files_in_folder if isAPICallError(item, "Call ssm2014 Request failed with status code 500") is not None]
print(f'There are {len(timeout_list)} timeout errors in this run.')

os.chdir('../../../')
full_df_name = f'schedules_{len(full_df)} - {datetime.now().strftime("%d.%m.%Y")}.csv'
full_df.to_csv(f'{full_df_name}', index=False)
print(f'{full_df_name} has been created in the current directory.')

# apac_df_name = f'schedules_{len(apac_df)} - {datetime.now().strftime("%d.%m.%Y")} (APAC).csv'
# apac_df.to_csv(f'{apac_df_name}', index=False)
# print(f'{apac_df_name} has been created in the current directory.')

# log_df_name = f'error_list - {datetime.now().strftime("%d.%m.%Y")}.csv'
# log_df.to_csv(f'{log_df_name}', index=False)
# print(f'{log_df_name} has been created in the current directory.')

print('The API Executable has finished running. Press any key to exit.')
input()
