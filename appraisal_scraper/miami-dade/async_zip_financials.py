import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
import json
from pprint import pformat, pprint
from queue import Queue
import sqlite3
import string
from threading import Thread

import requests


URL = 'https://miamidade.gov/Apps/PA/PApublicServiceProxy/PaServicesProxy.ashx'
DB_PATH = '../../main_db.sqlite'

folio_params = lambda: {'folioNumber': '',
        'clientAppName': 'PropertySearch',
        'Operation': 'GetPropertySearchByFolio',
        'endPoint': ''}

address_params = lambda: {'myAddress': '',
        'myUnit': '',
        'clientAppName': 'PropertySearch',
        'from': 0,
        'to': 1,
        'Operation': 'GetAddress',
        'endPoint': ''}

address_replacements = {
        'DRIVE': 'DR',
        'COURT': 'CT',
        'STREET': 'ST',
        'LANE': 'LN',
        'AVENUE': 'AVE',
        'TERRACE': 'TER',
        'EXTENSION': '',
        '441 AVENUE': '441',
        'NORTHWEST': 'NW',
        'NORTHEAST': 'NE',
        'SOUTHWEST': 'SW',
        'SOUTHEAST': 'SE',
        'NORTH': 'N',
        'SOUTH': 'S',
        'EAST': 'E',
        'WEST': 'W'}

INSERT_ASSESS = 'INSERT INTO assessments (year, building_value, land_value, extra_value, listed_area, prop_id) VALUES(?, ?, ?, ?, ?, ?)'

INSERT_SALES = 'INSERT INTO sales (price, date, prop_id) VALUES(?, ?, ?)'

INSERT_BUILDINGS = 'INSERT INTO buildings (building_num, year_constructed, building_area, prop_id) VALUES(?, ?, ?, ?)'


def folio_sample(zip_code, db_conn):
    cursor = db_conn.cursor()
    query = 'SELECT folio, prop_id FROM properties WHERE zip_code=?'
    res = cursor.execute(query, (zip_code,)).fetchall()
    return res


def get_property_info(folio):
    params = folio_params()
    params['folioNumber'] = folio.replace('-','')
    res = requests.get(URL, params=params, timeout=20).json()
    if not res['Completed']:
        raise Exception('Could not get info from folio: {}'.format(folio))
    return res


def build_assessment_rows(property_info):
    '''Assessed values in Assessment>AssessmentInfos[]>LandValue/BuildingOnlyValue/ExtraFeatureValue/Year
    Area: Land>Landlines[]>Units/AdjustedUnitPrice/UnitType/RollYear'''
    assessments = dict()

    if 'Assessment' not in property_info:
        return None

    for annual_assess in property_info['Assessment']['AssessmentInfos']:
        year = annual_assess['Year']
        land_value = annual_assess['LandValue']
        building_value = annual_assess['BuildingOnlyValue']
        extra_feat_value = annual_assess['ExtraFeatureValue']
        assessments[year] = [year, building_value, land_value, extra_feat_value, 0]

    if 'Land' not in property_info:
        return list(assessments.values())

    for annual_land in property_info['Land']['Landlines']:
        year = annual_land['RollYear']
        if year not in assessments:
            continue

        land_area = annual_land['Units']
        land_area_unit = annual_land['UnitType']
        depth = annual_land['Depth']

        if 'Front' in 'UnitType':
            land_area *= depth
        if 'Acre' in 'UnitType':
            land_area *= 43560

        assessments[year][-1] = land_area

    return list(assessments.values())


def build_sales_rows(property_info):
    '''Sales: SalesInfos[]>DateOfSale/SalePrice'''
    sales = list()

    for sale in property_info['SalesInfos']:
        date = sale['DateOfSale']
        price = sale['SalePrice']

        date_obj = datetime.strptime(date, '%m/%d/%Y')
        date = date_obj.strftime('%Y-%m-%d')
        sales.append([price, date])

    return sales


def build_building_rows(property_info):
    '''Building>BuildingInfos[]>BuildingNo/Effective/EffectiveArea'''
    buildings = list()

    if 'Building' not in property_info:
        return None

    seen_numbers = set()

    for structure in property_info['Building']['BuildingInfos']:
        building_number = structure['BuildingNo']
        if building_number in seen_numbers:
            continue

        seen_numbers.add(building_number)
        year_cons = structure['Effective']
        building_area = structure['EffectiveArea']

        buildings.append([building_number, year_cons, building_area])

    return buildings


def update_record(conn, q, folio_w_id):
    folio, prop_id = folio_w_id

    try:
        prop_info = get_property_info(folio)
    except:
        q.put(None)
        return None

    assess = [n + [prop_id] for n in build_assessment_rows(prop_info)]
    sales = [n + [prop_id] for n in build_sales_rows(prop_info)]
    buildings = [n + [prop_id] for n in build_building_rows(prop_info)]

    return_data = list()

    if assess:
        return_data.append((INSERT_ASSESS, assess))
    if sales:
        return_data.append((INSERT_SALES, sales))
    if buildings:
        return_data.append((INSERT_BUILDINGS, buildings))

    q.put(return_data)


def queue_worker(conn, q, num_props, stopped):
    counter = 0
    while not stopped():
        try:
            item = q.get(timeout=1)
        except:
            continue

        if not item:
            counter += 1
            q.task_done()
            continue

        for query, data in item:
            conn.cursor().executemany(query, data)

        counter += 1
        print('Progress: {}/{}'.format(counter, num_props))
        q.task_done()


def insert_property_financials(zip_code_scrape):
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    except:
        print('Failed to connect to db.')
        return

    # DELETE THIS BEFORE RUNNING FR FR
    folios = folio_sample(zip_code_scrape, conn)
    sql_queue = Queue()
    sql_stopped = False
    check_stopped = lambda: sql_stopped

    num_folios = len(folios)

    Thread(target=partial(queue_worker, conn, sql_queue, num_folios, check_stopped), daemon=False).start()

    # Using the 'with' block, code execution pauses until all tasks complete
    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(partial(update_record, conn, sql_queue), folios)

    sql_queue.join()
    sql_stopped = True

    conn.commit()
    conn.close()


def runner():
    parser = argparse.ArgumentParser(description='Scrapes property info from the Miami-Dade County Property Appraiser')
    parser.add_argument('zipcode', type=int)
    args = parser.parse_args()
    insert_property_financials(args.zipcode)


if __name__ == '__main__':
    runner()

