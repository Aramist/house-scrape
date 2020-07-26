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

INSERT_ASSESS = 'INSERT INTO assessments (year, land_value, building_value, extra_feature_value, property) VALUES(?, ?, ?, ?, ?)'

INSERT_LAND = 'INSERT INTO land_parcels (year, land_area, land_area_unit, adjusted_unit_price, property) VALUES(?, ?, ?, ?, ?)'

INSERT_SALES = 'INSERT INTO sales (price, date, property) VALUES(?, ?, ?)'

INSERT_BUILDINGS = 'INSERT INTO buildings (building_number, year_constructed, building_area, property) VALUES(?, ?, ?, ?)'


def process_addy(addy):
    addy = addy.upper()
    addy.replace('  ', ' ')
    split = addy.split(' ')
    number = split[0]
    split = split[1:]
    digit_set = set(string.digits)
    # Finds the street name, given it contains a number
    for n, text in enumerate(split):
        if set(text).intersection(digit_set):
            split[n] = ''.join(a for a in split[n] if a in digit_set)
    split.insert(0, number)
    preliminary = ' '.join(split)
    for orig, repl in address_replacements.items():
        preliminary = preliminary.replace(orig, repl)
    return preliminary


def address_sample(zip_code, db_conn):
    cursor = db_conn.cursor()
    query = 'SELECT address, address_id FROM addresses INNER JOIN zip_codes ON addresses.zip_code_id=zip_codes.zip_id WHERE zip_codes.zip_code=?'
    res = cursor.execute(query, (zip_code,)).fetchall()
    return res


def get_folio(addy):
    params = address_params()
    params['myAddress'] = process_addy(addy)
    res = requests.get(URL, params=params, timeout=15).json()
    if res['Completed']:
        index = -1
        for n, entry in enumerate(res['MinimumPropertyInfos']):
            house_num = int(entry['SiteAddress'].split(' ')[0])
            given_num = int(addy.split(' ')[0])
            if house_num == given_num:
                index = n
                break

        if index == -1:
            raise Exception('Could not get folio: {}'.format(addy))

        return res['MinimumPropertyInfos'][index]['Strap']
    else:
        raise Exception('Could not get folio: {}'.format(addy))

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
    assessments = list()

    if 'Assessment' not in property_info:
        return None

    for annual_assess in property_info['Assessment']['AssessmentInfos']:
        year = annual_assess['Year']
        land_value = annual_assess['LandValue']
        building_value = annual_assess['BuildingOnlyValue']
        extra_feat_value = annual_assess['ExtraFeatureValue']
        assessments.append([year, land_value, building_value, extra_feat_value])

    return assessments


def build_land_rows(property_info):
    parcels = list()

    # The schema is designed as such to cover properties for which multiple lots are joined, but recorded separately in the db

    if 'Land' not in property_info:
        return None

    for annual_land in property_info['Land']['Landlines']:
        year = annual_land['RollYear']
        land_area = annual_land['Units']
        land_area_unit = annual_land['UnitType']
        adjusted_price = annual_land['AdjustedUnitPrice']
        '''
        if 'Front' in land_area_unit:
            print('Front foot: ')
            continue
        '''
        if 'Ft.' in land_area_unit and land_area < 10:
            continue
        parcels.append([year, land_area, land_area_unit, adjusted_price])

    return parcels


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


def record_address(conn, q, addys):
    addy, addy_id = addys

    try:
        prop_info = get_property_info(get_folio(addy))
    except:
        q.put(None)
        return None

    residential = 'RESIDENTIAL' in prop_info['PropertyInfo']['DORDescription']

    if not residential:
        print('Not residential: {}'.format(addy))
        q.put(None)
        return None

    assess = [n + [addy_id] for n in build_assessment_rows(prop_info)]
    sales = [n + [addy_id] for n in build_sales_rows(prop_info)]
    land_units = [n + [addy_id] for n in build_land_rows(prop_info)]
    buildings = [n + [addy_id] for n in build_building_rows(prop_info)]

    return_data = list()

    if assess:
        return_data.append((INSERT_ASSESS, assess))
    if sales:
        return_data.append((INSERT_SALES, sales))
    if land_units:
        return_data.append((INSERT_LAND, land_units))
    if buildings:
        return_data.append((INSERT_BUILDINGS, buildings))

    q.put(return_data)


def queue_worker(conn, q, num_addys, stopped):
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
        print('Progress: {}/{}'.format(counter, num_addys))
        q.task_done()


def insert_address_financials(zip_code_scrape):
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    except:
        print('Failed to connect to db.')
        return

    # DELETE THIS BEFORE RUNNING FR FR
    addys = address_sample(zip_code_scrape, conn)
    sql_queue = Queue()
    sql_stopped = False
    check_stopped = lambda: sql_stopped

    num_addys = len(addys)

    Thread(target=partial(queue_worker, conn, sql_queue, num_addys, check_stopped), daemon=False).start()

    # Using the 'with' block, code execution pauses until all tasks complete
    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(partial(record_address, conn, sql_queue), addys)

    sql_queue.join()
    sql_stopped = True

    conn.commit()
    conn.close()


def runner():
    parser = argparse.ArgumentParser(description='Scrapes property info from the Miami-Dade County Property Appraiser')
    parser.add_argument('zipcode', type=int)
    args = parser.parse_args()
    insert_address_financials(args.zipcode)


if __name__ == '__main__':
    runner()

