import json
from os import path
from pprint import pprint, pformat
import sqlite3


DB_PATH = '../main_db.sqlite'

CREATE_STATES_TABLE = '''
CREATE TABLE IF NOT EXISTS states (
state_id INTEGER PRIMARY KEY AUTOINCREMENT,
short_name TEXT NOT NULL UNIQUE,
long_name TEXT NOT NULL UNIQUE
);
'''

CREATE_ZIP_TABLE = '''
CREATE TABLE IF NOT EXISTS zip_codes (
zip_id INTEGER PRIMARY KEY AUTOINCREMENT,
zip_code INTEGER UNIQUE NOT NULL,
city TEXT NOT NULL,
county TEXT NOT NULL,
state_id INTEGER NOT NULL,
FOREIGN KEY(state_id)
    REFERENCES states (state_id)
);
'''

CREATE_ADDRESS_TABLE = '''
CREATE TABLE IF NOT EXISTS addresses (
address_id INTEGER PRIMARY KEY AUTOINCREMENT,
address TEXT NOT NULL,
city TEXT NOT NULL,
state TEXT NOT NULL,
lat REAL NOT NULL,
lon REAL NOT NULL,
zip_code_id INTEGER NOT NULL,
FOREIGN KEY(zip_code_id)
    REFERENCES zip_codes (zip_id)
);
'''

def connect_db():
    try:
        return sqlite3.connect(DB_PATH)
    except Exception:
        print('Failed to connect to db at {}'.format(DB_PATH))
        return None


def run_query(connection, query):
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print('Failed to run query "{}"'.format(query))
        print(e)


def create_tables():
    conn = connect_db()

    run_query(conn, CREATE_STATES_TABLE)
    run_query(conn, CREATE_ZIP_TABLE)
    run_query(conn, CREATE_ADDRESS_TABLE)


def fix_cap(word):
    split = word.split(' ')
    split = [a[0] + a[1:].lower() for a in split]
    return ' '.join(split)


def insert_states():
    with open('test.json', 'r') as ctx:
        string = ''.join(ctx.readlines())
        string = string.replace(chr(39), chr(34))
        obj = json.loads(string)

    ignore_list = ('AS', 'DC', 'GU', 'MP', 'PR', 'VI')
    obj = [a for a in obj if a[1] not in ignore_list]
    states = [fix_cap(a[0]) for a in obj]
    state_short = [(obj[n][1], state) for n, state in enumerate(states)]

    query = 'INSERT INTO states (short_name, long_name) VALUES\n'
    for short, state in state_short:
        query += '("{}", "{}"),\n'.format(short, state)
    query = query[:-2] + ';'

    conn = connect_db()
    run_query(conn, query)


def insert_zips():
    conn = connect_db()
    cursor = conn.cursor()

    with open('mapping.json', 'r') as ctx:
        obj = json.load(ctx)

    states = list(obj.keys())

    lookup_id = 'SELECT "state_id" FROM states WHERE "long_name" = ?'
    state_ids = [cursor.execute(lookup_id, (state,)).fetchone()[0] for state in states]
    ss_mapping = {a: b for a, b in zip(states, state_ids)} # Disgusting

    insertion = 'INSERT INTO ZIP_CODES ("zip_code", "city", "county", "state_id") VALUES (?, ?, ?, ?)'
    data = list()

    zip_set = set()

    for state in obj:
        for zip_code in obj[state]:
            if int(zip_code[0]) in zip_set:
                continue
            zip_set.add(int(zip_code[0]))

            data.append([int(zip_code[0]),
                zip_code[1],
                zip_code[2],
                ss_mapping[state]])

    cursor.executemany(insertion, data)
    conn.commit()


def insert_addresses():
    conn = connect_db()
    cursor = conn.cursor()

    with open('../addresses_fl.txt', 'r') as ctx:
        next_line = ctx.readline()[:-1]
        while next_line != '':
            row = next_line.split(',')
            row = [a.strip() for a in row]
            zip_code = int(row[3])
            zcid_query = 'SELECT zip_id FROM zip_codes WHERE zip_code = ?'
            res = cursor.execute(zcid_query, (zip_code,))
            zcid = res.fetchone()[0]
            data = (row[0],
                    row[1],
                    row[2],
                    float(row[5]),
                    float(row[6]),
                    zcid)
            insertion = 'INSERT INTO addresses(address, city, state, lat, lon, zip_code_id) VALUES(?, ?, ?, ?, ?, ?)'
            cursor.execute(insertion, data)
            next_line = ctx.readline()[:-1]
        conn.commit()


