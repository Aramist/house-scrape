import sqlite3


def insert_coord_tree(main_db_conn, index_id, lat, lon):
    INSERT = 'insert into coord_index (id, minLat, maxLat, minLon, maxLon) values(?, ?, ?, ?, ?);'
    data = (index_id, lat, lat + 1e-7, lon, lon + 1e-7)

    main_db_conn.cursor().execute(INSERT, data)


def fill_coord_tree(main_db_conn):
    cur = main_db_conn.cursor()

    GET_COORDS = 'select prop_id, lat, lon from properties'

    props = cur.execute(GET_COORDS).fetchall()

    for prop in props:
        insert_coord_tree(main_db_conn, *prop)
    main_db_conn.commit()


def run():
    main_db = sqlite3.connect('main_db.sqlite')
    fill_coord_tree(main_db)

run()
