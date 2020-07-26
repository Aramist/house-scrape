import sqlite3


def search_city_state(main_db_conn, zip_code):
    SEARCH = 'select zip_id, city, short_name from zip_codes a inner join states b on a.state_id=b.state_id where zip_code=?'

    cursor = main_db_conn.cursor()
    res = cursor.execute(SEARCH, (zip_code,)).fetchone()

    return res


def insert_coord_tree(main_db_conn, lat, lon, index_id):
    INSERT = 'insert into coord_index (id, minLat, maxLat, minLon, maxLon) values(?, ?, ?, ?, ?);'
    data = (index_id, lat, lat + 1e-7, lon, lon + 1e-7)

    main_db_conn.cursor().execute(INSERT, data)


def run():
    main_db = sqlite3.connect('main_db.sqlite')
    merge_db = sqlite3.connect('temp_addresses.sqlite')

    QUERYALL = 'select address, zipcode, lat, lon from addy_record'
    DUPECHECK = 'select * from addresses where address=? limit 1'
    TREEINDEX = 'select id from coord_index order by id desc limit 1'
    INSERTADDY = 'insert into addresses (address, city, state, lat, lon, zip_code_id, coord_tree_index) values(?, ?, ?, ?, ?, ?, ?)'

    duplicates = 0
    index = 0

    max_index = 316461 # select count(*) from addy_record

    coord_tree_index = main_db.cursor().execute(TREEINDEX).fetchone()[0]

    for addy, zipc, lat, lon in merge_db.cursor().execute(QUERYALL):
        index += 1

        res = main_db.cursor().execute(DUPECHECK, (addy,)).fetchone()

        if res: # Res is either None or a non-empty list
            duplicates += 1
            print('{}/{}, {:.3f}'.format(duplicates, index, duplicates/index))
            continue

        coord_tree_index += 1
        insert_coord_tree(main_db, lat, lon, coord_tree_index)

        search_cs = search_city_state(main_db, zipc)
        if not search_cs:
            duplicates += 1
            print('{}/{}, {:.3f}'.format(duplicates, index, duplicates/index))
            continue

        zip_id, city, state = search_cs

        data = (addy, city, state, lat, lon, zip_id, coord_tree_index)
        main_db.cursor().execute(INSERTADDY, data)

    main_db.commit()


run()
