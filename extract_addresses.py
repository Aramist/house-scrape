import sqlite3

from lxml import etree


def mean_helper(iterable, fn):
    accum = 0
    count = 0
    for a in iterable:
        accum += fn(a)
        count += 1
    return accum / count


def record_way(address_node, node_db, storage_db):
    node_cursor = node_db.cursor()
    storage_cursor = storage_db.cursor()

    addy = {'housenumber': '',
            'zipcode': '',
            'street': ''}

    INSERT = 'INSERT INTO addy_record (address, zipcode, lat, lon) VALUES (?, ?, ?, ?)'
    LOOKUP_NODE = 'SELECT lat, lon FROM node_map WHERE node_id IN {}'

    way_nodes = list()

    for child in address_node:
        if child.tag == 'nd':
            way_nodes.append(int(child.attrib['ref']))
            continue

        if child.tag != 'tag':
            continue

        attr = child.attrib

        if attr['k'] == 'addr:housenumber':
            addy['housenumber'] = attr['v']
        elif attr['k'] == 'addr:postcode':
            addy['zipcode'] = attr['v'][:5]
        elif attr['k'] == 'addr:street':
            addy['street'] = attr['v']

    # Free RAM
    address_node.clear()

    # Reject those that lack necessary info
    if any([addy['zipcode'] == '',
        addy['housenumber'] == '',
        addy['street'] == '']):
        print('Missing info')
        return

    # Find the mean lat and lon
    way_nodes = '({})'.format( ', '.join(str(a) for a in way_nodes) )
    req = node_cursor.execute(LOOKUP_NODE.format(way_nodes)).fetchall()

    if not req:
        print('No matching nodes')
        return

    cm_lat = mean_helper(req, lambda x: x[0])
    cm_lon = mean_helper(req, lambda x: x[1])

    address = '{housenumber} {street}'.format(**addy)
    data = (address, int(addy['zipcode']), cm_lat, cm_lon)


    storage_cursor.execute(INSERT, data)


def record_addy(address_node, output_ctx):
    addy = {'city': '',
            'housenumber': '',
            'zipcode': '',
            'state': '',
            'street': '',
            'lat': '',
            'lon': ''}

    if 'lat' in address_node.attrib:
        addy['lat'] = address_node.attrib['lat']

    if 'lon' in address_node.attrib:
        addy['lon'] = address_node.attrib['lon']

    for child in address_node:
        if child.tag != 'tag':
            continue
        attr = child.attrib
        if attr['k'] == 'addr:city':
            addy['city'] = attr['v']
        elif attr['k'] == 'addr:housenumber':
            addy['housenumber'] = attr['v']
        elif attr['k'] == 'addr:postcode':
            addy['zipcode'] = attr['v']
        elif attr['k'] == 'addr:state':
            addy['state'] = attr['v']
        elif attr['k'] == 'addr:street':
            addy['street'] = attr['v']

    # Free RAM
    address_node.clear()

    # Reject those that lack a zip code
    if addy['zipcode'] == '':
        return

    form = '{housenumber} {street}, {city}, {state}, {zipcode}, {lat}, {lon}\n'
    output_ctx.write(form.format(**addy))


def save_nodes(osm_filename, db_path):
    conn = sqlite3.connect('node_map.sqlite')
    cursor = conn.cursor()

    INSERT = 'insert into node_map values (?, ?, ?)'

    ctx = etree.iterparse(osm_filename, events=('end',), tag='node')

    index = 1
    nodes = 0

    for _, event_tag in ctx:
        if index % 10000 == 0:
            print('Recording node {}'.format(index))
            conn.commit()
        index += 1

        if len(event_tag) != 0:
            event_tag.clear()
            continue

        attrib = event_tag.attrib

        if any(['id' not in attrib, 'lat' not in attrib, 'lon' not in attrib]):
            event_tag.clear()
            continue

        data = (attrib['id'], attrib['lat'], attrib['lon'])
        event_tag.clear()

        cursor.execute(INSERT, data)
    conn.commit()



def extract_ways(osm_filename, node_db, storage_db):
    ctx = etree.iterparse(osm_filename, events=('end',), tag=('node', 'way'))

    node_conn = sqlite3.connect(node_db)
    storage_conn = sqlite3.connect(storage_db)

    index = 1
    addresses = 0

    for _, event_tag in ctx:
        if event_tag.tag != 'way':
            event_tag.clear()
            continue

        if len(event_tag) == 0:
            event_tag.clear()
            continue

        if index % 5000 == 0:
            print('Reading way {}'.format(index))
        index += 1

        # Ensure the building attrib exists within the children
        if not any(('k' in child.attrib and child.attrib['k'] == 'building' and child.attrib['v'] == 'yes' for child in event_tag)):
            event_tag.clear()
            continue

        for child in event_tag:
            if child.tag != 'tag':
                continue
            if 'k' not in child.attrib:
                continue
            if child.attrib['k'] == 'addr:housenumber':
                record_way(event_tag, node_conn, storage_conn)
                addresses += 1
                if addresses % 10000 == 0:
                    print("Added address #{}".format(addresses))
                break
        else: # This case is very important
            event_tag.clear()
    storage_conn.commit()


def extract_addy(osm_filename, output_filename):
    ctx = etree.iterparse(osm_filename, events=('end',), tag='node')
    with open(output_filename, 'w') as out:
        index = 1
        addresses = 0

        for _, event_tag in ctx:
            if index % 5000 == 0:
                print('Reading node {}'.format(index))
            index += 1

            if len(event_tag) == 0:
                event_tag.clear()
                continue

            for child in event_tag:
                if child.tag != 'tag':
                    continue
                if 'k' not in child.attrib:
                    continue
                if child.attrib['k'] == 'addr:housenumber':
                    record_addy(event_tag, out)
                    print('Added Address #{}'.format(addresses))
                    addresses += 1
                    break
            else: # This case is very important
                event_tag.clear()


extract_ways('osm_cache/states/florida.osm', 'node_map.sqlite', 'temp_addresses.sqlite')

