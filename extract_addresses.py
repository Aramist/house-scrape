from lxml import etree


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


extract_addy('osm_cache/states/florida.osm', 'addresses_florida.txt')
