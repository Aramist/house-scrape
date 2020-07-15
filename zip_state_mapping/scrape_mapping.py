from functools import partial
from time import sleep

import requests_html


SCRAPE_URL = 'https://www.zipcodestogo.com/ZIP-Codes-by-State.htm'
global_storage = dict()


def get_state_links(html):
    return_dict = dict()

    list_items = html.find('div#leftCol > ul > li')
    for item in list_items:
        link = item.absolute_links.pop()
        name = item.text
        return_dict[name] = link
    return return_dict


async def aggregate_helper(state_links, state, session):
    res = await session.get(state_links[state])
    await res.html.arender()
    print('Got html for {}'.format(state))
    html = res.html
    rows = html.find('table .inner_table > tbody > tr')
    # Drop the first two rows, they are headers
    print(len(rows))
    for row in rows:
        search = row.find('td > a')
        if not search:
            continue
        zip_code = search[0].text
        county = search[1].text

        row_data = row.find('td')
        city = row_data[1].text

        global_storage[state].append([zip_code, city, county])
        print('{}: {}, {}, {}'.format(state, zip_code, city, county))


def aggregate_zip_codes(state_links, state_list):
    asess = requests_html.AsyncHTMLSession()
    jobs = [partial(aggregate_helper, state_links, state, asess) for state in state_list]
    asess.run(*jobs)
    sleep(20)
    asess.close()


def bunch(input_list, size):
    i = 0
    while i <= len(input_list)//size:
        if size * i == len(input_list):
            break
        yield input_list[size * i:min(size * i + size, len(input_list))]
        i += 1


def main():
    with requests_html.HTMLSession() as sess:
        res = sess.get(SCRAPE_URL)

    state_links = get_state_links(res.html)
    states = list(state_links.keys())

    for state in states:
        global_storage[state] = list()

    for package in bunch(states, 5):
        aggregate_zip_codes(state_links, package)

main()
