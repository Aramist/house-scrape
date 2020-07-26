from os import path
import sqlite3

from flask import Flask, jsonify, request, render_template


app = Flask(__name__)


RANGE_QUERY = '''
SELECT
    address_id,
    lat,
    lon,
    adjusted_unit_price,
    land_area_unit,
    land_area
FROM
    land_parcels a
INNER JOIN
    addresses b
ON
    a.property = b.address_id
WHERE
    year = 2020 AND
    address_id
IN (
    SELECT
        address_id
    FROM
        addresses c
    INNER JOIN
        coord_index d
    ON
        c.coord_tree_index = d.id
    WHERE
        minLat >= ? AND maxLat <= ? AND
        minLon >= ? AND maxLon <= ?
)
'''


@app.route('/api/v1', methods=['GET'])
def data_request():
    args = request.args
    if args.get('method', '') == 'landValue':
        conn = sqlite3.connect('../main_db.sqlite')
        lat, lon = args.get('lat', ''), args.get('lon', '')
        if lat == '' or lon == '':
            return jsonify({'completed': False, 'message': 'Missing latitude or longitude'})
        lat, lon = float(lat), float(lon)

        lat = [lat - 5e-1, lat + 5e-1]
        lon = [lon - 5e-1, lon + 5e-1]
        lat.extend(lon)

        cursor = conn.cursor()
        res = cursor.execute(RANGE_QUERY, lat).fetchall()

        res_json = [{'id': a[0], 'lat': a[1], 'lon': a[2], 'land_value': a[3], 'land_unit': a[4], 'land_area': a[5]} for a in res]
        return jsonify(res_json)


@app.route('/')
def root():
    html_path = 'index.html'
    return render_template(html_path)


if __name__ == '__main__':
    app.run(debug=True)
