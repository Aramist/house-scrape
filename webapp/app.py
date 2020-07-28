from os import path
import sqlite3

from flask import Flask, jsonify, request, render_template


app = Flask(__name__)


RANGE_QUERY = '''
SELECT
    prop_id,
    lat,
    lon
FROM properties
WHERE prop_id
IN (
    SELECT
        id
    FROM coord_index
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

        lat = [lat - 8e-3, lat + 8e-3]
        lon = [lon - 8e-3, lon + 8e-3]
        lat.extend(lon)

        cursor = conn.cursor()
        res = cursor.execute(RANGE_QUERY, lat).fetchall()

        res_json = [{'id': a[0], 'lon': a[1], 'lat': a[2], 'land_value': 0, 'land_unit': 0, 'land_area': 0} for a in res]
        return jsonify(res_json)


@app.route('/')
def root():
    html_path = 'index.html'
    return render_template(html_path)


if __name__ == '__main__':
    app.run(debug=True)
