import pandas as pd
import pymysql.cursors
import numpy as np
import requests
import json

class Colour:
   RED = '\033[91m'
   END = '\033[0m'
   GREEN = '\033[92m'

app_id = ''
app_code = ''

#Mysql connection details
rdsConn = pymysql.connect(host = '',
                          db = '',
                          user = '',
                          password = '',
                          charset = 'utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

#Some visualization options for our console
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

#our main functions
def haversine(lon1, lat1, lon2, lat2):
    #  convert degrees to radians
    lon1 = np.deg2rad(lon1)
    lat1 = np.deg2rad(lat1)
    lon2 = np.deg2rad(lon2)
    lat2 = np.deg2rad(lat2)

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r_e = 6371
    return c * r_e

def get_unixtime(dt641, dt642):
    unix1 = dt641.astype('datetime64[s]').astype('int')
    unix2 = dt642.astype('datetime64[s]').astype('int')
    difference = unix2 - unix1
    seconds = difference/1000/60/1000000
    return seconds

select_query = "SELECT DISTINCT mobile_user_id from score  where speed_range_id in (2, 9, 10, 11, 12, 13) and mobile_user_id > 4"
query = "SELECT report_date, created_date, latitude, longitude FROM score s where s.mobile_user_id = %(mobile_user_id)s and speed_range_id > 2 group by latitude, longitude order by id asc"
insert = ""

cursor = rdsConn.cursor()
cursor1 = rdsConn.cursor()
cursor2 = rdsConn.cursor()
cursor.execute(select_query)
users = cursor.fetchall()

for id in users:
    cursor1.execute(query, id)
    mobile_user_id = int(id["mobile_user_id"])
    print("Getting tracking data for user: " + Colour.RED + str(mobile_user_id) + Colour.END)
    result = cursor1.fetchall()
    try:
        df = pd.DataFrame(result)

        # merge our data
        m = df.reset_index().merge(df.reset_index(), on='created_date')
        m = m[m.index_x != m.index_y].drop(columns=['index_x', 'index_y'])

        # change our dataset from object type to float
        m["longitude_x"] = m.longitude_x.astype(float)
        m["longitude_y"] = m.longitude_y.astype(float)
        m["latitude_x"] = m.latitude_x.astype(float)
        m["latitude_y"] = m.latitude_y.astype(float)
        m['Distance'] = haversine(m.longitude_x, m.latitude_x, m.longitude_y, m.latitude_y)
        m['Time'] = get_unixtime(m.report_date_x, m.report_date_y)
        m['Speed'] = (60 / m.Time) * m.Distance

        m.drop(m.index[m['Time'] < 0], inplace=True)
        m.drop_duplicates(subset="report_date_x", keep='first', inplace=True)

        #Iterate through our dataset to request the API's
        for index, row in m.iterrows():
            lat = row["latitude_y"]
            lon = row["longitude_y"]
            distance = int(row['Distance'])
            speed = int(row['Speed'])
            created_date = str(row['created_date'])

            response = requests.get('https://places.api.here.com/places/v1/discover/here?at='+ str(lat) + '%2C'+ str(lon) + '&size=1' + '&app_id=' + app_id + '&app_code=' + app_code)
            response2 = requests.get('https://overpass-api.de/api/interpreter?data=[out:json][timeout:25];(way(around:10,' + str(lat) + ',' + str(lon) + ')["maxspeed"];);(._;>;);out;')
            data = response.json()
            data2 = response2.json()

            street = None
            city = None
            countryCode = None
            sublocality = None
            stateCode = None
            postalCode = None
            maxspeed = None
            surface = None
            highway = None
            lanes = None
            lit = None
            sidewalk = None
            width = None
            oneway = None
            respect = None
            street_width = None

            try :
                street = data['search']['context']['location']['address']['street']
                city = data['search']['context']['location']['address']['city']
                countryCode = data['search']['context']['location']['address']['countryCode']
                sublocality = data['search']['context']['location']['address']['district']
                stateCode = data['search']['context']['location']['address']['stateCode']
                postalCode = data['search']['context']['location']['address']['postalCode']

                for line in data2['elements']:
                    try:
                        maxspeed = line['tags']['maxspeed']
                        surface = line['tags']['surface']
                        highway = line['tags']['highway']
                        lanes = line['tags']['lanes']
                        lit = line['tags']['lit']
                        sidewalk = line['tags']['sidewalk']
                        width = line['tags']['width']
                        oneway = line['tags']['oneway']
                        street_width = line['tags']['street_width']
                        if maxspeed > speed:
                            respect = 1
                        else:
                            respect = 0
                    except:
                        pass
            except:
                pass
            if street != None:
                print(mobile_user_id, created_date)
                cursor2.execute("""INSERT INTO driver_tracking (mobile_user_id, created_date, street, sublocality, postal_code, city, state, country, speed, speed_limit, respect_limit,
                 distance, highway, lanes, light, oneway, sidewalk, surface, street_width) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)""",
                                (mobile_user_id, created_date, street, sublocality, postalCode, city, stateCode, countryCode, speed, maxspeed, respect, distance, highway, lanes
                                 , lit, oneway, sidewalk, surface, street_width))
                rdsConn.commit()
                print("Successfully inserted tracking data for user: " + Colour.GREEN + str(mobile_user_id) + Colour.END)
            else:
                print("Incomplete data")
    except:
        pass




