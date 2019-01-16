import pandas as pd
import pymysql.cursors
import numpy as np
import requests
import json

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


query = "SELECT report_date, created_date, latitude, longitude FROM score s where s.mobile_user_id = 14299 and speed_range_id > 2 group by latitude, longitude order by id asc"

cursor1 = rdsConn.cursor()

cursor1.execute(query)
result = cursor1.fetchall()

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
m['Speed'] = (60/m.Time) * m.Distance

m.drop(m.index[m['Time'] < 0], inplace = True)
m.drop_duplicates(subset="report_date_x", keep='first', inplace=True)

#Iterate through our dataset to request the API's
for index, row in m.iterrows():
    lat = row["latitude_y"]
    lon = row["longitude_y"]

    response = requests.get('https://places.api.here.com/places/v1/discover/here?at='+ str(lat) + '%2C'+ str(lon) + '&size=1' + '&app_id=' + app_id + '&app_code=' + app_code)
    data = response.json()
    try :
        street = data['search']['context']['location']['address']['street']
    except:
        street = ''
    try:
        city = data['search']['context']['location']['address']['city']
    except:
        city = ''
    try:
        countryCode = data['search']['context']['location']['address']['countryCode']
    except:
        countryCode = ''
    try:
        district = data['search']['context']['location']['address']['district']
    except:
        district = ''
    try:
        stateCode = data['search']['context']['location']['address']['stateCode']
    except:
        stateCode = ''
    try:
        postalCode = data['search']['context']['location']['address']['postalCode']
    except:
        postalCode = ''




