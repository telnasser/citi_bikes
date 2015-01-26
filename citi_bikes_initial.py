#import library to get the data set from the web
import requests
r = requests.get('http://www.citibikenyc.com/stations/json')

#gathering all the fields together -- important for setting up the database
key_list = []
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

#getting the json data into the panda dataframe
from pandas.io.json import json_normalize
df = json_normalize(r.json()['stationBeanList'])

#creating sqlite database and the required tables
import sqlite3 as lite
con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
        cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

# prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#this is the dynamic table whose values changes over time.
#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist()

#add the '_' to the station name and also add the data type for SQLite since column names can't be integers
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")