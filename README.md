# vertex-datapull


#Summary 
We have a Mysql table under the name of score, this holds information about user's travelling routes.
The route is sent to the server once the device is unlocked and the coordinate pairs are picked up 
individually by the device every 3 minutes. 

Using a sql query to select the timestamp when the location was picked up from the device, the time the group was sent to the server , lat and long we can group and order our data the following way: 
```
report_date             created_date          lat             long    
'2018-07-23 12:50:17','2018-07-23 13:24:07','20.61172005','-100.40829606'
'2018-07-23 12:56:17','2018-07-23 13:24:07','20.61167437','-100.40831555'
'2018-07-23 13:27:25','2018-07-23 13:30:37','20.61167177','-100.40827577'
'2018-07-23 13:47:08','2018-07-23 14:19:02','20.61169680','-100.40826807'
'2018-07-23 13:50:08','2018-07-23 14:19:02','20.61170654','-100.40832073'
'2018-07-23 16:36:19','2018-07-23 16:59:29','20.56082257','-100.41531155'
'2018-07-23 16:44:01','2018-07-23 16:59:29','20.55179966','-100.42291697'
'2018-07-23 16:49:29','2018-07-23 16:59:29','20.55411145','-100.42070601'
'2018-07-23 16:52:29','2018-07-23 16:59:29','20.55753864','-100.41818360'
'2018-07-23 16:55:29','2018-07-23 16:59:29','20.56269359','-100.41385984'

 ```
We can identify the routes by the created_date column and calculate speeds and distances from each of the vertices inside each group.
Using API's like Overpass and Here Maps we can also identify street names, speed_limits, street_types and other insightful data.

I decided to iterate and do this process for every id in a list and insert the results back into a sql table.

Script completed on 01/22/2019

Libraries required: 
```
import pandas as pd
import pymysql.cursors
import numpy as np
import requests

