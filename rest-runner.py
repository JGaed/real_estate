import datetime as dt
import time
import mysql_wrapper as MySQL
from config import mysql_host, mysql_user, mysql_database, mysql_password
from kleinanzeigen import Kleinanzeigen

INTERVALL = 300

POSTALCODE = 20359
RADIUS = 20

while True:

    start_time  = dt.datetime.now()
    print('### Start Run')
    print('### ' + start_time.date().isoformat())
    print('### ' + start_time.time().isoformat(timespec='seconds'))
    mysql_db = MySQL(mysql_host=mysql_host, 
                          mysql_user = mysql_user, 
                          mysql_database = mysql_database, 
                          mysql_password = mysql_password)
    
    Kleinanzeigen.runner(mysql_db, postalcode=POSTALCODE,
                         radius=RADIUS)

    finished_on = dt.datetime.now()
    runtime = (finished_on - start_time).seconds
    if runtime < INTERVALL:
        sleep_time = INTERVALL - runtime
        print(f'### Sleep for {sleep_time} seconds')
        time.sleep(INTERVALL - runtime)
    print('#----------------------------#')
