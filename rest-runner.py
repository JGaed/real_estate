import datetime as dt
import time
from kleinanzeigen import Kleinanzeigen

INTERVALL = 300

POSTALCODE = 20359
RADIUS = 20

while True:

    start_time  = dt.datetime.now()
    print('### Start Run')
    print('### ' + start_time.date().isoformat())
    print('### ' + start_time.time().isoformat(timespec='seconds'))
    Kleinanzeigen.runner(postalcode=POSTALCODE,
                         radius=RADIUS)

    finished_on = dt.datetime.now()
    runtime = (finished_on - start_time).seconds
    if runtime < INTERVALL:
        sleep_time = INTERVALL - runtime
        print(f'### Sleep for {sleep_time} seconds')
        time.sleep(INTERVALL - runtime)
    print('#----------------------------#')
