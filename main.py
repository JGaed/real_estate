#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kleinanzeigen import Kleinanzeigen
from config import mysql_table
import misc

if __name__ == '__main__':
    offers_in_database = misc.MySQL.get_table(mysql_table, ['id', 'date'], sort_by='id', max_entries=100, descanding=True)
    ids_in_database = [x[0] for x in offers_in_database]

    Kleinanzeigen.to_mysql(postalcode=20359, radius=100, max_number=2000)
    # Kleinanzeigen.to_mysql(postalcode=20359, radius=60, end_index=ids_in_database, max_number=100)

# ADD TITLE TO DB
# ADD DICTIONARY WITH ALL DETAILS TO DB
