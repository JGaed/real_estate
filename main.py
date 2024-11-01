#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kleinanzeigen import Kleinanzeigen
import misc

if __name__ == '__main__':
    offers_in_database = misc.MySQL.get_table('Kleinanzeigen_rent', ['id', 'date'], sort_by='id', max_entries=100, descanding=True)
    ids_in_database = [x[0] for x in offers_in_database]

    # Kleinanzeigen.to_mysql(20359, radius=20, end_index=ids_in_database, max_number=1000)
    df = Kleinanzeigen.create_df(postalcode=20359, radius=20, max_number=3)

# ADD TITLE TO DB
# ADD DICTIONARY WITH ALL DETAILS TO DB
