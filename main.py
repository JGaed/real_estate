#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kleinanzeigen import Kleinanzeigen
import misc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if __name__ == '__main__':
    offers_in_database = misc.MySQL.get_table('Kleinanzeigen', ['id', 'timestamp'], sort_by='timestamp', max_entries=10)

    Kleinanzeigen.to_mysql(20359, radius=20, max_number=400)




# ADD TITLE TO DB
# ADD DICTIONARY WITH ALL DETAILS TO DB