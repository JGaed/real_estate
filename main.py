#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kleinanzeigen import Kleinanzeigen
import misc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if __name__ == '__main__':
    offers_in_database = [int(x[0]) for x in misc.MySQL.get_table('Kleinanzeigen', 'id')]

    Kleinanzeigen.to_mysql(20359, radius=20, end_index=2464141260)




# ADD TITLE TO DB
# ADD DICTIONARY WITH ALL DETAILS TO DB