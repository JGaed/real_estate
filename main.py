#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kleinanzeigen import Kleinanzeigen
import pandas as pd
import numpy as np

if __name__ == '__main__':
    Kleinanzeigen.to_mysql(20359, radius=20, max_number=200)