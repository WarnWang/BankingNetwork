#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step15_generate_number_of_offers_target_received
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os

import pandas as pd

from constants import Constants as const

df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_stock_dummy.p'))
df = df[[const.TARGET_NAME, const.TARGET_CUSIP, const.TARGET_TICKER]]


def count_target_num(row):
    cusip = row[const.TARGET_CUSIP]
    ticker = row[const.TARGET_TICKER]
    name = row[const.TARGET_NAME]
