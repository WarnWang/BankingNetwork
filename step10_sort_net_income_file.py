#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step10_sort_net_income_file
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.DATA_PATH, 'net_income.csv'),
                     usecols=[const.COMPUSTAT_CUSIP, const.COMPUSTAT_TIC, const.COMPUSTAT_QUARTER,
                              const.COMPUSTAT_NET_INCOME],
                     dtype={const.COMPUSTAT_CUSIP: str, const.COMPUSTAT_TIC: str})
    df = df.dropna(subset=[const.COMPUSTAT_NET_INCOME]).reset_index(drop=True)
    df[const.COMPUSTAT_CUSIP] = df[const.COMPUSTAT_CUSIP].dropna().apply(lambda x: x[:-1])

    df.to_pickle(os.path.join(const.DATA_PATH, 'compustat_quarterly_net_income.p'))
