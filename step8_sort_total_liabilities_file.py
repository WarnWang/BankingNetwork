#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step8_sort_total_liabilities_file
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


""" The value unit is mil usd """

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.DATA_PATH, 'quarterly_total_liabilities.csv'),
                     usecols=[const.COMPUSTAT_CUSIP, const.COMPUSTAT_TIC, const.COMPUSTAT_QUARTER,
                              const.COMPUSTAT_LIABILITY],
                     dtype={const.COMPUSTAT_CUSIP: str, const.COMPUSTAT_TIC: str})
    df = df.dropna(subset=[const.COMPUSTAT_LIABILITY]).reset_index(drop=True)
    df[const.COMPUSTAT_CUSIP] = df[const.COMPUSTAT_CUSIP].dropna().apply(lambda x: x[:-1])
    df[const.COMPUSTAT_TIC] = df[const.COMPUSTAT_TIC].dropna().apply(lambda x: x.split('.')[0])

    df.to_pickle(os.path.join(const.DATA_PATH, 'compustat_quarterly_total_liabilities.p'))
