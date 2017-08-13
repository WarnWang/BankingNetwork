#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step11_filling_net_income_data
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

ltq_df = pd.read_pickle(os.path.join(const.DATA_PATH, 'compustat_quarterly_net_income.p'))


def fill_in_net_income(row, data_type):
    if data_type == const.ACQUIRER:
        market_value = row[const.ACQUIRER_NI]
        cusip = row[const.ACQUIRER_CUSIP]
        ticker = row[const.ACQUIRER_TICKER]
    else:
        market_value = row[const.TARGET_NI]
        cusip = row[const.TARGET_CUSIP]
        ticker = row[const.TARGET_TICKER]

    if not np.isnan(market_value):
        return market_value

    ann_date = row[const.ANNOUNCED_DATE]
    year = ann_date.year
    quarter = ann_date.month // 3 + 1
    c_qtr = '{}Q{}'.format(year, quarter)

    if hasattr(cusip, 'upper'):
        cusip = '{}10'.format(cusip.upper())
        tmp_df = ltq_df[ltq_df[const.COMPUSTAT_CUSIP] == cusip]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1]

    if hasattr(ticker, 'upper'):
        ticker = ticker.upper()
        tmp_df = ltq_df[ltq_df[const.COMPUSTAT_TIC] == ticker]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1]

    return np.nan


def fill_in_acq_net_income(row):
    return fill_in_net_income(row, const.ACQUIRER)


def fill_in_tar_net_income(row):
    return fill_in_net_income(row, const.TARGET)


if __name__ == '__main__':
    lt_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_LT.p'))
    lt_df[const.ACQUIRER_NI] = lt_df.apply(fill_in_acq_net_income, axis=1)
    lt_df[const.TARGET_NI] = lt_df.apply(fill_in_tar_net_income, axis=1)

    lt_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_NI.p'))
    lt_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_NI.csv'), index=False)
