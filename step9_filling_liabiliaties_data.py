#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step9_filling_liabiliaties_data
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

ltq_df = pd.read_pickle(os.path.join(const.DATA_PATH, 'compustat_quarterly_total_liabilities.p'))


def fill_in_total_liabilities(row):
    market_value = row[const.ACQUIRER_MVE]

    if not np.isnan(market_value):
        return market_value

    cusip = row[const.ACQUIRER_CUSIP]
    ticker = row[const.ACQUIRER_TICKER]
    ann_date = row[const.ANNOUNCED_DATE]
    year = ann_date.year
    quarter = ann_date.month // 3 + 1
    c_qtr = '{}Q{}'.format(year, quarter)

    if hasattr(cusip, 'upper'):
        cusip = '{}10'.format(cusip.upper())
        tmp_df = ltq_df[ltq_df[const.COMPUSTAT_CUSIP] == cusip]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1] / 1000.

    if hasattr(ticker, 'upper'):
        ticker = ticker.upper()
        tmp_df = ltq_df[ltq_df[const.COMPUSTAT_TIC] == ticker]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1] / 1000.

    return np.nan


if __name__ == '__main__':
    mve_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_MVE.p'))
    mve_df[const.ACQUIRER_LT] = mve_df.apply(fill_in_total_liabilities, axis=1)

    mve_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_LT.p'))
    mve_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_LT.csv'), index=False)
