#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step7_filling_market_value_info
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

""" filling missing data in acquirer market value """


import os

import pandas as pd
import numpy as np

from constants import Constants as const


def fill_in_market_value(row):
    market_value = row[const.ACQUIRER_MVE]

    if not np.isnan(market_value):
        return market_value

    cusip = row[const.ACQUIRER_CUSIP]
    ticker = row[const.ACQUIRER_TICKER]
    ann_date = row[const.ANNOUNCED_DATE]

    if hasattr(cusip, 'upper'):
        cusip = cusip.upper()

        if os.path.isfile(os.path.join(const.CUSIP_MARKET_VALUE_PATH, '{}10'.format(cusip))):
            mve = pd.read_pickle(os.path.join(const.CUSIP_MARKET_VALUE_PATH, '{}10'.format(cusip)))
            mve = mve[mve.index <= ann_date]
            if not mve.empty:
                return mve.iloc[-1]

    if hasattr(ticker, 'upper'):
        ticker = ticker.upper()
        if os.path.isfile(os.path.join(const.TICKER_MARKET_VALUE_PATH, ticker)):
            mve = pd.read_pickle(os.path.join(const.TICKER_MARKET_VALUE_PATH, ticker))
            mve = mve[mve.index <= ann_date]
            if not mve.empty:
                return mve.iloc[-1]

    return np.nan


if __name__ == '__main__':
    run_up_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180812_SDC_MnA_add_runup_1986_2016.p'))
    run_up_df[const.ACQUIRER_MVE] = run_up_df.apply(fill_in_market_value, axis=1)
    run_up_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_MVE.p'))
    run_up_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_MVE.csv'), index=False)
