#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step12_filling_total_asset_data
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os

import pandas as pd
import numpy as np

from constants import Constants as const


def fill_in_total_asset(row, data_type, at_df):
    if data_type == const.ACQUIRER:
        total_asset = row[const.ACQUIRER_TA]
        cusip = row[const.ACQUIRER_CUSIP]
        ticker = row[const.ACQUIRER_TICKER]
    else:
        total_asset = row[const.TARGET_TA]
        cusip = row[const.TARGET_CUSIP]
        ticker = row[const.TARGET_TICKER]

    if not np.isnan(total_asset):
        return total_asset

    ann_date = row[const.ANNOUNCED_DATE]
    year = ann_date.year
    quarter = ann_date.month // 3 + 1
    c_qtr = '{}Q{}'.format(year, quarter)

    if hasattr(cusip, 'upper'):
        cusip = '{}10'.format(cusip.upper())
        tmp_df = at_df[at_df[const.COMPUSTAT_CUSIP] == cusip]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1]

    if hasattr(ticker, 'upper'):
        ticker = ticker.upper()
        tmp_df = at_df[at_df[const.COMPUSTAT_TIC] == ticker]
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_QUARTER] == c_qtr]
        if not tmp_df.empty:
            return tmp_df.iloc[0, -1]

    return np.nan


if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.DATA_PATH, 'total_asset.csv'),
                     usecols=[const.COMPUSTAT_CUSIP, const.COMPUSTAT_TIC, const.COMPUSTAT_QUARTER,
                              const.COMPUSTAT_TA],
                     dtype={const.COMPUSTAT_CUSIP: str, const.COMPUSTAT_TIC: str})
    df = df.dropna(subset=[const.COMPUSTAT_TA]).reset_index(drop=True)
    df[const.COMPUSTAT_CUSIP] = df[const.COMPUSTAT_CUSIP].dropna().apply(lambda x: x[:-1])
    df[const.COMPUSTAT_TIC] = df[const.COMPUSTAT_TIC].dropna().apply(lambda x: x.split('.')[0])

    df.to_pickle(os.path.join(const.DATA_PATH, 'compustat_quarterly_total_asset.p'))


    def fill_in_acq_total_asset(row):
        return fill_in_total_asset(row, const.ACQUIRER, df)


    def fill_in_tar_total_asset(row):
        return fill_in_total_asset(row, const.TARGET, df)


    ni_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_NI.p'))
    ni_df[const.ACQUIRER_TA] = ni_df.apply(fill_in_acq_total_asset, axis=1)
    ni_df[const.TARGET_TA] = ni_df.apply(fill_in_tar_total_asset, axis=1)

    ni_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TA.p'))
    ni_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_TA.csv'), index=False)
