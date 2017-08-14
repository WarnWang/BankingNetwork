#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step14_generate_counting_related_dummy
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_stock_dummy.p'))
df = df[df[const.STATUS] == const.COMPLETED]
df = df[[const.YEAR, const.ACQUIRER_CUSIP, const.ACQUIRER_TICKER, const.ACQUIRER_NAME]]


def count_acquire_year_info(row, last_n_year):
    year = row[const.YEAR]
    cusip = row[const.ACQUIRER_CUSIP]
    ticker = row[const.ACQUIRER_TICKER]
    name = row[const.ACQUIRER_NAME]
    status = row[const.STATUS]

    start_year = year - last_n_year
    tmp_df_1 = df[df[const.ACQUIRER_CUSIP] == cusip]
    tmp_df_2 = df[df[const.ACQUIRER_TICKER] == ticker]
    tmp_df_3 = df[df[const.ACQUIRER_NAME] == name]
    if tmp_df_1.empty and tmp_df_2.empty and tmp_df_3.empty:
        return 0

    if tmp_df_1.empty:
        tmp_df_1 = df.copy()

    if tmp_df_2.empty:
        tmp_df_2 = df.copy()

    if tmp_df_3.empty:
        tmp_df_3 = df.copy()

    index_list = list(set(tmp_df_1.index).intersection(tmp_df_2.index).intersection(tmp_df_3.index))
    tmp_df = df.loc[index_list]
    tmp_df = tmp_df[tmp_df[const.YEAR] >= start_year]
    tmp_df = tmp_df[tmp_df[const.YEAR] <= year]

    return int(tmp_df.shape[0] > 1) if status == const.COMPLETED else int(not tmp_df.empty)


def generate_1_yr_dummy(row):
    return count_acquire_year_info(row, 1)


def generate_2_yr_dummy(row):
    return count_acquire_year_info(row, 2)


def generate_3_yr_dummy(row):
    return count_acquire_year_info(row, 3)


if __name__ == '__main__':
    stock_dummy_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_stock_dummy.p'))
    stock_dummy_df['Acquirer_Acquiring_Other_Target_Dummy_1_Yr'] = stock_dummy_df.apply(generate_1_yr_dummy, axis=1)
    stock_dummy_df['Acquirer_Acquiring_Other_Target_Dummy_2_Yr'] = stock_dummy_df.apply(generate_2_yr_dummy, axis=1)
    stock_dummy_df['Acquirer_Acquiring_Other_Target_Dummy_3_Yr'] = stock_dummy_df.apply(generate_3_yr_dummy, axis=1)

    stock_dummy_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_acquirer_dummy.p'))
    stock_dummy_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_acquirer_dummy.csv'),
                          index=False)
