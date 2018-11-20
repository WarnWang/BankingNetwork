#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step19_append_board_related_data
# @Date: 14/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

keys_to_add = ['board_year_1_left_num', 'board_year_1_left_percentage', 'board_year_2_left_num',
               'board_year_2_left_percentage', 'board_year_3_left_num', 'board_year_3_left_percentage',
               'exe_year_1_left_num', 'exe_year_1_left_percentage', 'exe_year_2_left_num', 'exe_year_2_left_percentage',
               'exe_year_3_left_num', 'exe_year_3_left_percentage', 'board_year_1_left_percentage100',
               'exe_year_1_left_percentage100']


def find_key_info(row, key, data_df):
    acq_cusip = row[const.ACQUIRER_CUSIP]
    tar_cusip = row[const.TARGET_CUSIP]
    acq_ticker = row[const.ACQUIRER_TICKER]
    tar_ticker = row[const.TARGET_TICKER]
    ann_date = row[const.ANNOUNCED_DATE]
    year = row[const.YEAR_MERGE]
    tmp_df = data_df[data_df[key].notnull()]
    if hasattr(acq_cusip, 'upper'):
        tmp_df = tmp_df[tmp_df[const.ACQUIRER_CUSIP] == acq_cusip]
    else:
        tmp_df = tmp_df.copy()

    if hasattr(acq_ticker, 'upper'):
        tmp_df = tmp_df[tmp_df[const.ACQUIRER_TICKER] == acq_ticker]

    tmp_df = tmp_df[tmp_df[const.YEAR_MERGE] == year]
    if tmp_df.empty:
        return np.nan

    if tmp_df.shape[0] == 1:
        return tmp_df.loc[tmp_df.index[0], key]

    tmp_df1 = tmp_df[tmp_df[const.ANNOUNCED_DATE] == ann_date]
    if tmp_df1.empty:
        tmp_df1 = tmp_df.copy()

    elif tmp_df1.shape[0] == 1:
        return tmp_df1.loc[tmp_df1.index[0], key]

    if hasattr(tar_cusip, 'upper'):
        tmp_df1 = tmp_df1[tmp_df1[const.TARGET_CUSIP] == tar_cusip]

    if hasattr(tar_ticker, 'upper'):
        tmp_df1 = tmp_df[tmp_df[const.TARGET_TICKER] == tar_ticker]

    if tmp_df1.shape[0] == 1:
        return tmp_df1.loc[tmp_df1.index[0], key]

    elif tmp_df1.empty:
        return tmp_df[key].iloc[0]

    return np.nan


if __name__ == '__main__':
    ni2_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_NI_Ratio2.p'))
    clean_df = pd.read_pickle(os.path.join(const.DATA_PATH, '20170814_dr_wang_previous_result.p'))
    keys_reserved = keys_to_add.copy()
    keys_reserved.append(const.YEAR_MERGE)
    keys_reserved.append(const.ANNOUNCED_DATE)
    keys_reserved.append(const.ACQUIRER_CUSIP)
    keys_reserved.append(const.TARGET_CUSIP)
    keys_reserved.append(const.ACQUIRER_TICKER)
    keys_reserved.append(const.TARGET_TICKER)

    clean_df = clean_df[keys_reserved]

    clean_df = clean_df.dropna(subset=keys_to_add, how='all')

    for key in keys_to_add:
        def process_row(row):
            return find_key_info(row, key, clean_df)


        ni2_df[key] = ni2_df.apply(process_row, axis=1)

    ni2_df.to_pickle(os.path.join(const.TEMP_PATH, '20170814_SDC_MnA_append_board.p'))
    ni2_df.to_csv(os.path.join(const.RESULT_PATH, '20170814_SDC_MnA_append_board.csv'), index=False)
