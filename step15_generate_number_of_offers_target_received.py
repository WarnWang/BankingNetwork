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

    tmp_df_1 = df[df[const.TARGET_CUSIP] == cusip]
    if tmp_df_1.empty:
        tmp_df_1 = df.copy()
    tmp_df_2 = df[df[const.TARGET_TICKER] == ticker]
    if tmp_df_2.empty:
        tmp_df_2 = df.copy()
    tmp_df_3 = df[df[const.TARGET_NAME] == name]
    if tmp_df_3.empty:
        tmp_df_3 = df.copy()

    return len(set(tmp_df_1.index).intersection(tmp_df_2.index).intersection(tmp_df_3.index))


if __name__ == '__main__':
    acquirer_dummy_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_acquirer_dummy.p'))
    acquirer_dummy_df['Acquirer_Acquiring_Other_Target_Dummy_3_Yr'] = acquirer_dummy_df.apply(count_target_num, axis=1)

    acquirer_dummy_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_target_offer.p'))
    acquirer_dummy_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_target_offer.csv'),
                             index=False)
