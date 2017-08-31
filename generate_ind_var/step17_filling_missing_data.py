#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step17_filling_missing_data
# @Date: 14/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

cleaned_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170814_dr_wang_previous_result.p'))

acq_filed_dict = {const.ACQUIRER_MVE: const.ACQUIRER_MVE,
                  const.ACQUIRER_LT: const.COMPUSTAT_LIABILITY,
                  const.ACQUIRER_NI: const.COMPUSTAT_NET_INCOME,
                  const.ACQUIRER_TA: const.COMPUSTAT_TA,
                  const.ACQUIRER_TOBINQ: const.ACQUIRER_TOBINQ,
                  const.ACQUIRER_ROA: const.ACQUIRER_ROA}

tar_field_dict = {const.TARGET_NI: const.COMPUSTAT_NET_INCOME, const.TARGET_TA: const.COMPUSTAT_TA}


def filling_missing_info(row, key):
    value = row[key]
    if not np.isnan(value):
        return value

    if key in acq_filed_dict:
        cusip = row[const.ACQUIRER_CUSIP]
        ticker = row[const.ACQUIRER_TICKER]
        year = row[const.YEAR]
        field_dict = acq_filed_dict

    else:
        cusip = row[const.TARGET_TICKER]
        ticker = row[const.TARGET_CUSIP]
        year = row[const.YEAR]
        field_dict = tar_field_dict

    if hasattr(cusip, 'upper'):
        tmp_df = cleaned_df[cleaned_df[const.COMPUSTAT_CUSIP] == cusip]
    else:
        tmp_df = cleaned_df.copy()

    if hasattr(ticker, 'upper'):
        tmp_df = tmp_df[tmp_df[const.COMPUSTAT_TIC] == ticker]

    tmp_df = tmp_df[tmp_df[const.YEAR] == year]
    if tmp_df.empty:
        return np.nan

    else:
        return tmp_df.loc[tmp_df.first_valid_index(), field_dict[key]]


if __name__ == '__main__':
    tar_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_target_offer.p'))

    for i in acq_filed_dict:
        def func(row):
            return filling_missing_info(row, i)


        tar_df[i] = tar_df.apply(func, axis=1)

    for i in tar_field_dict:
        def func(row):
            return filling_missing_info(row, i)


        tar_df[i] = tar_df.apply(func, axis=1)

    tar_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_dr_information.p'))
    tar_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_dr_information.csv'), index=False)
