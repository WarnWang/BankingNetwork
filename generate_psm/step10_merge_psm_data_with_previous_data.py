#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step10_merge_psm_data_with_previous_data
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_psm.step10_merge_psm_data_with_previous_data
"""

import os
import csv

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    psm_result = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_CAR_real_fault_file.pkl'))
    for prefix in [const.TARGET, const.ACQUIRER]:
        for col in [const.COMMERCIAL_ID, const.LINK_TABLE_RSSD9001]:
            key = '{}_{}'.format(prefix, col)
            psm_result[key] = psm_result[key].apply(int).apply(str)

    psm_col_set = set(psm_result.keys())

    psm_data = pd.read_stata(os.path.join(const.DATA_PATH, '20180908_revision', '20180908_psm_add_missing_rssd.dta'))
    data_df = psm_data[(psm_data['Target_real'] == 1) & (psm_data['Acquirer_real'] == 1)]
    data_df = data_df.drop(['Acquirer_real', 'Target_real'], axis=1)
    data_df.loc[:, 'Acquirer_link_table_rssd9001'] = data_df['Acquirer_link_table_rssd9001'].apply(int).apply(str)
    data_df.loc[:, 'Target_link_table_rssd9001'] = data_df['Target_link_table_rssd9001'].apply(int).apply(str)

    data_col_set = set(data_df.keys())

    intersection_keys = psm_col_set.intersection(data_col_set)
    intersection_keys.difference_update(['{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001),
                                         '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001),
                                         const.YEAR, const.QUARTER])

    data_df = data_df.drop(list(intersection_keys), axis=1)

    merged_psm_data_df = pd.merge(data_df, psm_result, how='right',
                                  on=['{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001),
                                      '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001),
                                      const.YEAR, const.QUARTER])

    merged_psm_data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180910_merged_psm_data_file.pkl'))
    merged_psm_data_df.to_csv(os.path.join(const.RESULT_PATH, '20180910_merged_psm_data_file.csv'), index=False,
                              quoting=csv.QUOTE_ALL)
