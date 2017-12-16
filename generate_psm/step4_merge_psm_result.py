#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step4_merge_psm_result
# @Date: 3/9/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os
import math
import csv

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    psm_result = pd.read_pickle(os.path.join(const.TEMP_PATH, '20171216_CAR_real_fault_file.pkl'))
    for prefix in [const.TARGET, const.ACQUIRER]:
        for col in [const.COMMERCIAL_ID, const.LINK_TABLE_RSSD9001]:
            key = '{}_{}'.format(prefix, col)
            psm_result[key] = psm_result[key].apply(int).apply(str)

    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170831_CAR_useful_row.pkl'))
    data_df = data_df.drop(['Acquirer_real', 'Target_real'], axis=1)
    data_df[const.QUARTER] = data_df[const.ANNOUNCED_DATE].apply(lambda x: math.ceil(x.month / 3))
    data_df['Acquirer_link_table_rssd9001'] = data_df['Acquirer_link_table_rssd9001'].apply(int).apply(str)
    data_df['Target_link_table_rssd9001'] = data_df['Target_link_table_rssd9001'].apply(int).apply(str)

    merged_psm_data_df = pd.merge(data_df, psm_result, how='right',
                                  on=['{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001),
                                      '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001),
                                      const.YEAR, const.QUARTER])

    merged_psm_data_df.to_pickle(os.path.join(const.TEMP_PATH, '20171216_merged_psm_data_file.pkl'))
    merged_psm_data_df.to_csv(os.path.join(const.RESULT_PATH, '20171216_merged_psm_data_file.csv'), index=False,
                              quoting=csv.QUOTE_ALL)
