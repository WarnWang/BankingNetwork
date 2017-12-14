#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step7_add_status_variable
# @Date: 14/12/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os

import pandas as pd

from constants import Constants as const


if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.RESULT_PATH, '20171212_psm_add_pscore_overlap.csv'),
                     dtype={'Deal_Number': int, 'Acquirer_real': int, 'Target_real': int})

    df_deal_groups = df.groupby('Deal_Number')
    df_num_count = df_deal_groups.count()
    match_36_index = df_num_count[df_num_count['Status'] == 36].index
    match_1_index = df_num_count[df_num_count['Status'] == 1].index
    match_6_index = df_num_count[df_num_count['Status'] == 6].index

    match_num_36 = df[df['Deal_Number'].isin(match_36_index)].index
    match_num_1 = df[df['Deal_Number'].isin(match_1_index)].index
    match_num_6 = df[df['Deal_Number'].isin(match_6_index)].index

    df.loc[:, 'missing_rssd'] = 0
    df.loc[match_num_1, 'missing_rssd'] = 3
    df.loc[match_num_6, 'missing_rssd'] = 1

    acquire_missing = df_deal_groups.filter(lambda x: x.shape[0]==6 and x[x['Acquirer_real'] == 1].shape[0]==6).index
    df.loc[acquire_missing, 'missing_rssd'] = 2

    df.to_csv(os.path.join(const.RESULT_PATH, '20171212_psm_add_missing_rssd.csv'), index=False)
    df.to_pickle(os.path.join(const.TEMP_PATH, '20171212_psm_add_missing_rssd.pkl'))
