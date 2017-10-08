#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step2_append_distance_data_to_original_data
# @Date: 8/10/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

df = pd.read_csv(os.path.join(const.DATA_PATH, '20161215_state_year_append_variables.csv'),
                 index_col=0, dtype={'year': int, 'acq_access_tgt': int, 'tgt_access_acq': int})

del df['index']

df_sub_group = df.groupby(['year', 'acq'])

new_index = df_sub_group.groups.keys()

for year, state in new_index:
    tmp_df = df_sub_group.get_group((year, state))
    index = df.shape[0]
    df.loc[index, 'year'] = year
    df.loc[index, 'acq'] = df.loc[index, 'tgt'] = state
    df.loc[index, 'acq_access_tgt'] = df.loc[index, 'tgt_access_acq'] = 1
    df.loc[index, 'acq_access_tgt'] = df.loc[index, 'acq_access_tgt'] = state
    df.loc[index, 'acq_state_gdp'] = df.loc[index, 'tgt_state_gdp'] = tmp_df.loc[tmp_df.index[0], 'acq_state_gdp']
    df.loc[index, 'acq_population'] = df.loc[index, 'tgt_population'] = tmp_df.loc[tmp_df.index[0], 'acq_population']
    df.loc[index, 'acq_state_gdp_growth'] = df.loc[index, 'tgt_state_gdp_growth'] = \
        tmp_df.loc[tmp_df.index[0], 'acq_state_gdp_growth']

d_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20171008_distance_between_different_states.pkl'))

d_df = d_df.reset_index().rename(index=str, columns={"state1": 'acq', "state2": 'tgt'})

merged_df = pd.merge(df, d_df, on=['acq', 'tgt'], how='left')

merged_df.to_pickle(os.path.join(const.TEMP_PATH, '20171008_state_state_data_add_distance.pkl'))
