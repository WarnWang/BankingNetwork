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
                 index_col=0, dtype={'index': str, 'year': str, 'acq_access_tgt': str, 'tgt_access_acq': str})

d_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20171008_distance_between_different_states.pkl'))

d_df = d_df.reset_index().rename(index=str, columns={"state1": 'acq', "state2": 'tgt'})

merged_df = pd.merge(df, d_df, on=['acq', 'tgt'], how='left')

merged_df.to_pickle(os.path.join(const.TEMP_PATH, '20171008_state_state_data_add_distance.pkl'))
