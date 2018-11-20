#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step3_generate_state_acq_dummy
# @Date: 8/10/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20171008_state_state_data_add_distance.pkl'))
df['year'] = df.year.apply(int)

acq_df = pd.read_excel(os.path.join(const.DATA_PATH, '20171008_SDC_bank_merger_state.xlsx'))

acq_df = acq_df[[const.YEAR_MERGE, 'Acquirer_State_abbr', 'Target_State_abbr']]

acq_df = acq_df.rename(index=str, columns={"Acquirer_State_abbr": 'acq',
                                           'Target_State_abbr': 'tgt',
                                           const.YEAR_MERGE: 'year'}).drop_duplicates()

acq_df.loc[:, 'acq_state_acquiring_tar_state_dummy'] = 1

merged_df = pd.merge(df, acq_df, on=['year', 'acq', 'tgt'], how='left')
merged_df['acq_state_acquiring_tar_state_dummy'] = merged_df['acq_state_acquiring_tar_state_dummy'].fillna(0).apply(int)
merged_df['acq_access_tgt'] = merged_df['acq_access_tgt'].apply(int)
merged_df['tgt_access_acq'] = merged_df['tgt_access_acq'].apply(int)

merged_df.to_pickle(os.path.join(const.TEMP_PATH, '20171008_state_state_data_add_dep_var.pkl'))
merged_df.to_stata(os.path.join(const.RESULT_PATH, '20171008_state_state_data_add_dep_var.dta'))
