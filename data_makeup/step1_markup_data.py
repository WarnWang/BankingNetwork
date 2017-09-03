#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step1_markup_data
# @Date: 3/9/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

refer_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170903_corrospend_variable.pkl'))
usecol = refer_df.branch.tolist()
usecol.append('DateAnnounced')
usecol.append('AcquirerCUSIP')
usecol.append('TargetCUSIP')

data_df = pd.read_csv(os.path.join(const.DATA_PATH,
                                   '20170902_CAR_Control_Ind_IV12_DirExe_Stata_CUSIPnum_3Part_IVtest.csv'))
makeup_df = pd.read_csv(os.path.join(const.DATA_PATH, '20160105_branch_data.csv'), usecols=usecol)

makeup_df = makeup_df.dropna(subset=['DateAnnounced', 'AcquirerCUSIP', 'TargetCUSIP'], how='any').rename(
    index=str, columns={'AcquirerCUSIP': 'AcqCUSIP', 'DateAnnounced': 'Date_Announced'})

for key in ['TargetCUSIP', 'AcqCUSIP', 'Date_Announced']:
    data_df[key] = data_df[key].apply(str)
    makeup_df[key] = makeup_df[key].apply(str)

merged_df = pd.merge(data_df, makeup_df, how='left', on=['TargetCUSIP', 'AcqCUSIP', 'Date_Announced'])

for i in refer_df.index:
    target = refer_df.loc[i, 'target']
    branch = refer_df.loc[i, 'branch']
    merged_df[target] = merged_df[target].fillna(merged_df[branch])

for i in [1, 3, 4]:
    for j in ['Acq', 'Target']:
        key = '{}CAR_{}Factor'.format(j, i)
        key_100 = '{}CAR_{}Factor100'.format(j, i)
        merged_df[key_100] = merged_df[key_100].fillna(merged_df[key] * 100)

for i in [1, 3, 4]:
    key = 'CombinedCAR_{}Factor100'.format(i)
    acq_key = 'AcqCAR_{}Factor100'.format(i)
    tar_key = 'TargetCAR_{}Factor100'.format(i)
    merged_df[key] = merged_df[key].fillna(merged_df[acq_key] + merged_df[tar_key])

merged_df = merged_df.drop(refer_df.branch.tolist(), axis=1)

merged_df.to_pickle(os.path.join(const.TEMP_PATH, '20170904_CAR_makeup_with_previous_branch.pkl'))
merged_df.to_csv(os.path.join(const.RESULT_PATH, '20170904_CAR_makeup_with_previous_branch.csv'), index=False)
