#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step4_append_lambda_to_different_file
# @Date: 8/10/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

lambda_df = pd.read_stata(os.path.join(const.RESULT_PATH, '20171008_state_year_append_lambda.dta'))
lambda_df = lambda_df[['year', 'acq', 'tgt', 'lambda']]
lambda_df = lambda_df.rename(index=str, columns={'year': const.YEAR,
                                                 'acq': 'Acq_State_abbr',
                                                 'tgt': 'Target_State_abbr'})

f_list = ['20170906_CAR_Control_Ind_IV12_DirExe_Stata_CUSIPnum_3Part_DeleteMissing_IVtest_drop.dta',
          '20170906_CAR_Control_Ind_IV12_DirExe_Stata_CUSIPnum_3Part_IVtest2_drop.dta',
          '20170925_CAR_Control_Ind_IV12_DirExe_Stata_CUSIPnum_3Part_B41995_DeleteMissing_IVtest_drop_3rdPartAll.dta',
          '20170929_CAR_Control_Ind_IV12_DirExe_Stata_CUSIPnum_3Part_DeleteMissingOverlap_IVtest_drop_3rdPartAll.dta']

for f_name in f_list:
    df = pd.read_stata(os.path.join(const.DATA_PATH, '20171008_datamerge', f_name))

    new_name = '20171008_{}'.format('_'.join(f_name.split('_')[1:]))

    merged_df = pd.merge(df, lambda_df, how='left', on=[const.YEAR, 'Acq_State_abbr', 'Target_State_abbr'])

    merged_df.to_pickle(os.path.join(const.TEMP_PATH, '{}.pkl'.format(new_name)))
    merged_df.to_stata(os.path.join(const.RESULT_PATH, new_name))
