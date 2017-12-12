#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step2_try_to_do_pscore
# @Date: 31/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os
import math

import pandas as pd

from constants import Constants as const

df = pd.read_excel(os.path.join(const.DATA_PATH, '20170829_CAR_Control_Ind_IV12_DirExe.xlsx'))

useful_column = [const.ANNOUNCED_DATE, const.YEAR]

for tag in [const.ACQUIRER, const.TARGET]:
    df.loc[:, '{}_{}'.format(tag, const.REAL)] = 1
    useful_column.append('{}_{}'.format(tag, const.REAL))

df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_CAR_real_tag.pkl'))

useful_df = df.dropna(subset=['{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001),
                              '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)], how='any')

for tag in [const.ACQUIRER, const.TARGET]:
    key = '{}_{}'.format(tag, const.LINK_TABLE_RSSD9001)
    useful_df.loc[:, key] = pd.to_numeric(useful_df[key], errors='raise', downcast='integer')
    useful_column.append(key)

useful_df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_CAR_useful_row.pkl'))

useful_df = useful_df[useful_column]

useful_df[const.QUARTER] = useful_df[const.ANNOUNCED_DATE].apply(lambda x: math.ceil(x.month / 3))

useful_df.to_pickle(os.path.join(const.TEMP_PATH, '20170831_CAR_useful_col.pkl'))
