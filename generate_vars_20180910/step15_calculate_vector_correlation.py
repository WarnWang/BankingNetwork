#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step15_calculate_vector_correlation
# @Date: 2018/10/5
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import numpy as np
import pandas as pd

from constants import Constants as const

KEY_SUFFIXES = ['REL', 'MBS', 'COMM_M', 'CI_LOAN', 'C_LOAN', 'CIL_PROF', 'REL_PROF', 'PROF_R']


def get_correlation(row):
    tar_list = []
    acq_list = []
    for key_suffix in KEY_SUFFIXES:
        acq_key = '{}_{}'.format(const.ACQ, key_suffix)
        tar_key = '{}_{}'.format(const.TAR, key_suffix)

        acq_val = row[acq_key]
        tar_val = row[tar_key]

        if np.isnan(acq_val) or np.isnan(tar_val):
            continue

        tar_list.append(tar_val)
        acq_list.append(acq_val)

    if tar_list:
        return np.corrcoef(np.array(acq_list), np.array(tar_list))[0][1]
    else:
        return np.nan


if __name__ == '__main__':
    data_df = pd.read_stata(os.path.join(const.DATA_PATH, '20181005_third_part_concise_3018.dta'))

    data_df.loc[:, 'Acq_Tar_Correlation'] = data_df.apply(get_correlation, axis=1)

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_correlation.pkl'))
    data_df.to_stata(os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_correlation.dta'),
                     write_index=False)
