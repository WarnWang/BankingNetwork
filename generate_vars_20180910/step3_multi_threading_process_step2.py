#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step3_multi_threading_process_step2.py
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20180910.step3_multi_threading_process_step2
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const


def process_bank_data_df(bank_df, distance_df):
    pass


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_merged_psm_data_file.pkl'))

    bank_branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))

    headquarter_info = bank_branch_df[bank_branch_df[const.BRANCH_ID_NUM] == 0]
    true_branch_info = bank_branch_df[bank_branch_df[const.BRANCH_ID_NUM] != 0]
    only_branch_count = true_branch_info[[const.YEAR, const.COMMERCIAL_RSSD9001, const.BRANCH_ID_NUM]].groupby(
        [const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(drop=False).rename(
        index=str, columns={const.BRANCH_ID_NUM: const.BRANCH_NUM})

    for prefix in [const.ACQUIRER, const.TARGET]:
        merge_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        branch_key = '{}_{}'.format(prefix, const.BRANCH_NUM)
        tmp_branch_count = only_branch_count.rename(index=str, columns={
            const.BRANCH_NUM: branch_key,
            const.COMMERCIAL_RSSD9001: merge_key
        })
        data_df = data_df.merge(tmp_branch_count, on=[const.YEAR, merge_key], how='left')
        data_df.loc[:, branch_key] = data_df[branch_key].fillna(0)

    for key in [const.ACQHQ_TARBR_AVG_DISTANCE, const.ACQHQ_TARBR_TOTAL_DISTANCE, const.TARHQ_ACQBR_AVG_DISTANCE,
                const.TARHQ_ACQBR_TOTAL_DISTANCE, const.TOTAL_DISTANCE, const.AVERAGE_DISTANCE]:
        data_df.loc[:, key] = np.nan
