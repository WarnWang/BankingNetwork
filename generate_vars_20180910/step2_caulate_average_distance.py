#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step2_caulate_average_distance
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20180910.step1_sort_luping_fips_data
"""

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_merged_psm_data_file.pkl'))

    branch_info = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))

    only_branch_info = branch_info[branch_info[const.BRANCH_ID_NUM] != 0]
    only_branch_count = only_branch_info[[const.YEAR, const.COMMERCIAL_RSSD9001, const.BRANCH_ID_NUM]].groupby(
        [const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(drop=False).rename(
        index=str, columns={const.BRANCH_ID_NUM: const.BRANCH_NUM})

    for prefix in [const.ACQUIRER, const.TARGET]:
        tmp_branch_count = only_branch_count.rename(index=str, columns={
            const.BRANCH_NUM: '{}_{}'.format(prefix, const.BRANCH_NUM),
            const.COMMERCIAL_RSSD9001: '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        })
