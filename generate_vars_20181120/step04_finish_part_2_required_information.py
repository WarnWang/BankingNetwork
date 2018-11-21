#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step04_finish_part_2_required_information
# @Date: 21/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
generate_vars_20181120.step04_finish_part_2_required_information
"""

import os
import multiprocessing

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    county_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181121_county_level_entry_info.pkl'))

    county_df.loc[:, const.ENTRY_BRANCH_PCT_CHANGE] = county_df[const.ENTRY_BRANCH_NUM] / county_df[const.BRANCH_NUM]
    county_df.loc[:, const.EXIT_BRANCH_PCT_CHANGE] = county_df[const.EXIT_BRANCH_NUM] / county_df[const.BRANCH_NUM]
    county_df.loc[:, const.NET_INCREASE_PCT_CHANGE] = county_df[const.NET_INCREASE_BRANCH_NUM] / county_df[
        const.BRANCH_NUM]

    suffix = const.EXCLUDE_AT_BANK

    for new_key, old_key in [(const.ENTRY_BRANCH_PCT_CHANGE, const.ENTRY_BRANCH_NUM),
                             (const.EXIT_BRANCH_PCT_CHANGE, const.EXIT_BRANCH_NUM),
                             (const.NET_INCREASE_PCT_CHANGE, const.NET_INCREASE_BRANCH_NUM)]:
        county_df.loc[:, '{}_{}'.format(new_key, suffix)] = county_df['{}_{}'.format(old_key, suffix)] / \
                                                            county_df['{}_{}'.format(const.BRANCH_NUM, suffix)]

    county_group = county_df.groupby(const.FIPS)


    def get_pct_change_variables(df):

        for key in [const.TOTAL_DEPOSITS_REAL, const.TOTAL_DEPOSITS_HHI,
                    '{}_{}'.format(const.TOTAL_DEPOSITS_REAL, const.EXCLUDE_AT_BANK)]:
            df.loc[:, '{}_pctchg'.format(key)] = df[key].pct_change(1)

        return df


    county_dfs = [df for _, df in county_group]
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 3)
    result_dfs = pool.map(get_pct_change_variables, county_dfs)
    result_df = pd.concat(result_dfs, sort=False, ignore_index=True)
    result_df.to_pickle(os.path.join(const.TEMP_PATH, '20181121_county_level_exit_entry_data.pkl'))
    result_df = result_df.replace({np.inf: np.nan})
    result_df.to_stata(os.path.join(const.RESULT_PATH, '20181121_county_level_exit_entry_data.dta'), write_index=True)
