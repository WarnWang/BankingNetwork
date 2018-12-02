#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step16_sort_bhc_county_level_data
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20181120.step16_sort_bhc_county_level_data
"""

import os
import multiprocessing

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const


def calculate_bhc_county_annual_change(tmp_df):
    result_df = pd.DataFrame(columns=[const.ENTRY_BRANCH_NUM, const.EXIT_BRANCH_NUM, const.NET_INCREASE_BRANCH_NUM,
                                      const.YEAR, const.FIPS, const.COMMERCIAL_RSSD9364])
    start_year = tmp_df[const.YEAR].min()
    end_year = tmp_df[const.YEAR].max()
    rssd9364 = tmp_df[const.COMMERCIAL_RSSD9364].iloc[0]
    fips = tmp_df[const.FIPS].iloc[0]
    result_df = result_df.append({const.YEAR: start_year, const.ENTRY_BRANCH_NUM: np.nan,
                                  const.NET_INCREASE_BRANCH_NUM: np.nan, const.EXIT_BRANCH_NUM: np.nan,
                                  const.COMMERCIAL_RSSD9364: rssd9364, const.FIPS: fips},
                                 ignore_index=True)

    for year in range(start_year + 1, end_year + 1):
        current_year_df: DataFrame = tmp_df[tmp_df[const.YEAR] == year]
        last_year_df: DataFrame = tmp_df[tmp_df[const.YEAR] == (year - 1)]

        current_year_branch_id = set(current_year_df['branch_id'])
        last_year_branch_id = set(last_year_df['branch_id'])

        entry_num = len(current_year_branch_id.difference(last_year_branch_id))
        exit_num = len(last_year_branch_id.difference(current_year_branch_id))
        net_num = len(current_year_branch_id) - len(last_year_branch_id)
        result_df = result_df.append({const.YEAR: year, const.ENTRY_BRANCH_NUM: entry_num,
                                      const.NET_INCREASE_BRANCH_NUM: net_num, const.EXIT_BRANCH_NUM: exit_num,
                                      const.COMMERCIAL_RSSD9364: rssd9364, const.FIPS: fips},
                                     ignore_index=True)

    return result_df


if __name__ == '__main__':
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_branch_link_76_16.pkl'))
    branch_bank_df: DataFrame = branch_bank_df[branch_bank_df[const.COMMERCIAL_RSSD9364] != 0].dropna(subset=[
        const.RSSD9001])
    branch_bank_df.loc[:, 'branch_id'] = branch_bank_df.apply(lambda x: '{}_{}'.format(int(x[const.RSSD9001]),
                                                                                       int(x[const.BRANCH_ID])), axis=1)

    branch_bank_group = branch_bank_df.groupby([const.COMMERCIAL_RSSD9364, const.FIPS, const.YEAR])

    branch_bank_count = branch_bank_group['branch_id'].count().reset_index(drop=False).rename(
        index=str, columns={'branch_id': const.BRANCH_NUM})
    branch_bank_td = branch_bank_group[const.TOTAL_DEPOSITS_REAL].sum().reset_index(drop=False)

    bhc_county_group = branch_bank_df.groupby([const.COMMERCIAL_RSSD9364, const.FIPS])
    bhc_county_dfs = [df for _, df in bhc_county_group]
    pool = multiprocessing.Pool(38)
    bhc_net_change_dfs = pool.map(calculate_bhc_county_annual_change, bhc_county_dfs)
    bhc_net_change_df: DataFrame = pd.concat(bhc_net_change_dfs, ignore_index=True, sort=False)
    bhc_net_change_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_count.pkl'))

    bhc_county_df = bhc_net_change_df.merge(branch_bank_count, on=[const.FIPS, const.COMMERCIAL_RSSD9364, const.YEAR])
    bhc_county_df2 = bhc_county_df.merge(branch_bank_td, on=[const.FIPS, const.COMMERCIAL_RSSD9364, const.YEAR])
    bhc_county_df2.loc[:, const.ENTRY_BRANCH_PCT_CHANGE] = bhc_county_df2[const.ENTRY_BRANCH_NUM] / bhc_county_df2[
        const.BRANCH_NUM]
    bhc_county_df2.loc[:, const.EXIT_BRANCH_PCT_CHANGE] = bhc_county_df2[const.EXIT_BRANCH_NUM] / bhc_county_df2[
        const.BRANCH_NUM]
    bhc_county_df2.loc[:, const.NET_INCREASE_PCT_CHANGE] = bhc_county_df2[const.NET_INCREASE_BRANCH_NUM] / \
                                                           bhc_county_df2[const.BRANCH_NUM]

    bhc_county_df2.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_count_td.pkl'))
