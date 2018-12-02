#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step18_generate_county_level_data
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step18_generate_county_level_data
"""

import os
import multiprocessing

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

event_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181202_common_county_event.pkl'))
TOTAL_DEPOSIT_EAT = '{}_{}'.format(const.TOTAL_DEPOSITS_REAL, const.EXCLUDE_AT_BANK)


def _calculate_total_deposit_hhi(deposit_series):
    total_deposit = sum(deposit_series)
    try:
        square_share_series = list(map(lambda x: (x / total_deposit) ** 2, deposit_series))
    except ZeroDivisionError:
        return np.nan
    else:
        return sum(square_share_series)


def get_pct_change_variables(df):
    for key in [const.TOTAL_DEPOSITS_REAL, const.TOTAL_DEPOSITS_HHI,
                '{}_{}'.format(const.TOTAL_DEPOSITS_REAL, const.EXCLUDE_AT_BANK)]:
        df.loc[:, '{}_pctchg'.format(key)] = df[key].pct_change(1)

    return df


def calculate_county_related_branch_data(tmp_df):
    result_df = pd.DataFrame(columns=[const.YEAR, const.FIPS, const.TOTAL_DEPOSITS_REAL, const.TOTAL_DEPOSITS_HHI,
                                      const.BRANCH_NUM, const.ENTRY_BRANCH_NUM, const.EXIT_BRANCH_NUM,
                                      const.NET_INCREASE_BRANCH_NUM, const.ENTRY_BRANCH_PCT_CHANGE,
                                      const.EXIT_BRANCH_PCT_CHANGE, const.NET_INCREASE_PCT_CHANGE, TOTAL_DEPOSIT_EAT])

    start_year = tmp_df[const.YEAR].min()
    end_year = tmp_df[const.YEAR].max()
    fips = tmp_df[const.FIPS].iloc[0]
    current_fips_event_df = event_df[event_df[const.FIPS] == fips]

    for year in range(start_year, end_year + 1):
        current_year_df = tmp_df[tmp_df[const.YEAR] == year]
        event_year = current_fips_event_df[current_fips_event_df[const.YEAR] == year]

        total_deposit = current_year_df[const.TOTAL_DEPOSITS_REAL].sum()
        total_deposit_eat = current_year_df[~current_year_df[const.COMMERCIAL_RSSD9364].isin(
            set(event_year[const.COMMERCIAL_RSSD9364]))][const.TOTAL_DEPOSITS_REAL].sum()
        total_deposit_hhi = _calculate_total_deposit_hhi(current_year_df[const.TOTAL_DEPOSITS_REAL])
        branch_num = current_year_df.shape[0]

        if year == start_year:
            result_dict = {const.YEAR: year, const.TOTAL_DEPOSITS_REAL: total_deposit, const.FIPS: fips,
                           const.TOTAL_DEPOSITS_HHI: total_deposit_hhi, const.BRANCH_NUM: branch_num,
                           TOTAL_DEPOSIT_EAT: total_deposit_eat}

        else:
            last_year_df = tmp_df[tmp_df[const.YEAR] == (year - 1)]
            current_year_branch = set(current_year_df[const.BRANCH_ID])
            last_year_branch = set(last_year_df[const.BRANCH_ID])
            entry_num = len(current_year_branch.difference(last_year_branch))
            exit_num = len(last_year_branch.difference(current_year_branch))
            net_num = len(current_year_branch) - len(last_year_branch)

            result_dict = {const.YEAR: year, const.TOTAL_DEPOSITS_REAL: total_deposit, const.FIPS: fips,
                           const.TOTAL_DEPOSITS_HHI: total_deposit_hhi, const.BRANCH_NUM: branch_num,
                           const.ENTRY_BRANCH_NUM: entry_num, const.EXIT_BRANCH_NUM: exit_num,
                           const.NET_INCREASE_BRANCH_NUM: net_num, TOTAL_DEPOSIT_EAT: total_deposit_eat,
                           const.NET_INCREASE_PCT_CHANGE: float(net_num) / branch_num,
                           const.ENTRY_BRANCH_PCT_CHANGE: float(entry_num) / branch_num,
                           const.EXIT_BRANCH_PCT_CHANGE: float(exit_num) / branch_num,
                           }

        result_df = result_df.append(result_dict, ignore_index=True)

    result_df = get_pct_change_variables(result_df)

    return result_df


if __name__ == '__main__':
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_branch_link_76_16.pkl'))
    branch_bank_df: DataFrame = branch_bank_df[branch_bank_df[const.COMMERCIAL_RSSD9364] != 0].dropna(subset=[
        const.RSSD9001])
    branch_bank_df.loc[:, const.BRANCH_ID] = branch_bank_df.apply(lambda x: '{}_{}'.format(int(x[const.RSSD9001]),
                                                                                           int(x[const.BRANCH_ID])),
                                                                  axis=1)

    county_branch_group = branch_bank_df.groupby(const.FIPS)

    fips_dfs = [df for _, df in county_branch_group]

    pool = multiprocessing.Pool(38)

    fips_county_dfs = pool.map(calculate_county_related_branch_data, fips_dfs)

    fips_county_df: DataFrame = pd.concat(fips_county_dfs, ignore_index=True, sort=False)
    fips_county_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_county_branch_related_variables.pkl'))

    cra_9364_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181202_cra_append_rssd9364.pkl'))
    cra_county_df: DataFrame = cra_9364_df.drop([const.RSSD9001, const.COMMERCIAL_RSSD9364], axis=1).groupby(
        [const.FIPS, const.YEAR], group_keys=False).sum().reset_index(drop=False)
    county_df: DataFrame = fips_county_df.merge(cra_county_df, on=[const.FIPS, const.YEAR], how='left')
    county_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_county_branch_td_sbl.pkl'))
