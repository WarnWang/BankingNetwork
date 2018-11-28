#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step10_generate_bhc_county_level
# @Date: 2018-11-27
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step10_generate_bhc_county_level
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const


def calculate_bank_county_info(tmp_df):
    fips_sub = tmp_df[[const.TOTAL_DEPOSITS_REAL, const.FIPS, const.YEAR, 'sumd9021']].copy()
    fips_group = fips_sub.groupby([const.FIPS, const.YEAR])

    branch_count = fips_group['sumd9021'].count().reset_index(drop=False).rename(
        index=str, columns={'sumd9021': const.BRANCH_NUM})
    branch_total_deposit = fips_group[const.TOTAL_DEPOSITS_REAL].sum().reset_index(drop=False)
    start_year = tmp_df[const.YEAR].min()
    end_year = tmp_df[const.YEAR].max()

    result_df = pd.DataFrame(columns=[const.FIPS, const.YEAR, const.EXIT_BRANCH_NUM, const.ENTRY_BRANCH_NUM])
    for fips in set(tmp_df[const.FIPS]):
        for year in range(start_year, end_year + 1):
            sub_tmp_df = tmp_df[tmp_df[const.YEAR] == year]
            last_tmp_df = tmp_df[tmp_df[const.YEAR] == year - 1]
            next_tmp_df = tmp_df[tmp_df[const.YEAR] == year + 1]
            result_dict = {const.YEAR: year, const.FIPS: fips}

            if year == start_year:
                result_dict[const.ENTRY_BRANCH_NUM] = np.nan

            else:
                current_branch_id = set(sub_tmp_df[const.BRANCH_ID])
                last_year_branch_id = set(last_tmp_df[const.BRANCH_ID])
                result_dict[const.ENTRY_BRANCH_NUM] = len(current_branch_id.difference(last_year_branch_id))

            if year == end_year:
                result_dict[const.EXIT_BRANCH_NUM] = np.nan

            else:
                current_branch_id = set(sub_tmp_df[const.BRANCH_ID])
                next_year_branch_id = set(next_tmp_df[const.BRANCH_ID])
                result_dict[const.EXIT_BRANCH_NUM] = len(current_branch_id.difference(next_year_branch_id))

            result_df = result_df.append(result_dict, ignore_index=True)

    result_df = result_df.merge(branch_count, on=[const.FIPS, const.YEAR])
    result_df = result_df.merge(branch_total_deposit, on=[const.FIPS, const.YEAR])

    return result_df


if __name__ == '__main__':
    bank_branch_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))

    county_branch_df: DataFrame = bank_branch_df.groupby(const.RSSD9001).apply(calculate_bank_county_info).reset_index(
        drop=False)[[const.RSSD9001, const.FIPS, const.YEAR, const.EXIT_BRANCH_NUM, const.ENTRY_BRANCH_NUM,
                     const.BRANCH_NUM, const.TOTAL_DEPOSITS]].copy()
    county_branch_df.to_pickle(os.path.join(const.TEMP_PATH, '20181127_county_branch_status_count.pkl'))
