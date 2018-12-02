#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step15_multiprocessing_county_level_event_list
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20181120.step15_multiprocessing_county_level_event_list
"""

import os
import multiprocessing

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_branch_link_76_16.pkl'))
    branch_bank_df.loc[:, const.FIPS] = branch_bank_df[const.FIPS].apply(int)


    def acquire_row_info_list(row):
        acq_9001: str = row[const.ACQ_9001]
        tar_9001: str = row[const.TAR_9001]
        common_county_event = pd.DataFrame(columns=[const.FIPS, const.YEAR, const.COMMERCIAL_RSSD9364])

        if acq_9001.isdigit():
            acq_9001 = int(acq_9001)
        else:
            return common_county_event

        if tar_9001.isdigit():
            tar_9001 = int(tar_9001)
        else:
            return common_county_event

        year = int(row[const.YEAR_MERGE])

        current_branch_df = branch_bank_df[branch_bank_df[const.YEAR] == year]

        acq_branch = current_branch_df[current_branch_df[const.COMMERCIAL_RSSD9364] == acq_9001]
        tar_branch = current_branch_df[current_branch_df[const.COMMERCIAL_RSSD9364] == tar_9001]

        if tar_branch.empty or acq_branch.empty:
            return common_county_event

        co_exist_county = acq_branch[acq_branch[const.FIPS].isin(set(tar_branch[const.FIPS]))]
        if co_exist_county.empty:
            return common_county_event

        fips_list = list(set(co_exist_county[const.FIPS]))
        for fips in fips_list:
            common_county_event = common_county_event.append({const.FIPS: fips, const.YEAR: year,
                                                              const.COMMERCIAL_RSSD9364: acq_9001}, ignore_index=True)
            common_county_event = common_county_event.append({const.FIPS: fips, const.YEAR: year,
                                                              const.COMMERCIAL_RSSD9364: tar_9001}, ignore_index=True)

        return common_county_event


    row_list = [i for _, i in data_df.iterrows()]

    pool = multiprocessing.Pool(38)
    result_dfs = pool.map(acquire_row_info_list, row_list)
    event_df: DataFrame = pd.concat(result_dfs, ignore_index=True, sort=False)
    event_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_common_county_event_rssd.pkl'))
