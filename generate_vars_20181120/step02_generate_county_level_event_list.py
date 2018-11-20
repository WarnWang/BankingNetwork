#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_generate_county_level_event_list
# @Date: 20/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step02_generate_county_level_event_list
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))
    branch_bank_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))
    branch_bank_df.loc[:, const.FIPS] = branch_bank_df[const.FIPS].apply(int)
    branch_bank_df.loc[:, const.RSSD9001] = branch_bank_df[const.RSSD9001].apply(lambda x: str(int(x)))

    common_county_event = pd.DataFrame(columns=[const.FIPS, const.YEAR, const.RSSD9001])

    for i in data_df.index:
        acq_9001 = data_df.loc[i, const.ACQ_9001]
        tar_9001 = data_df.loc[i, const.TAR_9001]
        year = data_df.loc[i, const.YEAR_MERGE]

        current_branch_df = branch_bank_df[branch_bank_df['year'] == year]

        acq_branch = current_branch_df[current_branch_df[const.RSSD9001] == acq_9001]
        tar_branch = current_branch_df[current_branch_df[const.RSSD9001] == tar_9001]

        if tar_branch.empty or acq_branch.empty:
            continue

        co_exist_county = acq_branch[acq_branch[const.FIPS].isin(set(tar_branch[const.FIPS]))]
        if co_exist_county.empty:
            continue

        fips_list = list(set(co_exist_county[const.FIPS]))
        for fips in fips_list:
            common_county_event = common_county_event.append({const.FIPS: fips, const.YEAR: year,
                                                              const.RSSD9001: acq_9001}, ignore_index=True)
            common_county_event = common_county_event.append({const.FIPS: fips, const.YEAR: year,
                                                              const.RSSD9001: tar_9001}, ignore_index=True)

    common_county_event.drop_duplicates().to_pickle(
        os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))
