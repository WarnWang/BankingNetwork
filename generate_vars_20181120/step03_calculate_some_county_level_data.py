#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step03_calculate_some_county_level_data
# @Date: 20/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
generate_vars_20181120.step03_calculate_some_county_level_data
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    branch_county_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))
    branch_county_df.loc[:, const.FIPS] = branch_county_df[const.FIPS].apply(int)
    branch_county_df.loc[:, const.RSSD9001] = branch_county_df[const.RSSD9001].apply(lambda x: str(int(x)))
    branch_county_group = branch_county_df.groupby(const.FIPS)

    common_county_event_rssd = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))

    county_summary_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181120_county_level_data.pkl')).rename(
        index=str, columns={'BRNUM': const.BRANCH_NUM})


    def calculate_county_related_information(county_df):
        county_fips = county_df.iloc[0][const.FIPS]

        start_year = county_df[const.YEAR].min()
        end_year = county_df[const.YEAR].max()

        result_df = pd.DataFrame()

        if start_year == end_year:
            return result_df

        for year in range(start_year + 1, end_year):
            last_year = year - 1
            next_year = year + 1


        return result_df