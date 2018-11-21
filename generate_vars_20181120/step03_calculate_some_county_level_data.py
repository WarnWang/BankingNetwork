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

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    branch_county_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_branch_location_total_deposit_info.pkl'))
    branch_county_df.loc[:, const.FIPS] = branch_county_df[const.FIPS].apply(int)
    branch_county_df.loc[:, const.RSSD9001] = branch_county_df[const.RSSD9001].apply(lambda x: str(int(x)))
    branch_county_group = branch_county_df.groupby(const.FIPS, group_keys=True)

    common_county_event_rssd = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))

    county_summary_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181120_county_level_data.pkl')).rename(
        index=str, columns={'BRNUM': const.BRANCH_NUM})


    def calculate_county_related_information(county_df):
        county_fips = county_df.iloc[0][const.FIPS]
        suffix = const.EXCLUDE_AT_BANK

        start_year = county_df[const.YEAR].min()
        end_year = county_df[const.YEAR].max()

        result_df = pd.DataFrame()

        if start_year == end_year:
            return result_df

        for year in range(start_year, end_year + 1):
            current_year_sub_sample = county_df[county_df[const.YEAR] == year]

            last_year = year - 1

            county_summary_row = county_summary_df[(county_summary_df[const.FIPS] == county_fips) &
                                                   (county_summary_df[const.YEAR] == year)].iloc[0]

            result_dict = {const.YEAR: year, const.BRANCH_NUM: county_summary_row[const.BRANCH_NUM],
                           const.TOTAL_DEPOSITS_REAL: county_summary_row[const.TOTAL_DEPOSITS_REAL],
                           const.TOTAL_DEPOSITS_HHI: county_summary_row[const.TOTAL_DEPOSITS_HHI]}

            county_rssd_df = common_county_event_rssd[(common_county_event_rssd[const.YEAR] == year) &
                                                      (common_county_event_rssd[const.FIPS] == county_fips)]

            this_year_exclude_at_branch_df = current_year_sub_sample[~current_year_sub_sample[const.RSSD9001].isin(
                set(county_rssd_df[const.RSSD9001]))]

            result_dict['{}_{}'.format(const.BRANCH_NUM, suffix)] = this_year_exclude_at_branch_df.shape[0]
            result_dict['{}_{}'.format(const.TOTAL_DEPOSITS_REAL, suffix)] = this_year_exclude_at_branch_df[
                const.TOTAL_DEPOSITS_REAL].sum()

            if last_year < start_year:
                result_dict[const.ENTRY_BRANCH_NUM] = np.nan
                result_dict[const.EXIT_BRANCH_NUM] = np.nan
                result_dict[const.NET_INCREASE_BRANCH_NUM] = np.nan

                for key in [const.ENTRY_BRANCH_NUM, const.EXIT_BRANCH_NUM, const.NET_INCREASE_BRANCH_NUM]:
                    result_dict['{}_{}'.format(key, const.EXCLUDE_AT_BANK)] = np.nan

            else:
                last_year_sub_sample = county_df[county_df[const.YEAR] == last_year]
                last_year_exclude_at_branch_df = last_year_sub_sample[~last_year_sub_sample[const.RSSD9001].isin(
                    set(county_rssd_df[const.RSSD9001]))]
                entry_num = 0
                exit_num = 0
                entry_eab = 0
                exit_eab = 0

                last_year_rssd9001 = set(last_year_sub_sample[const.RSSD9001])
                this_year_rssd9001 = set(current_year_sub_sample[const.RSSD9001])

                entry_rssd = this_year_rssd9001.difference(last_year_rssd9001)

                entry_num += current_year_sub_sample[current_year_sub_sample[const.RSSD9001].isin(entry_rssd)].shape[0]
                entry_eab += this_year_exclude_at_branch_df[this_year_exclude_at_branch_df[const.RSSD9001].isin(
                    entry_rssd)].shape[0]

                exit_rssd = last_year_rssd9001.difference(this_year_rssd9001)

                exit_num += last_year_sub_sample[last_year_sub_sample[const.RSSD9001].isin(exit_rssd)].shape[0]
                exit_eab += last_year_exclude_at_branch_df[last_year_exclude_at_branch_df[const.RSSD9001].isin(
                    exit_rssd)].shape[0]

                common_rssd = this_year_rssd9001.intersection(last_year_rssd9001)

                for rssd9001 in common_rssd:
                    tmp_last_year = last_year_sub_sample[last_year_sub_sample[const.RSSD9001] == rssd9001]
                    tmp_this_year = current_year_sub_sample[current_year_sub_sample[const.RSSD9001] == rssd9001]

                    entry_num += tmp_this_year[~tmp_this_year[const.BRANCH_ID].isin(
                        set(tmp_last_year[const.BRANCH_ID]))].shape[0]
                    exit_num += tmp_last_year[~tmp_last_year[const.BRANCH_ID].isin(
                        set(tmp_this_year[const.BRANCH_ID]))].shape[0]

                    eab_this_year = this_year_exclude_at_branch_df[
                        this_year_exclude_at_branch_df[const.RSSD9001] == rssd9001]
                    eab_last_year = last_year_exclude_at_branch_df[
                        last_year_exclude_at_branch_df[const.RSSD9001] == rssd9001]

                    entry_eab += eab_this_year[~eab_this_year[const.BRANCH_ID].isin(
                        set(eab_last_year[const.BRANCH_ID]))].shape[0]
                    exit_eab += eab_last_year[~eab_last_year[const.BRANCH_ID].isin(
                        set(eab_this_year[const.BRANCH_ID]))].shape[0]

                result_dict[const.ENTRY_BRANCH_NUM] = entry_num
                result_dict[const.EXIT_BRANCH_NUM] = exit_num
                result_dict[const.NET_INCREASE_BRANCH_NUM] = entry_num - exit_num

                result_dict['{}_{}'.format(const.ENTRY_BRANCH_NUM, suffix)] = entry_eab
                result_dict['{}_{}'.format(const.EXIT_BRANCH_NUM, suffix)] = exit_eab
                result_dict['{}_{}'.format(const.NET_INCREASE_BRANCH_NUM, suffix)] = entry_eab - exit_eab

            result_df = result_df.append(result_dict, ignore_index=True)

        return result_df


    county_dfs = [df for _, df in branch_county_group]

    result_dfs = list(map(calculate_county_related_information, county_dfs))
