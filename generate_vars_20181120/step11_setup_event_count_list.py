#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step11_setup_event_count_list
# @Date: 2018-11-28
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step11_setup_event_count_list
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    county_branch_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181128_county_branch_status_count.pkl'))
    event_df: DataFrame = pd.read_pickle(os.path.join(
        const.TEMP_PATH, '20181121_third_part_concise_3018_append_some_variables.pkl'))

    county_branch_df = county_branch_df[county_branch_df[const.RSSD9001].notnull()]
    county_branch_df.loc[:, const.RSSD9001] = county_branch_df[const.RSSD9001].apply(lambda x: str(int(x)))


    def query_fips_list(row):
        acq_rssd = row[const.ACQ_9001]
        tar_rssd = row[const.TAR_9001]
        year = row[const.YEAR_MERGE]

        sub_df = county_branch_df[county_branch_df[const.RSSD9001].isin({acq_rssd, tar_rssd})]
        sub_df = sub_df[sub_df[const.YEAR] == year]

        fips_list = list(set(sub_df[const.FIPS]))
        result_df = pd.DataFrame()

        for fips in fips_list:
            row_dict = {'Deal_Number': row['Deal_Number'], const.FIPS: fips}
            result_df = result_df.append(row_dict, ignore_index=True)

        if result_df.empty:
            result_df = result_df.append({'Deal_Number': row['Deal_Number']}, ignore_index=True)

        return result_df


    result_event_dfs = []

    for i in event_df.index:
        result_event_dfs.append(query_fips_list(event_df.loc[i]))
    result_event_df: DataFrame = pd.concat(result_event_dfs, ignore_index=True, sort=False)
    event_df_merge_county = event_df.merge(result_event_df, on=['Deal_Number'])
    event_df_merge_county.to_pickle(os.path.join(const.TEMP_PATH, '20181128_event_count_list.pkl'))

    for prefix in [const.TAR, const.ACQ]:
        merge_key = const.TAR_9001 if prefix == const.TAR else const.ACQ_9001

        rename_dict = {const.RSSD9001: merge_key, const.YEAR: const.YEAR_MERGE}

        for key in [const.EXIT_BRANCH_NUM, const.ENTRY_BRANCH_NUM, const.BRANCH_NUM, const.TOTAL_DEPOSITS_REAL]:
            rename_dict[key] = '{}_{}'.format(prefix, key)

        county_branch_renamed = county_branch_df.rename(index=str, columns=rename_dict)

        event_df_merge_county = event_df_merge_county.merge(county_branch_renamed,
                                                            on=[const.YEAR_MERGE, const.FIPS, merge_key],
                                                            how='left')

    event_df_merge_county.to_pickle(os.path.join(const.TEMP_PATH, '20181128_event_count_list_county_info.pkl'))
