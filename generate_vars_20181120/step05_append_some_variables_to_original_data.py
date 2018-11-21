#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step05_append_some_variables_to_original_data
# @Date: 21/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
This file is from the 20181121 new requirement.
Append total loans, total sales (or total revenue), rcfd0071, total salaries and net income
however total loans and net income have already been included.

python3 -m generate_vars_20181120.step05_append_some_variables_to_original_data
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))

    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')
    bhcf_data_df: DataFrame = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))

    bhcf_used_columns = ['BHCK4135', 'BHCP4135', 'BHCK8560', 'BHBCA220', 'BHCKA220', 'BHCP3612',
                         const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.YEAR_MERGE]
    bhcf_useful_df = bhcf_data_df[bhcf_used_columns].copy()

    bhcf_sorted_dfs = []
    for link_id in [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001]:
        tmp_bhcf_df = bhcf_useful_df.drop(
            [const.COMMERCIAL_RSSD9001 if link_id == const.COMMERCIAL_RSSD9364 else const.COMMERCIAL_RSSD9364], axis=1)
        tmp_bhcf_df = tmp_bhcf_df.dropna(subset=[link_id])
        tmp_bhcf_df = tmp_bhcf_df[tmp_bhcf_df[link_id] != '0']
        tmp_bhcf_df = tmp_bhcf_df.groupby([link_id, const.YEAR_MERGE], group_keys=False).sum().reset_index(
            drop=False).rename(index=str, columns={link_id: const.COMMERCIAL_RSSD9001})

        bhcf_sorted_dfs.append(tmp_bhcf_df)

    bhcf_sorted_df = pd.concat(bhcf_sorted_dfs, ignore_index=True, sort=False).drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR_MERGE], keep='last')

    for year in [2015, 2016]:
        tmp_bhcf_df = bhcf_sorted_df[bhcf_sorted_df[const.YEAR_MERGE] == 2014].copy()
        tmp_bhcf_df.loc[:, const.YEAR_MERGE] = year
        bhcf_sorted_df = bhcf_sorted_df.append(tmp_bhcf_df, ignore_index=True, sort=False)

    for prefix in [const.TAR, const.ACQ]:
        link_key = const.TAR_9001 if prefix == const.TAR else const.ACQ_9001
        bhcf_renamed_df = bhcf_sorted_df.rename(index=str, columns={
            'BHCK4135': '{}_{}_1'.format(prefix, const.TOTAL_SALARIES),
            'BHCP4135': '{}_{}_2'.format(prefix, const.TOTAL_SALARIES),
            'BHCK8560': '{}_{}_1'.format(prefix, const.TOTAL_SALES),
            'BHBCA220': '{}_{}_2'.format(prefix, const.TOTAL_SALES),
            'BHCKA220': '{}_{}_3'.format(prefix, const.TOTAL_SALES),
            'BHCP3612': '{}_{}_4'.format(prefix, const.TOTAL_SALES),
            const.COMMERCIAL_RSSD9001: link_key
        })
        data_df = data_df.merge(bhcf_renamed_df, on=[link_key, const.YEAR_MERGE], how='left')

    # append call report related variables
    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    call_dfs = []
    call_df_useful_columns = [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, 'RIAD4135',
                              'RCFD0071', 'RIAD5416']

    for year in range(1976, 2015):
        call_df = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))

        useful_keys = set(call_df_useful_columns).intersection(call_df.keys())
        call_df_useful = call_df[list(useful_keys)].copy()

        sub_call_dfs = []
        for key in [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001]:
            call_df_valid = call_df_useful.drop(
                [const.COMMERCIAL_RSSD9001 if key == const.COMMERCIAL_RSSD9364 else const.COMMERCIAL_RSSD9364],
                axis=1
            )
            call_df_valid = call_df_valid[call_df_valid[key].notnull()]
            call_df_valid.loc[:, key] = call_df_valid[key].apply(str)
            call_df_valid = call_df_valid[call_df_valid[key] != '0']

            sub_call_dfs.append(call_df_valid.groupby(key, group_keys=False).sum().reset_index(drop=False).rename(
                index=str, columns={key: const.COMMERCIAL_RSSD9001}
            ))

        call_df_formatted = pd.concat(sub_call_dfs, ignore_index=True, sort=False)
        call_df_formatted.loc[:, const.YEAR_MERGE] = year
        call_dfs.append(call_df_formatted)

    merged_call_df: DataFrame = pd.concat(call_dfs, ignore_index=True, sort=False).drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR_MERGE], keep='last')

    for year in [2015, 2016]:
        tmp_call_df = merged_call_df[merged_call_df[const.YEAR_MERGE] == 2014].copy()
        tmp_call_df.loc[:, const.YEAR_MERGE] = year
        merged_call_df = merged_call_df.append(tmp_call_df, ignore_index=True, sort=False)

    for prefix in [const.TAR, const.ACQ]:
        link_key = const.TAR_9001 if prefix == const.TAR else const.ACQ_9001
        call_renamed_df = merged_call_df.rename(index=str, columns={
            'RIAD4135': '{}_{}_3'.format(prefix, const.TOTAL_SALARIES),
            'RCFD0071': '{}_{}_1'.format(prefix, 'RCFD0071'),
            'RIAD5416': '{}_{}_5'.format(prefix, const.TOTAL_SALES),
            const.COMMERCIAL_RSSD9001: link_key
        })
        data_df = data_df.merge(call_renamed_df, on=[link_key, const.YEAR_MERGE], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181121_third_part_concise_3018_append_some_variables.pkl'))
    data_df  = data_df.replace({np.inf: np.nan})
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20181121_third_part_concise_3018_append_some_variables.dta'),
                     write_index=False)

