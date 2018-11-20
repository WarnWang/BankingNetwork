#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step14_add_new_book_info
# @Date: 2018/10/5
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step14_add_new_book_info
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const


def generate_2015_2016_data(tmp_df):
    result_df = tmp_df.copy()
    for year in [2015, 2016]:
        tmp_sub_df = tmp_df[tmp_df[const.YEAR_MERGE] == 2014].copy()
        tmp_sub_df.loc[:, const.YEAR_MERGE] = year
        result_df = result_df.append(tmp_sub_df, ignore_index=True, sort=False)

    return result_df


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181004_third_part_concise_3018_add_some_vars.pkl'))
    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')

    bhcf_data_df = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))

    useful_cols = [const.YEAR_MERGE, const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001, 'BHCK4079', 'BHCK4107']

    bhcf_data_df_useful = bhcf_data_df[useful_cols]

    # organize BHCF related data
    bhcf_data_df_9001 = bhcf_data_df_useful.drop([const.COMMERCIAL_RSSD9364], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9001])
    bhcf_data_df_9364 = bhcf_data_df_useful.drop([const.COMMERCIAL_RSSD9001], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9364])
    bhcf_data_df_9364 = bhcf_data_df_9364[bhcf_data_df_9364[const.COMMERCIAL_RSSD9364] != '0']

    bhcf_data_df_9001_agg = bhcf_data_df_9001.groupby([const.COMMERCIAL_RSSD9001, const.YEAR_MERGE]).sum().reset_index(
        drop=False)
    bhcf_data_df_9364_agg = bhcf_data_df_9364.groupby([const.COMMERCIAL_RSSD9364, const.YEAR_MERGE]).sum().reset_index(
        drop=False).rename(index=str, columns={const.COMMERCIAL_RSSD9364: const.COMMERCIAL_RSSD9001})

    bhcf_data_df_merged = bhcf_data_df_9001_agg.append(bhcf_data_df_9364_agg, ignore_index=True,
                                                       sort=False).drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR_MERGE], keep='last')

    bhcf_data_df_merged.loc[:, const.TOTAL_INCOME] = bhcf_data_df_merged['BHCK4079'] + bhcf_data_df_merged['BHCK4107']
    bhcf_data_df_append = generate_2015_2016_data(bhcf_data_df_merged)[
        const.YEAR_MERGE, const.COMMERCIAL_RSSD9001, const.TOTAL_INCOME]

    for prefix in [const.ACQ, const.TAR]:
        match_key = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)
        bhcf_rename_dict = {const.COMMERCIAL_RSSD9001: match_key,
                            const.TOTAL_INCOME: '{}_{}'.format(prefix, const.TOTAL_INCOME)}

        data_to_merge = bhcf_data_df_append.rename(index=str, columns=bhcf_rename_dict)
        data_df = data_df.merge(data_to_merge, on=[match_key, const.YEAR_MERGE], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_add_total_income.pkl'))
    data_df = data_df.replace({np.inf: np.nan})
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20181005_third_part_concise_3018_add_total_income.dta'),
                     write_index=False)
