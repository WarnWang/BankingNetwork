#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step12_append_some_needed_variables
# @Date: 3/10/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step12_append_some_needed_variables
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const

TOTAL_ASSETS = ['BHCK2170', 'BHCKC244', 'BHCKC248', 'BHCP2170']
MORTGAGE_LENDING = ['BHCK3164', 'BHCP3164']
TOTAL_INTEREST_INCOME = ['BHCK4107']
TOTAL_INTEREST_NONINTEREST_INCOME = ['BHCP4000']
NET_INCOME_LOSS = ['BHCK4340', 'BHCP4340', 'BHCT4340', 'BHPA4340', 'BHSP4340']

INFO_DICT = {const.TOTAL_ASSETS_ABBR: TOTAL_ASSETS,
             const.MORTGAGE_LENDING_ABBR: MORTGAGE_LENDING,
             const.INTEREST_INCOME_ABBR: TOTAL_INTEREST_INCOME,
             const.TOTAL_INTEREST_NONINTEREST_INCOME_ABBR: TOTAL_INTEREST_NONINTEREST_INCOME,
             const.NET_INCOME_LOSS_ABBR: NET_INCOME_LOSS}

ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)


def generate_2015_2016_data(tmp_df):
    result_df = tmp_df.copy()
    for year in [2015, 2016]:
        tmp_sub_df = tmp_df[tmp_df[const.YEAR] == 2014].copy()
        tmp_sub_df.loc[:, const.YEAR] = year
        result_df = result_df.append(tmp_sub_df, ignore_index=True, sort=False)

    return result_df


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180924_third_part_concise_3018_add_sbl_tl.pkl'))
    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')
    bhcf_data_df = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))

    useful_cols = [const.YEAR, const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001]
    useful_cols.extend(TOTAL_ASSETS)
    useful_cols.extend(MORTGAGE_LENDING)
    useful_cols.extend(TOTAL_INTEREST_INCOME)
    useful_cols.extend(TOTAL_INTEREST_NONINTEREST_INCOME)
    useful_cols.extend(NET_INCOME_LOSS)
    bhcf_data_df_useful = bhcf_data_df[useful_cols]

    # organize BHCF related data
    bhcf_data_df_9001 = bhcf_data_df_useful.drop([const.COMMERCIAL_RSSD9364], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9001])
    bhcf_data_df_9364 = bhcf_data_df_useful.drop([const.COMMERCIAL_RSSD9001], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9364])
    bhcf_data_df_9364 = bhcf_data_df_9364[bhcf_data_df_9364[const.COMMERCIAL_RSSD9364] != '0']

    bhcf_data_df_9001_agg = bhcf_data_df_9001.groupby([const.COMMERCIAL_RSSD9001, const.YEAR]).sum().reset_index(
        drop=False)
    bhcf_data_df_9364_agg = bhcf_data_df_9364.groupby([const.COMMERCIAL_RSSD9364, const.YEAR]).sum().reset_index(
        drop=False).rename(index=str, columns={const.COMMERCIAL_RSSD9364: const.COMMERCIAL_RSSD9001})

    bhcf_data_df_merged = bhcf_data_df_9001_agg.append(bhcf_data_df_9364_agg, ignore_index=True,
                                                       sort=False).drop_duplicates(
        subset=[const.COMMERCIAL_RSSD9001, const.YEAR], keep='last')

    rename_dict = {}
    for abbr in INFO_DICT:
        for i in INFO_DICT[abbr]:
            rename_dict[i] = '{}_{}'.format(abbr, i)

    bhcf_data_df_renamed = bhcf_data_df_merged.rename(index=str, columns=rename_dict)

    bhcf_data_df_append = generate_2015_2016_data(bhcf_data_df_renamed)

    for prefix in [const.ACQ, const.TAR]:
        match_key = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)
        bhcf_rename_dict = {const.COMMERCIAL_RSSD9001: match_key}
        for data_key in bhcf_data_df_append.keys():
            if data_key not in {const.YEAR, const.COMMERCIAL_RSSD9001}:
                bhcf_rename_dict[data_key] = '{}_{}'.format(prefix, data_key)
        data_to_merge = bhcf_data_df_append.rename(index=str, columns=bhcf_rename_dict)
        data_df = data_df.merge(data_to_merge, on=[match_key, const.YEAR], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181003_third_part_concise_3018_add_some_vars.pkl'))
    data_df = data_df.replace({np.inf: np.nan})
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20181003_third_part_concise_3018_add_some_vars.dta'),
                     write_index=False)
