#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step2_caulate_average_distance
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20180910.step2_caulate_average_distance
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const


def calculate_hq_branch_total_distance(hq_id, tar_id, query_year=1990, hq_info=None, sub_branch_info=None):
    if query_year >= 2010:
        distance_df = pd.read_stata(const.POST2010_DISTANCE_FILE)

    elif query_year >= 2000:
        distance_df = pd.read_stata(const.POST2000_DISTANCE_FILE)

    else:
        distance_df = pd.read_stata(const.OLD_DISTANCE_FILE)

    hq_fips_info = hq_info[hq_info[const.COMMERCIAL_RSSD9001] == hq_id]
    if hq_fips_info.empty:
        hq_fips_info = hq_info[hq_info[const.COMMERCIAL_RSSD9364] == hq_id]

    if hq_fips_info.empty:
        return np.nan

    elif hq_fips_info.shape[0] > 1:
        tmp_sub_df = hq_fips_info[hq_fips_info[const.YEAR] == query_year]
        if tmp_sub_df.empty:
            hq_fips = hq_fips_info[const.FIPS].iloc[0]
        else:
            hq_fips = tmp_sub_df[const.FIPS].iloc[0]
    else:
        hq_fips = hq_fips_info[const.FIPS].iloc[0]

    sub_branch_fips = sub_branch_info[(sub_branch_info[const.COMMERCIAL_RSSD9001] == tar_id)
                                      & (sub_branch_info[const.YEAR] == query_year)]

    td = 0

    for j in sub_branch_fips.index:
        branch_fips = sub_branch_fips.loc[j, const.FIPS]
        if branch_fips == hq_fips:
            continue

        sub_distance_df = distance_df[(distance_df['county1'] == hq_fips) & (distance_df['county2'] == branch_fips)]
        if not sub_distance_df.empty:
            td += sub_distance_df['mi_to_county'].iloc[0]
        else:
            sub_distance_df = distance_df[(distance_df['county2'] == hq_fips) & (distance_df['county1'] == branch_fips)]
            td += sub_distance_df['mi_to_county'].iloc[0]

    return td


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_merged_psm_data_file.pkl'))

    branch_info = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))

    headquarter_info = branch_info[branch_info[const.BRANCH_ID_NUM] == 0]
    only_branch_info = branch_info[branch_info[const.BRANCH_ID_NUM] != 0]
    only_branch_count = only_branch_info[[const.YEAR, const.COMMERCIAL_RSSD9001, const.BRANCH_ID_NUM]].groupby(
        [const.YEAR, const.COMMERCIAL_RSSD9001]).count().reset_index(drop=False).rename(
        index=str, columns={const.BRANCH_ID_NUM: const.BRANCH_NUM})

    for prefix in [const.ACQUIRER, const.TARGET]:
        merge_key = '{}_{}'.format(prefix, const.COMMERCIAL_ID)
        branch_key = '{}_{}'.format(prefix, const.BRANCH_NUM)
        tmp_branch_count = only_branch_count.rename(index=str, columns={
            const.BRANCH_NUM: branch_key,
            const.COMMERCIAL_RSSD9001: merge_key
        })
        data_df = data_df.merge(tmp_branch_count, on=[const.YEAR, merge_key], how='left')
        data_df.loc[:, branch_key] = data_df[branch_key].fillna(0)

    for key in [const.ACQHQ_TARBR_AVG_DISTANCE, const.ACQHQ_TARBR_TOTAL_DISTANCE, const.TARHQ_ACQBR_AVG_DISTANCE,
                const.TARHQ_ACQBR_TOTAL_DISTANCE, const.TOTAL_DISTANCE, const.AVERAGE_DISTANCE]:
        data_df.loc[:, key] = np.nan

    for i in data_df.index:
        print(i)
        acq_id = data_df.loc[i, '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID)]
        tar_id = data_df.loc[i, '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID)]
        acq_branch_num = data_df.loc[i, '{}_{}'.format(const.ACQUIRER, const.BRANCH_NUM)]
        tar_branch_num = data_df.loc[i, '{}_{}'.format(const.TARGET, const.BRANCH_NUM)]
        year = data_df.loc[i, const.YEAR]

        if acq_branch_num >= 1:
            total_distance = calculate_hq_branch_total_distance(tar_id, acq_id, year)
            average_distance = total_distance / acq_branch_num
            data_df.loc[i, const.TARHQ_ACQBR_TOTAL_DISTANCE] = total_distance
            data_df.loc[i, const.TARHQ_ACQBR_AVG_DISTANCE] = average_distance

        if tar_branch_num >= 1:
            total_distance = calculate_hq_branch_total_distance(acq_id, tar_id, year)
            average_distance = total_distance / tar_branch_num
            data_df.loc[i, const.ACQHQ_TARBR_TOTAL_DISTANCE] = total_distance
            data_df.loc[i, const.ACQHQ_TARBR_AVG_DISTANCE] = average_distance

        if acq_branch_num + tar_branch_num >= 1:
            total_distance = data_df.loc[
                                 i, const.ACQHQ_TARBR_TOTAL_DISTANCE] + data_df.loc[i, const.TARHQ_ACQBR_TOTAL_DISTANCE]
            average_distance = total_distance / (acq_branch_num + tar_branch_num)
            data_df.loc[i, const.TOTAL_DISTANCE] = total_distance
            data_df.loc[i, const.AVERAGE_DISTANCE] = average_distance

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20180910_psm_data_append_distance.pkl'))
