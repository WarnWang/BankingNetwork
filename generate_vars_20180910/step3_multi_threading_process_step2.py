#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step3_multi_threading_process_step2.py
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


"""
python3 -m generate_vars_20180910.step3_multi_threading_process_step2
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const


def match_distance_from_fips(fips_data_df):
    same_fips_sub_df = fips_data_df[fips_data_df['Acquirer_FIPS'] == fips_data_df['Target_FIPS']].copy()
    if same_fips_sub_df.empty:
        dfs_to_concate = []
    else:
        same_fips_sub_df.loc[:, const.DISTANCE] = 0
        dfs_to_concate = [same_fips_sub_df]

    diff_fips_sub_df = fips_data_df[fips_data_df['Acquirer_FIPS'] != fips_data_df['Target_FIPS']].copy()

    if not diff_fips_sub_df.empty:
        data_year = fips_data_df[const.YEAR].iloc[0]
        if data_year >= 2010:
            distance_df = pd.read_stata(const.POST2010_DISTANCE_FILE)
        elif data_year >= 2000:
            distance_df = pd.read_stata(const.POST2000_DISTANCE_FILE)
        else:
            distance_df = pd.read_stata(const.OLD_DISTANCE_FILE)

        useful_distance_df = distance_df[['county1', 'county2', 'mi_to_county']].rename(index=str, columns={
            'mi_to_county': const.DISTANCE, 'county1': 'Acquirer_FIPS', 'county2': 'Target_FIPS'}).drop_duplicates(
            subset=['Acquirer_FIPS', 'Target_FIPS'])

        useful_distance_df2 = distance_df[['county1', 'county2', 'mi_to_county']].rename(index=str, columns={
            'mi_to_county': const.DISTANCE, 'county2': 'Acquirer_FIPS', 'county1': 'Target_FIPS'}).drop_duplicates(
            subset=['Acquirer_FIPS', 'Target_FIPS'])

        matched_df1 = diff_fips_sub_df.merge(useful_distance_df, on=['Acquirer_FIPS', 'Target_FIPS'], how='left')
        dfs_to_concate.append(matched_df1)
        unmatched_df1 = matched_df1[matched_df1[const.DISTANCE].isnull()]
        if not unmatched_df1.empty:
            del unmatched_df1[const.DISTANCE]

            matched_df2 = unmatched_df1.merge(useful_distance_df2, on=['Acquirer_FIPS', 'Target_FIPS'], how='left')
            dfs_to_concate.append(matched_df2)
    if dfs_to_concate:
        result_df = pd.concat(dfs_to_concate, sort=False)
    else:
        result_df = pd.DataFrame()
    return result_df


def calculate_distance_variables(distance_data_df):
    result_dict = {const.TOTAL_DISTANCE: distance_data_df[const.DISTANCE].sum(),
                   const.AVERAGE_DISTANCE: distance_data_df[const.DISTANCE].mean()}

    tar_hq_df = distance_data_df[distance_data_df['Target_BRNUM_SUMD9021'] == 0].copy()
    result_dict[const.TARHQ_ACQBR_AVG_DISTANCE] = tar_hq_df[const.DISTANCE].mean()
    result_dict[const.TARHQ_ACQBR_TOTAL_DISTANCE] = tar_hq_df[const.DISTANCE].sum()

    acq_hq_df = distance_data_df[distance_data_df['Acquirer_BRNUM_SUMD9021'] == 0].copy()
    result_dict[const.ACQHQ_TARBR_AVG_DISTANCE] = acq_hq_df[const.DISTANCE].mean()
    result_dict[const.ACQHQ_TARBR_TOTAL_DISTANCE] = acq_hq_df[const.DISTANCE].sum()

    return pd.Series(result_dict)


def constuct_distance_related_variables(distance_data_df):
    return distance_data_df.groupby([const.YEAR, const.QUARTER, 'Target_id', 'Acquirer_id']).apply(
        calculate_distance_variables).reset_index(drop=False)


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_merged_psm_data_file.pkl'))

    bank_branch_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))

    for key in [const.ACQHQ_TARBR_AVG_DISTANCE, const.ACQHQ_TARBR_TOTAL_DISTANCE, const.TARHQ_ACQBR_AVG_DISTANCE,
                const.TARHQ_ACQBR_TOTAL_DISTANCE, const.TOTAL_DISTANCE, const.AVERAGE_DISTANCE]:
        data_df.loc[:, key] = np.nan

    year_list = data_df[const.YEAR].drop_duplicates()
    quarter_list = data_df[const.QUARTER].drop_duplicates()

    distance_tmp_save_path = os.path.join(const.TEMP_PATH, '20180911_distance_match')
    constructed_tmp_save_path = os.path.join(const.TEMP_PATH, '20180911_constructed_distance_variables')

    for dir_path in [distance_tmp_save_path, constructed_tmp_save_path]:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)

    distance_dfs = []
    distance_cal_cols = [const.YEAR, const.QUARTER, '{}_{}'.format(const.TARGET, const.COMMERCIAL_ID),
                         '{}_{}'.format(const.ACQUIRER, const.COMMERCIAL_ID)]
    data_sub_df = data_df[distance_cal_cols]

    for year in year_list:
        for quarter in quarter_list:
            print('Time: {}-{}'.format(year, quarter))
            tmp_data_df = data_sub_df[(data_sub_df[const.YEAR] == year) & (data_sub_df[const.QUARTER] == quarter)]

            tmp_branch_df = bank_branch_df[bank_branch_df[const.YEAR] == year]
            tmp_save_path = os.path.join(constructed_tmp_save_path,
                                         '{}_{}_constructed_distance.pkl'.format(year, quarter))

            if os.path.isfile(tmp_save_path):
                distance_dfs.append(pd.read_pickle(tmp_save_path))
                continue

            for tag in [const.ACQUIRER, const.TARGET]:
                match_key = '{}_{}'.format(tag, const.COMMERCIAL_ID)
                tag_fips = '{}_{}'.format(tag, const.FIPS)
                tag_branch_id_num = '{}_{}'.format(tag, const.BRANCH_ID_NUM)
                rename_branch_df = tmp_branch_df.rename(index=str, columns={
                    const.FIPS: tag_fips,
                    const.BRANCH_ID_NUM: tag_branch_id_num
                })

                # first use rssd9001 to match
                match_id_key = const.COMMERCIAL_RSSD9001
                one_id_branch_df = rename_branch_df[[match_id_key, tag_fips, tag_branch_id_num]]

                match_branch_df = one_id_branch_df.rename(index=str, columns={match_id_key: match_key})
                matched_df = tmp_data_df.merge(match_branch_df, how='left', on=match_key)
                succeed_matched_df = matched_df[matched_df[tag_fips].notnull()].copy()
                unmatched_df = matched_df[matched_df[tag_fips].isnull()].copy()
                del unmatched_df[tag_fips]
                del unmatched_df[tag_branch_id_num]

                # then use rssd 9364 to match
                match_id_key = const.COMMERCIAL_RSSD9364
                one_id_branch_df = rename_branch_df[[match_id_key, tag_fips, tag_branch_id_num]].dropna(how='any')

                match_branch_df = one_id_branch_df.rename(index=str, columns={match_id_key: match_key})
                matched_df_9364 = unmatched_df.merge(match_branch_df, how='left', on=match_key)

                tmp_data_df = pd.concat([succeed_matched_df, matched_df_9364], ignore_index=True, sort=False)

            remove_duplicate_data_df = tmp_data_df.drop_duplicates()

            # remove both headquarter data
            rm_2hqs_data_df = remove_duplicate_data_df[
                ~((remove_duplicate_data_df['Acquirer_BRNUM_SUMD9021'] == 0)
                  & (remove_duplicate_data_df['Target_BRNUM_SUMD9021'] == 0)
                  )].copy()

            rm_2brs_data_df = rm_2hqs_data_df[
                ~((rm_2hqs_data_df['Acquirer_BRNUM_SUMD9021'] > 0)
                  & (rm_2hqs_data_df['Target_BRNUM_SUMD9021'] > 0)
                  )].copy()

            rm_2brs_distance_df = match_distance_from_fips(rm_2brs_data_df)
            rm_2brs_distance_df.to_pickle(os.path.join(distance_tmp_save_path,
                                                       '{}_{}_matched_result.pkl'.format(year, quarter)))

            constructed_distance_var = constuct_distance_related_variables(rm_2brs_distance_df)
            constructed_distance_var.to_pickle(tmp_save_path)
            distance_dfs.append(constructed_distance_var)

    all_distance_df = pd.concat(distance_dfs, ignore_index=True, sort=False)
    all_distance_df.to_pickle(os.path.join(const.TEMP_PATH, '20180911_constructed_distance_variables.pkl'))

    merged_previous_info_df = data_df.merge(all_distance_df, on=[const.YEAR, const.QUARTER, 'Target_id', 'Acquirer_id'],
                                            how='left')
    merged_previous_info_df.to_pickle(os.path.join(const.TEMP_PATH, '20180911_psm_add_distance_vars.pkl'))
    merged_previous_info_df.to_csv(os.path.join(const.RESULT_PATH, '20180911_psm_add_distance_vars.csv'), index=str)
