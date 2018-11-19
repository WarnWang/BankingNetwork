#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step02_append_fips_information_to_previous_dataset
# @Date: 18/10/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181018.step02_append_fips_information_to_previous_dataset
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)

SOD_DATA_PATH = os.path.join(const.DATA_PATH, '20180908_revision', 'SOD_1976-1985_Fed_micro_data.dta')
CALL_REPORT_PATH = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_correlation.pkl'))

    # get bhc county information
    # sod_df: DataFrame = pd.read_stata(SOD_DATA_PATH)
    # sod_hq_df: DataFrame = sod_df[sod_df['sumd9021'] == -1].copy()
    #
    # sod_hq_df_valid: DataFrame = sod_hq_df[sod_hq_df['year'] < 1994].copy()
    # sod_hq_df_valid.loc[:, const.FIPS] = sod_hq_df_valid['sumd9210'] * 1000 + sod_hq_df_valid['sumd9150']
    # sod_hq_df_useful = sod_hq_df_valid[['rssd9001', 'year', const.FIPS]].rename(index=str, columns={
    #     'year': const.YEAR, const.FIPS: '{}_{}'.format(const.TAR, const.FIPS), 'rssd9001': TAR_9001})

    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')

    bhcf_data_df = pd.read_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))
    bhcf_data_df.loc[:, const.FIPS] = bhcf_data_df['RSSD9210'] * 1000 + bhcf_data_df['RSSD9150']
    bhcf_data_df_valid = bhcf_data_df[[const.COMMERCIAL_RSSD9001, const.YEAR, const.FIPS]].rename(
        columns={const.FIPS: '{}_{}'.format(const.TAR, const.FIPS), const.COMMERCIAL_RSSD9001: TAR_9001})
    data_df_append_tar_fips1 = data_df.merge(bhcf_data_df_valid, on=[const.YEAR, TAR_9001], how='left')

    bhcf_data_df_acq = bhcf_data_df[[const.COMMERCIAL_RSSD9001, const.YEAR, const.FIPS]].rename(
        columns={const.FIPS: '{}_{}'.format(const.ACQ, const.FIPS), const.COMMERCIAL_RSSD9001: ACQ_9001})
    data_df_append_tar_fips1 = data_df_append_tar_fips1.merge(bhcf_data_df_acq, on=[const.YEAR, ACQ_9001], how='left')

    # try call report
    call_dfs = []
    for year in range(1976, 2015):
        call_df = pd.read_pickle(os.path.join(CALL_REPORT_PATH, 'call{}.pkl'.format(year)))
        call_df.loc[:, const.FIPS] = call_df['RSSD9210'] * 1000 + call_df['RSSD9150']
        call_df.loc[:, const.YEAR] = year
        call_df_valid: DataFrame = call_df[[const.COMMERCIAL_RSSD9001, const.YEAR, const.FIPS]].rename(
            columns={const.FIPS: '{}_{}'.format(const.TAR, const.FIPS), const.COMMERCIAL_RSSD9001: TAR_9001})
        call_dfs.append(call_df_valid)

    for year in [2015, 2016]:
        tmp_call_df: DataFrame = call_dfs[-1]
        tmp_call_df.loc[:, const.YEAR] = year
        call_dfs.append(tmp_call_df)

    merged_call_df: DataFrame = pd.concat(call_dfs, ignore_index=True, sort=False)
    merged_call_df.loc[:, TAR_9001] = merged_call_df[TAR_9001].apply(lambda x: str(int(x)))
    merged_call_df: DataFrame = merged_call_df.drop_duplicates(subset=[TAR_9001, const.YEAR])
    data_df_append_tar_fips = data_df.merge(merged_call_df, on=[const.YEAR, TAR_9001], how='left')

    merged_call_df_acq = merged_call_df.rename(index=str,
                                               columns={TAR_9001: ACQ_9001,
                                                        '{}_{}'.format(const.TAR, const.FIPS):
                                                            '{}_{}'.format(const.ACQ, const.FIPS)})
    data_df_append_tar_fips = data_df_append_tar_fips.merge(merged_call_df_acq, on=[const.YEAR, ACQ_9001], how='left')

    fips_seris = data_df_append_tar_fips1['Tar_FIPS'].fillna(data_df_append_tar_fips['Tar_FIPS'])
    data_df_append_tar_fips.loc[:, 'Tar_FIPS'] = fips_seris

    fips_seris = data_df_append_tar_fips1['Acq_FIPS'].fillna(data_df_append_tar_fips['Acq_FIPS'])
    data_df_append_tar_fips.loc[:, 'Acq_FIPS'] = fips_seris
    data_df_append_tar_fips.to_pickle(os.path.join(const.TEMP_PATH, '20181018_third_part_concise_3018_append_fips.pkl'))
