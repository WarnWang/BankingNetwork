#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step04_generate_tar_county_df
# @Date: 19/11/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181018.step04_generate_tar_county_df
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

SOD_DATA_PATH = os.path.join(const.DATA_PATH, '20180908_revision', 'SOD_1976-1985_Fed_micro_data.dta')
SOD_DATA2_PATH = os.path.join(const.DATA_PATH, '20180908_revision', 'SOD_1986-2006_Fed_micro_data.dta')
FDIC_DATA_PATH = os.path.join(const.DATA_PATH, 'FDIC_data', 'Branch Office Deposits')


def calculate_soc_fips(row):
    county_code = int(row['sumd9150'])
    state_code = int(row['sumd9210'])
    return state_code * 1000 + county_code


RSSD9001 = 'rssd9001'
YEAR = 'year'
ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)

if __name__ == '__main__':
    county_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181119_county_level_data_rotated_data.pkl'))

    data_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181005_third_part_concise_3018_append_fips.pkl'))

    data_df_drop_fips = data_df.drop(['Tar_FIPS', 'Acq_FIPS'], axis=1)

    # generate firm FIPS list
    sod_df: DataFrame = pd.read_stata(SOD_DATA_PATH)
    sod_df.loc[:, const.FIPS] = sod_df.apply(calculate_soc_fips, axis=1)
    sod_df_drop_duplicates = sod_df[[RSSD9001, YEAR, const.FIPS]].copy().drop_duplicates()

    sod_df2: DataFrame = pd.read_stata(SOD_DATA2_PATH).dropna(subset=['sumd9150', 'sumd9210'], how='any')
    sod_df2.loc[:, const.FIPS] = sod_df2.apply(calculate_soc_fips, axis=1)
    sod_df2_drop_duplicates = sod_df2[[RSSD9001, YEAR, const.FIPS]].copy().drop_duplicates()

    fdic_dfs = [sod_df_drop_duplicates, sod_df2_drop_duplicates]
    for year in range(1994, 2017):
        tmp_path = os.path.join(FDIC_DATA_PATH, 'ALL_{}.csv'.format(year))

        fdic_df = pd.read_csv(tmp_path, encoding='latin-1').rename(index=str, columns={
            'STCNTYBR': const.FIPS, 'YEAR': YEAR, 'RSSDHCR': RSSD9001})

        fdic_dfs.append(fdic_df[[const.FIPS, YEAR, RSSD9001]].copy())

    merged_df = pd.concat(fdic_dfs, ignore_index=True, sort=False).dropna(subset=[RSSD9001]).drop_duplicates()
    merged_df.loc[:, RSSD9001] = merged_df[RSSD9001].apply(lambda x: str(int(x)))

    for key in [const.TAR, const.ACQ]:
        merged_key = TAR_9001 if key == const.TAR else ACQ_9001
        merged_df_renamed = merged_df.rename(index=str, columns={RSSD9001: merged_key,
                                                                 const.FIPS: '{}_{}'.format(key, const.FIPS),
                                                                 YEAR: const.YEAR})
        merge_fips_df = data_df_drop_fips.merge(merged_df_renamed, on=[const.YEAR, merged_key], how='left')

        county_df_renamed = county_df.rename(lambda x: '{}_{}'.format(key, x) if x != const.YEAR else x, axis=1)
        merge_county_df = merge_fips_df.merge(county_df_renamed, on=[const.YEAR, '{}_{}'.format(key, const.FIPS)],
                                              how='left')
        merge_county_df.to_pickle(
            os.path.join(const.TEMP_PATH, '20181119_third_part_{}_county_information_data.pkl'.format(key)))
        merge_county_df.to_stata(
            os.path.join(const.RESULT_PATH, '20181119_third_part_{}_county_information_data.dta'.format(key)),
            write_index=False)
