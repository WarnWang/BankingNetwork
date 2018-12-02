#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step14_bhc_commercial_link_file_76_93
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step14_bhc_commercial_link_file_76_93
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


if __name__ == '__main__':
    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')
    bhc_commercial_links = []
    for year in range(1976, 1994):
        call_df: DataFrame = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))
        call_df_valid = call_df[[const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364]].drop_duplicates()
        call_df_valid.loc[:, const.YEAR] = year
        call_df_valid.loc[:, const.COMMERCIAL_RSSD9364] = call_df_valid.apply(
            lambda x: x[const.COMMERCIAL_RSSD9364] if x[const.COMMERCIAL_RSSD9364] != 0
            else x[const.COMMERCIAL_RSSD9001], axis=1)
        bhc_commercial_links.append(call_df_valid)

    bhc_commercial_link_df: DataFrame = pd.concat(bhc_commercial_links, ignore_index=True, sort=False)
    bhc_commercial_link_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_link.pkl'))

    # Sort a branch list
    sod_df: DataFrame = pd.read_stata(SOD_DATA_PATH).rename(index=str,
                                                            columns={const.TOTAL_DEPOSITS: const.TOTAL_DEPOSITS_REAL})
    sod_df.loc[:, const.FIPS] = sod_df.apply(calculate_soc_fips, axis=1)
    sod_df = sod_df[sod_df['sumd9310'] != 9]
    sod_df_drop_duplicates = sod_df[
        [const.RSSD9001, const.YEAR, const.FIPS, const.TOTAL_DEPOSITS_REAL, const.BRANCH_ID]].copy()

    sod_df2: DataFrame = pd.read_stata(SOD_DATA2_PATH).dropna(subset=['sumd9150', 'sumd9210'], how='any').rename(
        index=str, columns={const.TOTAL_DEPOSITS: const.TOTAL_DEPOSITS_REAL})
    sod_df2.loc[:, const.FIPS] = sod_df2.apply(calculate_soc_fips, axis=1)
    sod_df2 = sod_df2[sod_df2['sumd9310'] != 9]
    sod_df2_drop_duplicates = sod_df2[
        [const.RSSD9001, const.YEAR, const.FIPS, const.TOTAL_DEPOSITS_REAL, const.BRANCH_ID]].copy()

    bc_link_df_rename: DataFrame = bhc_commercial_link_df.rename(
        index=str, columns={const.COMMERCIAL_RSSD9001: const.RSSD9001})
    sod_df_merged: DataFrame = pd.concat([sod_df_drop_duplicates, sod_df2_drop_duplicates], ignore_index=True,
                                         sort=False)
    sod_df_merged.loc[:, const.TOTAL_DEPOSITS_REAL] = sod_df_merged[const.TOTAL_DEPOSITS_REAL].apply(
        lambda x: float(x.replace(',', '')) if hasattr(x, 'replace') else x)
    sod_df_merged = sod_df_merged[sod_df_merged[const.YEAR] < 1994]

    sod_df_9364: DataFrame = sod_df_merged.merge(bc_link_df_rename, on=[const.RSSD9001, const.YEAR], how='left')
    sod_df_9364.loc[:, const.COMMERCIAL_RSSD9364] = sod_df_9364[const.COMMERCIAL_RSSD9364].fillna(
        sod_df_9364[const.RSSD9001])

    sod_df_9364.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_branch_link_76_93_sod.pkl'))

    fdic_dfs = [sod_df_9364]
    for year in range(1994, 2017):
        tmp_path = os.path.join(FDIC_DATA_PATH, 'ALL_{}.csv'.format(year))

        fdic_df = pd.read_csv(tmp_path, encoding='latin-1').rename(index=str, columns={
            'STCNTYBR': const.FIPS, 'YEAR': const.YEAR, 'RSSDHCR': const.COMMERCIAL_RSSD9364, 'RSSDID': const.RSSD9001,
            'DEPSUM': const.TOTAL_DEPOSITS_REAL, 'BRNUM': const.BRANCH_ID})

        fdic_dfs.append(
            fdic_df[[const.FIPS, const.YEAR, const.COMMERCIAL_RSSD9364, const.RSSD9001, const.TOTAL_DEPOSITS_REAL,
                     const.BRANCH_ID]].copy())

    bc_branch_link_7616_df: DataFrame = pd.concat(fdic_dfs, ignore_index=True, sort=False)
    bc_branch_link_7616_df.loc[:, const.COMMERCIAL_RSSD9364] = bc_branch_link_7616_df[const.COMMERCIAL_RSSD9364].fillna(
        bc_branch_link_7616_df[const.RSSD9001])
    bc_branch_link_7616_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_commercial_branch_link_76_16.pkl'))
