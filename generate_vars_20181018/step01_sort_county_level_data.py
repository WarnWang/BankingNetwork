#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step01_sort_county_level_data
# @Date: 18/10/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181018.step01_sort_county_level_data
"""

import os
import multiprocessing

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

SOD_DATA_PATH = os.path.join(const.DATA_PATH, '20180908_revision', 'SOD_1976-1985_Fed_micro_data.dta')
SOD_DATA2_PATH = os.path.join(const.DATA_PATH, '20180908_revision', 'SOD_1986-2006_Fed_micro_data.dta')
FDIC_DATA_PATH = os.path.join(const.DATA_PATH, 'FDIC_data', 'Branch Office Deposits')

BRANCH_NUMBER = 'BRNUM'
TOTAL_DEPOSITS = 'TOTAL_DEPOSITS'
TOTAL_DEPOSITS_HHI = 'TOTAL_DEPOSITS_HHI'


def _calculate_total_deposit_hhi(deposit_series):
    total_deposit = deposit_series.sum()
    share_series = deposit_series / total_deposit
    square_share_series = share_series.apply(lambda x: x ** 2)
    return total_deposit, square_share_series.sum()


def _calculate_county_data(valid_data_df: DataFrame):
    county_group = valid_data_df.groupby(const.FIPS)

    def calcule_related_information(sub_df: DataFrame):
        branch_num: int = sub_df.shape[0]
        total_deposit, total_deposit_hhi = _calculate_total_deposit_hhi(sub_df[const.TOTAL_DEPOSITS])
        return pd.Series({BRANCH_NUMBER: branch_num, TOTAL_DEPOSITS: total_deposit,
                          TOTAL_DEPOSITS_HHI: total_deposit_hhi})

    county_data_df = county_group.apply(calcule_related_information).reset_index(drop=False)
    return county_data_df


def _calculate_sod_data(year: int):
    sod_df: DataFrame = pd.read_stata(SOD_DATA_PATH if year <= 1985 else SOD_DATA2_PATH)
    if year not in set(sod_df['year']):
        return DataFrame()

    sod_valid_df: DataFrame = sod_df[(sod_df['year'] == year) & (sod_df['sumd9310'] != 9)].copy()

    def calculte_fips(row):
        county_code = int(row['sumd9150'])
        state_code = int(row['sumd9210'])
        return state_code * 1000 + county_code

    sod_valid_df.loc[:, const.FIPS] = sod_valid_df.apply(calculte_fips, axis=1)
    sod_valid_df_renamed: DataFrame = sod_valid_df.rename(index=str, columns={'sumd2200': const.TOTAL_DEPOSITS})
    sod_valid_df_valid: DataFrame = sod_valid_df_renamed[[const.FIPS, const.TOTAL_DEPOSITS]].copy()
    return _calculate_county_data(sod_valid_df_valid)


def _calculate_fdic_data(year: int):
    file_path = os.path.join(FDIC_DATA_PATH, 'ALL_{}.csv'.format(year))
    if os.path.isfile(file_path):
        fdic_data_df: DataFrame = pd.read_csv(file_path, encoding='latin-1')
    else:
        return DataFrame()

    formatted_data: DataFrame = fdic_data_df.rename(index=str,
                                                    columns={'STCNTYBR': const.FIPS, 'DEPSUMBR': const.TOTAL_DEPOSITS})
    formatted_data.loc[:, const.TOTAL_DEPOSITS] = formatted_data[const.TOTAL_DEPOSITS].apply(
        lambda x: float(x.replace(',', '') if hasattr(x, 'replace') else x))
    valid_formatted_data: DataFrame = formatted_data[[const.FIPS, const.TOTAL_DEPOSITS]].copy()
    return _calculate_county_data(valid_formatted_data)


def sort_county_level_data(year: int):
    if year < 1994:
        county_df: DataFrame = _calculate_sod_data(year)
    else:
        county_df: DataFrame = _calculate_fdic_data(year)

    if not county_df.empty:
        county_df.loc[:, const.YEAR] = year

    return county_df


def create_lag_variable(sub_df):
    valid_df: DataFrame = sub_df[[BRANCH_NUMBER, TOTAL_DEPOSITS, TOTAL_DEPOSITS_HHI]].copy()
    for year_lag in [1, 2]:
        tmp_valid_df: DataFrame = valid_df.shift(-year_lag).rename(columns=lambda x: '{}_{}'.format(x, year_lag))
        sub_df = pd.concat([sub_df, tmp_valid_df], axis=1)

    return sub_df


if __name__ == '__main__':
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 3)

    result_dfs = pool.map(sort_county_level_data, range(1976, 2017))
    result_df: DataFrame = pd.concat(result_dfs, ignore_index=True, sort=False)

    # result_df.to_pickle(os.path.join(const.TEMP_PATH, '20181018_county_level_data.pkl'))
    result_df.to_pickle(os.path.join(const.TEMP_PATH, '20181120_county_level_data.pkl'))

    result_group = result_df.groupby(const.FIPS)
    rotated_df = result_group.apply(create_lag_variable).reset_index(drop=True)
    # rotated_df.to_pickle(os.path.join(const.TEMP_PATH, '20181018_county_level_data_rotated_data.pkl'))
    rotated_df.to_pickle(os.path.join(const.TEMP_PATH, '20181120_county_level_data_rotated_data.pkl'))
