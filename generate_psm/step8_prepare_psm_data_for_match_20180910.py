#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step8_prepare_psm_data_for_match_20180910
# @Date: 9/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_psm.step8_prepare_psm_data_for_match_20180910
"""

import os
import multiprocessing

import pandas as pd
import pathos

from constants import Constants as const

PSM_COV_LIST = [const.INTEREST_INCOME_RATIO, const.TOTAL_ASSETS, const.NET_INCOME_LOSS, const.LEVERAGE_RATIO, const.ROA,
                const.MORTGAGE_LENDING_RATIO, const.BANK_TYPE, const.BANK_EFFICIENCY, const.BANK_TYPE,
                const.BUSINESS_FOCUS, const.SBL_RATIO, const.MORTGAGE_LENDING_RATIO]


def get_last_quarter(year, quarter):
    if quarter > 3:
        return year, quarter - 3
    else:
        return year - 1, 12


def generate_required_variables(period_tuple):
    quarterly_call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv')
    annual_call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    if len(period_tuple) == 1:
        year = period_tuple[0]
        file_path = os.path.join(annual_call_report_path, 'call{}.pkl'.format(year))
        data_df = pd.read_pickle(file_path)
        last_year_file_path = os.path.join(annual_call_report_path, 'call{}.pkl'.format(year - 1))

    else:
        year = period_tuple[0]
        quarter = period_tuple[1]
        file_path = os.path.join(quarterly_call_report_path, 'call{}{:02d}.pkl'.format(year, quarter))
        data_df = pd.read_pickle(file_path)
        last_year_file_path = os.path.join(quarterly_call_report_path, 'call{}{:02d}.pkl'.format(year - 1, quarter))

    current_var_set = set(data_df.keys())

    result_list = [period_tuple]
    for var in PSM_COV_LIST:
        if var in current_var_set or var == const.BANK_TYPE:
            continue

        if var == const.MORTGAGE_LENDING_RATIO:
            var_1 = const.MORTGAGE_LENDING
            var_2 = const.TOTAL_ASSETS

        elif var == const.SBL_RATIO:
            var_1 = const.SMALL_BUSINESS_LENDING
            var_2 = const.TOTAL_ASSETS

        elif var == const.BANK_EFFICIENCY:
            var_1 = const.TOTAL_LOANS
            var_2 = const.EMPLOYEE_NUMBER

        elif var == const.BUSINESS_FOCUS:
            var_1 = const.TOTAL_DEPOSITS
            var_2 = const.TOTAL_ASSETS

        elif var == const.INTEREST_INCOME_RATIO:
            var_1 = const.NET_INTEREST_INCOME
            var_2 = const.TOTAL_ASSETS

        elif var == const.ROA:
            var_1 = const.NET_INCOME_LOSS
            var_2 = const.TOTAL_ASSETS

        elif var == const.LEVERAGE_RATIO:
            var_1 = const.TOTAL_LIABILITIES
            var_2 = const.TOTAL_ASSETS
        else:
            continue

        if var_1 not in current_var_set or var_2 not in current_var_set:
            result_list.append((var, var_1, var_2))
        else:
            data_df.loc[:, var] = data_df[var_1] / data_df[var_2]

    if not os.path.isfile(last_year_file_path):
        result_list.append(const.BANK_TYPE)

    else:
        last_year_df = pd.read_pickle(last_year_file_path)

        current_year_sub_df = data_df[[const.TOTAL_LOANS, const.COMMERCIAL_RSSD9001]].drop_duplicates(
            subset=[const.COMMERCIAL_RSSD9001])
        last_year_sub_df = last_year_df[[const.TOTAL_LOANS, const.COMMERCIAL_RSSD9001]].drop_duplicates(
            subset=[const.COMMERCIAL_RSSD9001])

        merged_df = current_year_sub_df.merge(last_year_sub_df, on=const.COMMERCIAL_RSSD9001, how='left',
                                              suffixes=['', '_1'])
        merged_df.loc[:, const.BANK_TYPE] = (merged_df[const.TOTAL_LOANS] - merged_df[
            '{}_1'.format(const.TOTAL_LOANS)]) / merged_df['{}_1'.format(const.TOTAL_LOANS)]

        data_df = data_df.merge(merged_df[[const.COMMERCIAL_RSSD9001, const.BANK_TYPE]], on=const.COMMERCIAL_RSSD9001,
                                how='left')

    data_df.to_pickle(file_path)

    return result_list


if __name__ == '__main__':
    # Sort call report data, generate required variables (except loan growth rate)
    pool = pathos.multiprocessing.ProcessingPool(multiprocessing.cpu_count() - 3)

    year_list = [(i,) for i in range(1976, 2015)]
    year_quarter_list = [(i, j) for i in range(1976, 2015) for j in [3, 6, 9, 12]]

    miss_list = pool.map(generate_required_variables, year_list)

    miss_list_2 = pool.map(generate_required_variables, year_quarter_list)

    for item in miss_list:
        print(item)

    for item2 in miss_list_2:
        print(item2)
    # psm_data = pd.read_stata(os.path.join(const.DATA_PATH, '20180908_revision', '20180908_psm_add_missing_rssd.dta'))
    # real_psm_data = psm_data[(psm_data['Target_real'] == 1) & (psm_data['Acquirer_real'] == 1)]
    #
    # psm_group = real_psm_data.groupby([const.QUARTER, const.YEAR])
