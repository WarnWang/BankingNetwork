#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step4_generate_branch_level_related_data
# @Date: 11/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step4_generate_branch_level_related_data
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const

const.ACQUIRER = 'Acq'
const.TARGET = 'Tar'

ACQ_9001 = '{}_{}'.format(const.ACQUIRER, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TARGET, const.LINK_TABLE_RSSD9001)

TOTAL_LOANS = ['BHCP2125', 'BHCK5524', 'BHCK5525', 'BHCK5526', 'BHCK2122', 'BHCP1403', 'BHCP1407']
const.EMPLOYEE_NUMBER = 'BHCK4150'
TOTAL_ASSETS = ['BHC02170', 'BHC22170', 'BHC52170', 'BHC92170', 'BHCE2170', 'BHCK2170', 'BHCKC244', 'BHCKC248',
                'BHCP2170', 'BHCT2170', 'BHCK3368', 'BHCKA224', 'BHCKB696', 'BHCKB697', 'BHCKB698', 'BHCKB699',
                'BHCT3368', 'BHBC3368']
TOTAL_DEPOSITES = ['BHCKF252', 'BHCP2200']

if __name__ == '__main__':
    part3_df = pd.read_stata(os.path.join(const.DATA_PATH, '20180908_revision',
                                          '20180908_third_part_concise_3118.dta'))
    part3_df = part3_df[part3_df[const.YEAR] >= 1986].reset_index(drop=True)

    for key in [ACQ_9001, TAR_9001]:
        part3_df.loc[:, key] = part3_df[key].dropna().apply(lambda x: str(int(x)))

    bhc_save_path = os.path.join(const.DATA_PATH, 'bhc_csv_all_yearly')
    if not os.path.isdir(bhc_save_path):
        os.makedirs(bhc_save_path)

    # sort data file
    for year in range(1986, 2015):
        data_file = pd.read_csv(os.path.join(const.DATA_PATH, '20180908_revision', 'bhc_csv_all_yearly',
                                             'bhcf{}.csv'.format(year))).rename(lambda x: x.upper(), axis='columns')
        data_file.loc[:, const.YEAR] = year
        for key in [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001]:
            data_file.loc[:, key] = data_file[key].dropna().apply(lambda x: str(int(x)))
        data_file = data_file.drop_duplicates(subset=[const.COMMERCIAL_RSSD9001], keep='last')
        data_file = data_file.loc[:, ~data_file.columns.duplicated()]

        data_file.to_pickle(os.path.join(bhc_save_path, 'bhcf{}.pkl'.format(year)))

    # calculate bank type
    last_year_data_file = pd.read_pickle(os.path.join(bhc_save_path, 'bhcf{}.pkl'.format(1986)))
    data_dfs = [last_year_data_file.copy()]
    useful_key_list = [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.YEAR]

    for year in range(1987, 2015):
        current_year_data_file = pd.read_pickle(os.path.join(bhc_save_path, 'bhcf{}.pkl'.format(year)))
        common_keys = set(current_year_data_file.keys()).intersection(last_year_data_file.keys())

        bank_type_df = None

        for i, loan_key in enumerate(TOTAL_LOANS):
            if loan_key not in common_keys:
                continue
            bank_type_key = '{}_{}'.format(const.BANK_TYPE, i + 1)
            if bank_type_key not in useful_key_list:
                useful_key_list.append(bank_type_key)
            current_loan = current_year_data_file[[const.COMMERCIAL_RSSD9001, loan_key]].dropna(how='any')
            last_loan = last_year_data_file[[const.COMMERCIAL_RSSD9001, loan_key]].dropna(how='any')
            loan_data = current_loan.merge(last_loan, on=const.COMMERCIAL_RSSD9001, how='left', suffixes=['', '_1'])
            loan_data.loc[:, bank_type_key] = (loan_data[loan_key] - loan_data[
                '{}_1'.format(loan_key)]) / loan_data['{}_1'.format(loan_key)]
            if bank_type_df is None:
                bank_type_df = loan_data[[const.COMMERCIAL_RSSD9001, bank_type_key]].copy()

            else:
                bank_type_df = bank_type_df.merge(loan_data[[const.COMMERCIAL_RSSD9001, bank_type_key]],
                                                  on=[const.COMMERCIAL_RSSD9001], how='outer')

        last_year_data_file = current_year_data_file.copy()
        current_year_data_file = current_year_data_file.merge(bank_type_df, on=[const.COMMERCIAL_RSSD9001], how='left')
        current_year_data_file.to_pickle(os.path.join(bhc_save_path, 'bhcf{}_add_bt.pkl'.format(1986)))
        data_dfs.append(current_year_data_file)

    all_data_df = pd.concat(data_dfs, ignore_index=True, sort=False)
    all_data_df.to_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data.pkl'))

    # calculate bank efficiency
    for i, loan_key in enumerate(TOTAL_LOANS):
        bank_efficiency_key = '{}_{}'.format(const.BANK_EFFICIENCY, i + 1)
        all_data_df.loc[:, bank_efficiency_key] = all_data_df[loan_key] / all_data_df[const.EMPLOYEE_NUMBER]
        useful_key_list.append(bank_efficiency_key)

    # calculate BUSINESS focus
    index = 1
    for dep_key in TOTAL_DEPOSITES:
        for ta_key in TOTAL_ASSETS:
            bus_focus_key = '{}_{}'.format(const.BUSINESS_FOCUS, index)
            all_data_df.loc[:, bus_focus_key] = all_data_df[dep_key] / all_data_df[ta_key]
            useful_key_list.append(bus_focus_key)
            index += 1

    all_data_df.to_pickle(os.path.join(bhc_save_path, '1986_2014_all_bhcf_data_append_variables.pkl'))

    useful_data_df = all_data_df[useful_key_list].copy()
    useful_data_df_2014 = useful_data_df[useful_data_df[const.YEAR] == 2014].copy()

    for i in [2015, 2016]:
        copy_df = useful_data_df_2014.copy()
        copy_df.loc[:, const.YEAR] = i
        useful_data_df = useful_data_df.append(copy_df, ignore_index=True)

    for prefix in [const.TARGET, const.ACQUIRER]:
        match_id = ACQ_9001 if prefix == const.ACQUIRER else TAR_9001

        rename_dict = {const.COMMERCIAL_RSSD9001: match_id}
        for i in useful_key_list:
            if i in {const.YEAR, const.COMMERCIAL_RSSD9001}:
                continue
            rename_dict[i] = '{}_{}'.format(prefix, i)

        current_data_df = useful_data_df.rename(index=str, columns=rename_dict)
        part3_df = part3_df.merge(current_data_df, on=[match_id, const.YEAR], how='left')

    part3_df.to_pickle(os.path.join(const.TEMP_PATH, '20180911_third_part_concise_3018.pkl'))
    part3_df = part3_df.replace({float('inf'): np.nan})
    part3_df.to_stata(os.path.join(const.RESULT_PATH, '20180911_third_part_concise_3018.dta'), write_index=False)
