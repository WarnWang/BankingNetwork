#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step13_construct_some_related_variable_from_call_report
# @Date: 2018/10/4
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step12_append_some_needed_variables
"""

import os

import numpy as np
import pandas as pd

from constants import Constants as const

ACQ_9001 = '{}_{}'.format(const.ACQ, const.LINK_TABLE_RSSD9001)
TAR_9001 = '{}_{}'.format(const.TAR, const.LINK_TABLE_RSSD9001)

REAL_ESTATE_LOAN = 'RCFD1410'
NON_RESIDENTiAL_PROP = 'RCON1480'
TOTAL_ASSETS = 'RCFD2170'
MORTGAGE_BACK_SEC = 'RCFD8639'
COM_IND_LOAN = 'RCFD1766'
COM_IND_LOAN = 'RCFD1766'
CONSUMER_LOAN = 'RCFD1975'
INTEREST_FROM_CI_LOAN = 'RIAD4012'
INTEREST_FROM_RE_LOAN = 'RIAD4011'

MORTGAGE_BACK_SEC_SUB = 'RCFD0408'
MORTGAGE_BACK_SEC_SUB2 = 'RCFD0602'

USEFUL_KEYS = [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.YEAR_MERGE, REAL_ESTATE_LOAN, NON_RESIDENTiAL_PROP,
               TOTAL_ASSETS, MORTGAGE_BACK_SEC, COM_IND_LOAN, CONSUMER_LOAN, INTEREST_FROM_CI_LOAN,
               INTEREST_FROM_RE_LOAN, MORTGAGE_BACK_SEC_SUB, MORTGAGE_BACK_SEC_SUB2]


def generate_2015_2016_data(tmp_df):
    result_df = tmp_df.copy()
    for year in [2015, 2016]:
        tmp_sub_df = tmp_df[tmp_df[const.YEAR_MERGE] == 2014].copy()
        tmp_sub_df.loc[:, const.YEAR_MERGE] = year
        result_df = result_df.append(tmp_sub_df, ignore_index=True, sort=False)

    return result_df


if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181003_third_part_concise_3018_add_some_vars.pkl'))

    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    call_dfs = []

    for year in range(1976, 2015):
        call_df = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))
        call_df.loc[:, const.YEAR_MERGE] = year
        useful_keys = set(USEFUL_KEYS).intersection(call_df.keys())

        call_dfs.append(call_df[useful_keys].copy())

    merged_call_df = pd.concat(call_dfs, ignore_index=True, sort=False)
    merged_call_df.loc[:, MORTGAGE_BACK_SEC] = merged_call_df[MORTGAGE_BACK_SEC].fillna(
        merged_call_df[MORTGAGE_BACK_SEC_SUB2] + merged_call_df[MORTGAGE_BACK_SEC_SUB]
    )
    for key in [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001]:
        merged_call_df.loc[:, key] = merged_call_df[key].dropna().apply(lambda x: str(int(x)))
    merged_call_df_9001 = merged_call_df.drop([const.COMMERCIAL_RSSD9364], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9001]).groupby([const.COMMERCIAL_RSSD9001, const.YEAR_MERGE]).sum().reset_index(
        drop=False)
    merged_call_df_9364 = merged_call_df.drop([const.COMMERCIAL_RSSD9001], axis=1).dropna(
        subset=[const.COMMERCIAL_RSSD9364]).groupby([const.COMMERCIAL_RSSD9364, const.YEAR_MERGE]).sum().reset_index(
        drop=False).rename(index=str, columns={const.COMMERCIAL_RSSD9364: const.COMMERCIAL_RSSD9001})
    merged_call_df_9364 = merged_call_df_9364[merged_call_df_9364[const.COMMERCIAL_RSSD9001] != '0']
    cdf = merged_call_df_9001.append(merged_call_df_9364, ignore_index=True, sort=False)
    cdf = cdf.drop_duplicates(subset=[const.COMMERCIAL_RSSD9001, const.YEAR_MERGE], keep='last')
    cdf = generate_2015_2016_data(cdf)

    cdf.loc[:, const.REAL_ESTATE_LOANS] = (cdf[REAL_ESTATE_LOAN] - cdf[NON_RESIDENTiAL_PROP]) / cdf[TOTAL_ASSETS]
    cdf.loc[:, const.MBS] = cdf[MORTGAGE_BACK_SEC] / cdf[TOTAL_ASSETS]
    cdf.loc[:, const.COMMERCIAL_MORTGAGES] = cdf[NON_RESIDENTiAL_PROP] / cdf[TOTAL_ASSETS]
    cdf.loc[:, const.C_AND_I_LOANS] = cdf[COM_IND_LOAN] / cdf[TOTAL_ASSETS]
    cdf.loc[:, const.CONSUMER_LOANS] = cdf[CONSUMER_LOAN] / cdf[TOTAL_ASSETS]
    cdf.loc[:, const.C_AND_I_LOAN_PROFITABILITY] = cdf[INTEREST_FROM_CI_LOAN] / cdf[COM_IND_LOAN]
    cdf.loc[:, const.REAL_ESTATE_LOAN_PROFITABILITY] = cdf[INTEREST_FROM_RE_LOAN] / cdf[REAL_ESTATE_LOAN]
    cdf.loc[:, const.PROFITABILITY_RATIO] = cdf[const.C_AND_I_LOAN_PROFITABILITY] / cdf[
        const.REAL_ESTATE_LOAN_PROFITABILITY]

    keys_to_attach = [const.REAL_ESTATE_LOANS, const.MBS, const.COMMERCIAL_MORTGAGES, const.C_AND_I_LOANS,
                      const.CONSUMER_LOANS, const.C_AND_I_LOAN_PROFITABILITY, const.REAL_ESTATE_LOAN_PROFITABILITY,
                      const.PROFITABILITY_RATIO]
    keep_keys = keys_to_attach[:]
    keep_keys.append(const.YEAR_MERGE)
    keep_keys.append(const.COMMERCIAL_RSSD9001)
    cdf_useful = cdf[keep_keys]

    for prefix in [const.ACQ, const.TAR]:
        match_key = '{}_{}'.format(prefix, const.LINK_TABLE_RSSD9001)
        bhcf_rename_dict = {const.COMMERCIAL_RSSD9001: match_key}
        for data_key in keys_to_attach:
            bhcf_rename_dict[data_key] = '{}_{}'.format(prefix, data_key)
        data_to_merge = cdf_useful.rename(index=str, columns=bhcf_rename_dict)
        data_df = data_df.merge(data_to_merge, on=[match_key, const.YEAR_MERGE], how='left')

    data_df.to_pickle(os.path.join(const.TEMP_PATH, '20181004_third_part_concise_3018_add_some_vars.pkl'))
    data_df = data_df.replace({np.inf: np.nan})
    data_df.to_stata(os.path.join(const.RESULT_PATH, '20181004_third_part_concise_3018_add_some_vars.dta'),
                     write_index=False)