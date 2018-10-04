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
CONSUMER_LOAN = 'RCFD1975'
INTEREST_FROM_CI_LOAN = 'RIAD4012'
INTEREST_FROM_RE_LOAN = 'RIAD4011'

MORTGAGE_BACK_SEC_SUB = 'RCFD0408'
MORTGAGE_BACK_SEC_SUB2 = 'RCFD0602'

USEFUL_KEYS = [const.COMMERCIAL_RSSD9001, const.COMMERCIAL_RSSD9364, const.YEAR, REAL_ESTATE_LOAN, NON_RESIDENTiAL_PROP,
               TOTAL_ASSETS, MORTGAGE_BACK_SEC, COM_IND_LOAN, CONSUMER_LOAN, INTEREST_FROM_CI_LOAN,
               INTEREST_FROM_RE_LOAN, MORTGAGE_BACK_SEC_SUB, MORTGAGE_BACK_SEC_SUB2]

if __name__ == '__main__':
    data_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181003_third_part_concise_3018_add_some_vars.pkl'))

    call_report_path = os.path.join(const.DATA_PATH, 'commercial', 'commercial_csv_yearly')

    call_dfs = []

    for year in range(1976, 2015):
        call_df = pd.read_pickle(os.path.join(call_report_path, 'call{}.pkl'.format(year)))
        call_df.loc[:, const.YEAR] = year
        useful_keys = set(USEFUL_KEYS).intersection(call_df.keys())

        call_dfs.append(call_df[useful_keys].copy())

    merged_call_df = pd.concat(call_dfs, ignore_index=True, sort=False)