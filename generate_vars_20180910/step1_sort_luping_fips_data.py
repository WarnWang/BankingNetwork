#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step1_sort_luping_fips_data
# @Date: 10/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20180910.step1_sort_luping_fips_data
"""

import os

import pandas as pd

from constants import Constants as const

if __name__ == '__main__':
    data_path = '/home/zigan/Documents/wangyouan/research/BankingNetwork/yuluping/overlap'
    branch_df = pd.read_csv(os.path.join(data_path, '20170816_ross_martin_fdic_76-16.csv')).rename(
        index=str, columns={'RSSDID_RSSD9001': const.COMMERCIAL_RSSD9001, 'YEAR': const.YEAR_MERGE,
                            'STNUMBR_SUMD9210': const.FIPS_STATE_CODE, 'CNTYNUMB_SUMD9150': const.FIPS_COUNTY_CODE,
                            'RSSDHCR_RSSD9364': const.COMMERCIAL_RSSD9364})

    branch_df = branch_df.dropna(subset=[const.FIPS_COUNTY_CODE, const.FIPS_STATE_CODE], how='any')
    for key in [const.COMMERCIAL_RSSD9001, const.FIPS_COUNTY_CODE, const.FIPS_STATE_CODE, const.COMMERCIAL_RSSD9364]:
        branch_df.loc[:, key] = branch_df[key].dropna().apply(lambda x: str(int(x)))

    branch_df.loc[:, const.FIPS] = branch_df.apply(lambda x: '{:02d}{:03d}'.format(int(x[const.FIPS_STATE_CODE]),
                                                                                   int(x[const.FIPS_COUNTY_CODE])),
                                                   axis=1)
    keep_col_list = [const.COMMERCIAL_RSSD9364, const.COMMERCIAL_RSSD9001, const.YEAR_MERGE, const.FIPS,
                     const.BRANCH_ID_NUM]
    branch_df[keep_col_list].to_pickle(os.path.join(const.TEMP_PATH, '20180910_bank_branch_info.pkl'))
