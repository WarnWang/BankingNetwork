#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step17_generate_bhc_cra_df
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

FDIC_DATA_PATH = os.path.join(const.DATA_PATH, 'FDIC_data', 'Branch Office Deposits')

if __name__ == '__main__':
    cra_df: DataFrame = pd.read_stata(os.path.join(const.DATA_PATH, 'CRA_Data', '9616exp_discl_table_d11_bankcnty.dta'))
    cra_df.loc[:, const.FIPS] = cra_df.apply(lambda x: int(x['state']) * 1000 + int(x['county']), axis=1)
    cra_df.loc[:, const.SB_LOAN] = cra_df.apply(
        lambda x: x['amt_orig_lt100k'] + x['amt_orig_100_250k'] + x['amt_orig_gt250k'], axis=1)
    cra_df.loc[:, '{}Num'.format(const.SB_LOAN)] = cra_df.apply(
        lambda x: x['num_orig_lt100k'] + x['num_orig_100_250k'] + x['num_orig_gt250k'], axis=1)
    cra_df.loc[:, const.RSSD9001] = cra_df['rssdid'].apply(lambda x: int(x.strip()) if x.isdigit() else np.nan)
    cra_valid_df: DataFrame = cra_df[cra_df[const.RSSD9001].notnull()].copy()

    useful_cols = ['activity_year', const.RSSD9001, const.FIPS, 'num_orig_lt100k', 'amt_orig_lt100k',
                   'num_orig_100_250k', 'amt_orig_100_250k', 'num_orig_gt250k', 'amt_orig_gt250k', const.SB_LOAN,
                   '{}Num'.format(const.SB_LOAN)]
    cra_useful_df = cra_valid_df[useful_cols].rename(index=str, columns={'activity_year': const.YEAR})

    fdic_dfs = []

    for year in range(1996, 2017):
        tmp_path = os.path.join(FDIC_DATA_PATH, 'ALL_{}.csv'.format(year))

        fdic_df = pd.read_csv(tmp_path, encoding='latin-1').rename(index=str, columns={
            'STCNTYBR': const.FIPS, 'YEAR': const.YEAR, 'RSSDHCR': const.COMMERCIAL_RSSD9364, 'RSSDID': const.RSSD9001,
            'DEPSUM': const.TOTAL_DEPOSITS_REAL, 'BRNUM': const.BRANCH_ID})

        fdic_dfs.append(
            fdic_df[[const.YEAR, const.COMMERCIAL_RSSD9364, const.RSSD9001]].drop_duplicates())

    bhc_commercial_link: DataFrame = pd.concat(fdic_dfs, ignore_index=True, sort=False)

    cra_9364_df: DataFrame = cra_useful_df.merge(bhc_commercial_link, on=[const.RSSD9001, const.YEAR], how='left')
    cra_9364_df.loc[:, const.COMMERCIAL_RSSD9364] = cra_9364_df[const.COMMERCIAL_RSSD9364].replace({0: np.nan}).fillna(
        cra_9364_df[const.RSSD9001])
    cra_9364_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_cra_append_rssd9364.pkl'))

    bhc_cra_df: DataFrame = cra_9364_df.drop([const.RSSD9001], axis=1)
    bhc_cra_df_sum: DataFrame = bhc_cra_df.groupby([const.COMMERCIAL_RSSD9364, const.FIPS, const.YEAR],
                                                   group_keys=False).sum().reset_index(drop=False)

    bhc_cra_df_sum.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_cra.pkl'))
