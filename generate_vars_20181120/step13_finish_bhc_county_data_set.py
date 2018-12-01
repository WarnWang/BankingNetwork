#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step13_finish_bhc_county_data_set
# @Date: 2018-12-01
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step13_finish_bhc_county_data_set
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    county_branch_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181128_county_branch_status_count.pkl'))
    county_branch_df.loc[:, const.RSSD9001] = county_branch_df[const.RSSD9001].apply(lambda x: str(int(x)))
    county_branch_df[const.FIPS] = county_branch_df[const.FIPS].astype(int)
    county_branch_df[const.YEAR] = county_branch_df[const.YEAR].astype(int)

    cra_df: DataFrame = pd.read_stata(os.path.join(const.DATA_PATH, 'CRA_Data', '9616exp_discl_table_d11_bankcnty.dta'))
    cra_df.loc[:, const.FIPS] = cra_df.apply(lambda x: int(x['state']) * 1000 + int(x['county']), axis=1)
    cra_df.loc[:, const.SB_LOAN] = cra_df.apply(
        lambda x: x['amt_orig_lt100k'] + x['amt_orig_100_250k'] + x['amt_orig_gt250k'], axis=1)
    cra_df.loc[:, '{}Num'.format(const.SB_LOAN)] = cra_df.apply(
        lambda x: x['num_orig_lt100k'] + x['num_orig_100_250k'] + x['num_orig_gt250k'], axis=1)
    cra_df.loc[:, const.RSSD9001] = cra_df['rssdid'].apply(lambda x: str(int(x.strip())) if x.isdigit() else np.nan)
    cra_valid_df: DataFrame = cra_df[cra_df[const.RSSD9001].notnull()].copy()
    cra_valid_df = cra_valid_df[cra_valid_df[const.RSSD9001] != '0'].copy()

    useful_cols = ['activity_year', const.RSSD9001, const.FIPS, 'num_orig_lt100k', 'amt_orig_lt100k',
                   'num_orig_100_250k', 'amt_orig_100_250k', 'num_orig_gt250k', 'amt_orig_gt250k', const.SB_LOAN,
                   '{}Num'.format(const.SB_LOAN)]
    cra_useful_df = cra_valid_df[useful_cols].rename(index=str, columns={'activity_year': const.YEAR})

    cra_useful_df[const.YEAR] = cra_useful_df[const.YEAR].astype(int)

    bhc_county_cra_df: DataFrame = county_branch_df.merge(cra_useful_df, how='left',
                                                          on=[const.RSSD9001, const.YEAR, const.FIPS])

    county_event_df: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))
    county_event_df.loc[:, 'AT_merge_count'] = 1
    county_event_df['year'] = county_event_df['year'].astype(int)
    county_event_df[const.FIPS] = county_event_df[const.FIPS].astype(int)
    bhc_county_cra_df_event: DataFrame = bhc_county_cra_df.merge(county_event_df, how='left',
                                                                 on=[const.RSSD9001, const.YEAR, const.FIPS])
    bhc_county_cra_df_event.loc[:, 'AT_merge_count'] = bhc_county_cra_df_event['AT_merge_count'].fillna(0)
    bhc_county_cra_df_event.to_pickle(os.path.join(const.TEMP_PATH, '20181201_bhc_county_cra_df_failure_result.pkl'))
