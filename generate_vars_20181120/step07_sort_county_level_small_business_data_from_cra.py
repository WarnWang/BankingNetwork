#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step07_sort_county_level_small_business_data_from_cra
# @Date: 2018-11-22
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
generate_vars_20181120.step07_sort_county_level_small_business_data_from_cra
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    cra_df: DataFrame = pd.read_stata(os.path.join(const.DATA_PATH, 'CRA_Data', '9616exp_discl_table_d11_bankcnty.dta'))
    cra_df.loc[:, const.FIPS] = cra_df.apply(lambda x: int(x['state']) * 1000 + int(x['county']), axis=1)
    cra_df.loc[:, const.SB_LOAN] = cra_df.apply(
        lambda x: x['amt_orig_lt100k'] + x['amt_orig_100_250k'] + x['amt_orig_gt250k'], axis=1)
    cra_df.loc[:, const.RSSD9001] = cra_df['rssdid'].apply(lambda x: str(int(x.strip())) if x.isdigit() else np.nan)
    cra_df_renamed = cra_df.rename(index=str, columns={'activity_year': const.YEAR})[[
        const.SB_LOAN, const.FIPS, const.RSSD9001, const.YEAR]].copy()
    cra_df_renamed.to_pickle(os.path.join(const.TEMP_PATH, '20181122_cra_data_set.pkl'))

    county_sbl_amt_df = cra_df_renamed.drop([const.RSSD9001], axis=1).groupby(
        [const.FIPS, const.YEAR], group_keys=False).sum().reset_index(drop=False)
    county_sbl_amt_df.to_pickle(os.path.join(const.TEMP_PATH, '20181122_county_sb_loan_amount.pkl'))
