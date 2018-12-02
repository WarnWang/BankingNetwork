#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step19_generate_bhc_county_level_data
# @Date: 2018-12-02
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_vars_20181120.step19_generate_bhc_county_level_data
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    bhc_cra_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_cra.pkl'))
    bhc_county_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_count_td.pkl'))

    bhc_cra_df_valid: DataFrame = bhc_cra_df[bhc_cra_df[const.COMMERCIAL_RSSD9364] != 0]
    bhc_county_df_sbl = bhc_county_df.merge(bhc_cra_df_valid, on=[const.COMMERCIAL_RSSD9364, const.FIPS, const.YEAR],
                                            how='left')
    bhc_county_df_sbl_valid: DataFrame = bhc_county_df_sbl[bhc_county_df_sbl[const.FIPS] > 0]

    bhc_county_df_sbl_valid.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_branch_dataset.pkl'))
    event_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181202_common_county_event.pkl'))
    event_df.loc[:, 'AT_Merge'] = 1
    for key in [const.YEAR, const.FIPS, const.COMMERCIAL_RSSD9364]:
        event_df.loc[:, key] = event_df[key].apply(int)
    final_df = bhc_county_df_sbl_valid.merge(event_df, on=[const.FIPS, const.YEAR, const.COMMERCIAL_RSSD9364],
                                             how='left')
    final_df.loc[:, 'AT_Merge'] = final_df['AT_Merge'].fillna(0)
    final_df.to_pickle(os.path.join(const.TEMP_PATH, '20181202_bhc_county_branch_dataset_add_ind.pkl'))

    final_df.to_stata(os.path.join(const.RESULT_PATH, '20181202_bhc_county_branch_dataset.dta'), write_index=False)
