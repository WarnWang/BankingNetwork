#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step09_finish_county_level_data
# @Date: 2018-11-22
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
generate_vars_20181120.step09_finish_county_level_data
"""

import os

import numpy as np
import pandas as pd
from pandas import DataFrame

from constants import Constants as const


if __name__ == '__main__':
    county_sbl_amt_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181122_county_sb_loan_amount.pkl'))
    county_sbl_rate_df: DataFrame = pd.read_pickle(os.path.join(const.TEMP_PATH, '20181122_county_sb_loan_rate.pkl'))
    county_sbl_df = county_sbl_rate_df.merge(county_sbl_amt_df, on=[const.FIPS, const.YEAR], how='outer')

    common_county_event: DataFrame = pd.read_pickle(
        os.path.join(const.TEMP_PATH, '20181120_common_county_event_rssd.pkl'))
    count_event_count = common_county_event.groupby([const.FIPS, const.YEAR]).count().reset_index(drop=False).rename(
        index=str, columns={const.RSSD9001: 'AT_merge_count'})

    merged_df = county_sbl_df.merge(count_event_count, on=[const.FIPS, const.YEAR], how='left')
    merged_df.loc[:, 'AT_merge_count'] = merged_df['AT_merge_count'].fillna(0)
    merged_df.to_pickle(os.path.join(const.TEMP_PATH, '20181122_county_level_sb_loan_data.pkl'))
    merged_df = merged_df.replace({np.inf: np.nan})
    merged_df.to_stata(os.path.join(const.RESULT_PATH, '20181122_county_level_sb_loan_data.dta'), write_index=False)
