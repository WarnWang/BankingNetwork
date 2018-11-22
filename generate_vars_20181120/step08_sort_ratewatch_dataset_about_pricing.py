#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step08_sort_ratewatch_dataset_about_pricing
# @Date: 2018-11-22
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
generate_vars_20181120.step07_sort_county_level_small_business_data_from_cra
"""

import os

import pandas as pd
from pandas import DataFrame

from constants import Constants as const

if __name__ == '__main__':
    loan_institution_df: DataFrame = pd.read_csv(
        os.path.join(const.DATA_PATH, 'RateWatch', 'RW_MasterHistoricalLoanData_082018', 'Loan_InstitutionDetails.txt'),
        sep='|', encoding='latin1')
    loan_institution_df.loc[:, const.FIPS] = loan_institution_df.apply(lambda x: x['STATE_FPS'] * 1000 + x['CNTY_FPS'],
                                                                       axis=1)
    institution_df = loan_institution_df[[const.FIPS, 'ACCT_NBR']].rename(index=str,
                                                                          columns={'ACCT_NBR': 'accountnumber'})

    smb_df_equip: DataFrame = pd.read_stata(os.path.join(const.DATA_PATH, 'RateWatch', 'SmallBusEquip250K.dta'))
    smb_df_equip_rate = smb_df_equip[smb_df_equip['prod_code'] == '00542RATE'].copy()
    smb_df_equip_rate.loc[:, const.SB_LOAN_RATE] = smb_df_equip_rate['applicablemeasurement'].apply(
        lambda x: float(x) / 100)
    smb_df_equip_rate.loc[:, const.YEAR] = smb_df_equip_rate['date'].apply(lambda x: int(x[:4]))

    smb_df_append_fips = smb_df_equip_rate.merge(institution_df, on=['accountnumber'], how='left').drop(
        ['prod_code', 'prod_name', 'date', 'tier_min', 'tier_max', 'applicablemeasurement', 'comments'], axis=1)
    smb_df_append_fips.to_pickle(os.path.join(const.TEMP_PATH, '20181122_sb_loan_data_related.pkl'))

    loan_price_count = smb_df_append_fips.drop(['accountnumber'], axis=1).groupby(
        [const.FIPS, const.YEAR]).mean().reset_index(drop=False)
    loan_price_count.to_pickle(os.path.join(const.TEMP_PATH, '20181122_county_sb_loan_rate.pil'))
