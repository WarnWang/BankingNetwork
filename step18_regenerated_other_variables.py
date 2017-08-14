#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step18_regenerated_other_variables
# @Date: 14/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os

import pandas as pd

from constants import Constants as const

ta_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_dr_information.p'))

# Return to Asset, Acquirer_Net_Income_mil/Acquirer_Total_Assets_mil
ta_df['Acquirer_ROA'] = ta_df['Acquirer_ROA'].fillna(ta_df[const.ACQUIRER_NI] / ta_df[const.ACQUIRER_TA])

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_ROA2.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_ROA2.csv'), index=False)

# (Acquirer_Market_Value_mil + Acquirer_Total_Liabilities_mil)/Acquirer_Total_Assets_mil
tobinq = (ta_df[const.ACQUIRER_MVE] + ta_df[const.ACQUIRER_LT]) / ta_df[const.ACQUIRER_TA]
ta_df['Acquirer_TobinQ'] = ta_df[const.ACQUIRER_TOBINQ].fillna(tobinq)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TobinQ2.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_TobinQ2.csv'), index=False)

# Value_of_Transaction_mil/Acquirer_Total_Assets_mil
adj_deal_size = ta_df['Value_of_Transaction_mil'] / ta_df[const.ACQUIRER_TA]
ta_df['Deal_Size_Adjusted_by_Asset'] = ta_df['Deal_Size_Adjusted_by_Asset'].fillna(adj_deal_size)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_deal_size2.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_deal_size2.csv'), index=False)

# Acquirer_Total_Assets_mil/Target_Total_Assets_mil
asset_ratio = ta_df[const.ACQUIRER_TA] / ta_df[const.TARGET_TA]
ta_df['Acq_Tar_Asset_Ratio'] = ta_df['Acq_Tar_Asset_Ratio'].fillna(asset_ratio)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TA_Ratio2.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_TA_Ratio2.csv'), index=False)

# Acquirer_Net_Income_mil/Target_Net_Income_Last_Twelve_Months_mil
ni_ratio = ta_df[const.ACQUIRER_NI] / ta_df[const.TARGET_NI]
ta_df['Acq_Tar_Net_Income_Ratio'] = ta_df['Acq_Tar_Net_Income_Ratio'].fillna(ni_ratio)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_NI_Ratio2.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_NI_Ratio2.csv'), index=False)
