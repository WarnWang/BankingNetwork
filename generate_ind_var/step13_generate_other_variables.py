#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step13_generate_other_variables
# @Date: 13/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

ta_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TA.p'))

# Return to Asset, Acquirer_Net_Income_mil/Acquirer_Total_Assets_mil
ta_df['Acquirer_ROA'] = ta_df[const.ACQUIRER_NI] / ta_df[const.ACQUIRER_TA]

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_ROA.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_ROA.csv'), index=False)

# (Acquirer_Market_Value_mil + Acquirer_Total_Liabilities_mil)/Acquirer_Total_Assets_mil
ta_df['Acquirer_TobinQ'] = (ta_df[const.ACQUIRER_MVE] + ta_df[const.ACQUIRER_LT]) / ta_df[const.ACQUIRER_TA]

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TobinQ.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_TobinQ.csv'), index=False)

# Value_of_Transaction_mil/Acquirer_Total_Assets_mil
ta_df['Deal_Size_Adjusted_by_Asset'] = ta_df['Value_of_Transaction_mil'] / ta_df[const.ACQUIRER_TA]

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_deal_size.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_deal_size.csv'), index=False)

# equals 1 if Attitude == "Friendly" and equals 0 otherwise
ta_df['Attitude_Dummy'] = (ta_df['Attitude'] == 'Friendly').apply(int)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_attitude.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_attitude.csv'), index=False)

# equals 1 if Target_Public == "Public" and equals 0 otherwise
ta_df['Target_Public_Dummy'] = (ta_df['Target_Public'] == 'Public').apply(int)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_target_public.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_target_public.csv'), index=False)

# Acquirer_Total_Assets_mil/Target_Total_Assets_mil
ta_df['Acq_Tar_Asset_Ratio'] = ta_df[const.ACQUIRER_TA] / ta_df[const.TARGET_TA]

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_TA_Ratio.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_TA_Ratio.csv'), index=False)

# Acquirer_Net_Income_mil/Target_Net_Income_Last_Twelve_Months_mil
ta_df['Acq_Tar_Net_Income_Ratio'] = ta_df[const.ACQUIRER_NI] / ta_df[const.TARGET_NI]

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_NI_Ratio.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_NI_Ratio.csv'), index=False)

# equals 1 if Perc_of_Cash == 100, otherwise equals 0 (including if Perc_of_Cash is missing)
ta_df['Cash_Deal_Dummy'] = (ta_df['Perc_of_Cash'] == 100).apply(int)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_cash_dummy.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_cash_dummy.csv'), index=False)

# equals 1 if Perc_of_Stock == 100, otherwise equals 0 (including if Perc_of_Stock is missing)
ta_df['Stock_Deal_Dummy'] = (ta_df['Perc_of_Stock'] == 100).apply(int)

ta_df.to_pickle(os.path.join(const.TEMP_PATH, '20170813_SDC_MnA_fill_in_stock_dummy.p'))
ta_df.to_csv(os.path.join(const.RESULT_PATH, '20170813_SDC_MnA_fill_in_stock_dummy.csv'), index=False)
