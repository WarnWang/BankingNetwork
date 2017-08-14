#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step16_sort_dr_wang_data.py
# @Date: 14/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const


def get_information(df):
    result_dict = {}
    for key in [const.ACQUIRER_MVE, const.COMPUSTAT_TIC, const.COMPUSTAT_NET_INCOME, const.COMPUSTAT_LIABILITY,
                const.COMPUSTAT_LIABILITY]:
        series = df[key].dropna()
        if series.empty:
            result_dict[key] = np.nan

        else:
            result_dict[key] = series.iloc[0]

    return pd.Series(result_dict)


if __name__ == '__main__':
    df = pd.read_stata(os.path.join(const.DATA_PATH, '20160504CAR_198_2014_bhc.dta'))
    df = df.rename(index=str, columns={'Year': const.YEAR, 'DateAnnounced': const.ANNOUNCED_DATE,
                                       'AcquirerPrimaryTicker': const.ACQUIRER_TICKER,
                                       'TargetPrimaryTicker': const.TARGET_TICKER,
                                       'AcquirerCUSIP': const.ACQUIRER_CUSIP,
                                       'TargetCUSIP': const.TARGET_CUSIP,
                                       'NetIncome_Tar': const.TARGET_NI,
                                       'Total_Assets_Tar': const.TARGET_TA,
                                       'NetIncome_Acq': const.ACQUIRER_NI,
                                       'Acquirer_Total_Assets_mil_Combin': const.ACQUIRER_TA,
                                       'AcquirerMarketCap': const.ACQUIRER_MVE,
                                       'TotalLiabilities_Acq': const.ACQUIRER_LT,
                                       'AcquirerROA': const.ACQUIRER_ROA,
                                       'AcquirerTobinQ': const.ACQUIRER_TOBINQ
                                       })

    df = df.dropna(subset=[const.ANNOUNCED_DATE, const.YEAR], how='all')
    df[const.YEAR] = df[const.YEAR].fillna(
        df[df[const.YEAR].isnull()][const.ANNOUNCED_DATE].apply(lambda x: x.year)).apply(int)
    df[const.ACQUIRER_CUSIP] = df[const.ACQUIRER_CUSIP].dropna().apply(lambda x: str(x).zfill(6))
    df[const.TARGET_CUSIP] = df[const.TARGET_CUSIP].dropna().apply(lambda x: str(x).zfill(6))

    df.to_pickle(os.path.join(const.DATA_PATH, '20170814_dr_wang_previous_result.p'))

    # remain useful information
    acq_df = df[[const.YEAR, const.ACQUIRER_MVE, const.ACQUIRER_NI, const.ACQUIRER_TA, const.ACQUIRER_CUSIP,
                 const.ACQUIRER_TICKER, const.ACQUIRER_LT, const.ACQUIRER_ROA, const.ACQUIRER_TOBINQ]]
    tar_df = df[[const.YEAR, const.TARGET_CUSIP, const.TARGET_TICKER, const.TARGET_NI, const.TARGET_TA]]

    # use compustat variables as intermedia variables
    acq_df = acq_df.rename(index=str, columns={const.ACQUIRER_NI: const.COMPUSTAT_NET_INCOME,
                                               const.ACQUIRER_TA: const.COMPUSTAT_TA,
                                               const.ACQUIRER_LT: const.COMPUSTAT_LIABILITY,
                                               const.ACQUIRER_TICKER: const.COMPUSTAT_TIC,
                                               const.ACQUIRER_CUSIP: const.COMPUSTAT_CUSIP})
    tar_df = tar_df.rename(index=str, columns={const.TARGET_CUSIP: const.COMPUSTAT_CUSIP,
                                               const.TARGET_TICKER: const.COMPUSTAT_TIC,
                                               const.TARGET_TA: const.COMPUSTAT_TA,
                                               const.TARGET_NI: const.COMPUSTAT_NET_INCOME})

    merged_df = pd.concat([acq_df, tar_df], ignore_index=True, axis=0)

    cleaned_df = merged_df.groupby([const.YEAR, const.COMPUSTAT_CUSIP]).apply(get_information).dropna(
        subset=['ltq', 'niq', 'Acquirer_Market_Value_mil'], how='all').reset_index()
    cleaned_df.to_pickle(os.path.join(const.TEMP_PATH, '20170814_dr_wang_previous_result.p'))
