#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step6_prepare_market_capital_data
# @Date: 12/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

""" This file is used to calculate market capital data, the unit is million """

import os

import pandas as pd
import numpy as np

from constants import Constants as const


def save_file(df, save_path):
    tmp_df = df.set_index(const.CRSP_DATE)
    save_name = tmp_df.iloc[0, 0]
    mve_series = tmp_df[const.CRSP_MVE]
    mve_series.to_pickle(os.path.join(save_path, save_name))


def save_file_cusip(df):
    return save_file(df[[const.CRSP_DATE, const.CRSP_CUSIP, const.CRSP_MVE]], const.CUSIP_MARKET_VALUE_PATH)


def save_file_ticker(df):
    return save_file(df[[const.CRSP_DATE, const.CRSP_TICKER, const.CRSP_MVE]], const.TICKER_MARKET_VALUE_PATH)


if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.DATA_PATH, 'stock_price_cshoc.csv'),
                     usecols=[const.CRSP_DATE, const.CRSP_TICKER, const.CRSP_PRICE,
                              const.CRSP_SHROUT, const.CRSP_CUSIP],
                     dtype={const.CRSP_DATE: str, const.CRSP_SHROUT: np.float64, const.CRSP_CUSIP: str,
                            const.CRSP_PRICE: np.float64, const.CRSP_TICKER: str}
                     )
    df = df.dropna(subset=[const.CRSP_SHROUT, const.CRSP_PRICE], how='any')

    df[const.CRSP_DATE] = pd.to_datetime(df[const.CRSP_DATE], format='%Y%m%d')
    df[const.CRSP_PRICE] = df[const.CRSP_PRICE].apply(abs)

    df[const.CRSP_MVE] = df[const.CRSP_PRICE] * df[const.CRSP_SHROUT] / 1000

    mve_df = df[[const.CRSP_DATE, const.CRSP_TICKER, const.CRSP_CUSIP, const.CRSP_MVE]]
    mve_df.to_pickle(os.path.join(const.TEMP_PATH, '20170812_market_value.p'))
    # mve_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20170812_market_value.p'))

    mve_df.dropna(subset=[const.CRSP_TICKER]).groupby(const.CRSP_TICKER).apply(save_file_ticker)
    mve_df.dropna(subset=[const.CRSP_CUSIP]).groupby(const.CRSP_CUSIP).apply(save_file_cusip)
