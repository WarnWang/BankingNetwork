#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step1_split_crsp_file
# @Date: 12/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const

if __name__ == '__main__':
    df = pd.read_csv(os.path.join(const.DATA_PATH, 'crsp_stock_data.csv'),
                     usecols=[const.CRSP_CUSIP, const.CRSP_DATE, const.CRSP_TICKER, const.CRSP_PRICE],
                     dtype={
                         const.CRSP_DATE: str, const.CRSP_CUSIP: str, const.CRSP_TICKER: str, const.CRSP_PRICE: np.float
                     })
    df[const.CRSP_DATE] = pd.to_datetime(df[const.CRSP_DATE], format='%Y%m%d')
    df = df[df[const.CRSP_CUSIP] != '32054860']

    df[const.CRSP_CUSIP] = df[const.CRSP_CUSIP].dropna().apply(lambda x: x[:-2])

    # save price file to different files based on ticker info
    df[[const.CRSP_DATE, const.CRSP_TICKER, const.CRSP_PRICE]].dropna(subset=[const.CRSP_TICKER]).groupby(
        const.CRSP_TICKER).apply(lambda x: x.to_pickle(os.path.join(const.TICKER_STOCK_PRICE_PATH,
                                                                    '{}.p'.format(x.iloc[0, 1]))))

    # save price file to different files based on cusip info
    df[[const.CRSP_DATE, const.CRSP_CUSIP, const.CRSP_PRICE]].dropna(subset=[const.CRSP_CUSIP]).groupby(
        const.CRSP_CUSIP).apply(lambda x: x.to_pickle(os.path.join(const.CUSIP_STOCK_PRICE_PATH,
                                                                   '{}.p'.format(x.iloc[0, 1]))))
