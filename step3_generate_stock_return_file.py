#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step3_generate_stock_return_file
# @Date: 12/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import pathos

from constants import Constants as const


def reformat_file(input_path, save_path):
    df = pd.read_pickle(input_path)
    df = df.pct_change().dropna()
    df.name = const.CRSP_RETURN
    if not df.empty:
        df.to_pickle(save_path)
    return 1


def reformat_cusip_file(file_name):
    return reformat_file(os.path.join(const.CUSIP_STOCK_PRICE_PATH, file_name),
                         os.path.join(const.CUSIP_STOCK_RETURN_PATH, file_name))


def reformat_ticker_file(file_name):
    return reformat_file(os.path.join(const.TICKER_STOCK_PRICE_PATH, file_name),
                         os.path.join(const.TICKER_STOCK_RETURN_PATH, file_name))


if __name__ == '__main__':
    cusip_list = os.listdir(const.CUSIP_STOCK_PRICE_PATH)
    ticker_file_list = os.listdir(const.TICKER_STOCK_PRICE_PATH)

    pool = pathos.multiprocessing.Pool(processes=pathos.multiprocessing.cpu_count() - 2)
    pool.map(reformat_ticker_file, ticker_file_list)
    pool.map(reformat_cusip_file, cusip_list)
