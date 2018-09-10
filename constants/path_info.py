#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: path_info
# @Date: 11/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os


class PathInfo(object):
    # ROOT_PATH = '/home/zigan/Documents/WangYouan/research/banking'
    ROOT_PATH = '/home/zigan/Documents/wangyouan/research/BankingNetwork'

    DATA_PATH = os.path.join(ROOT_PATH, 'data')
    CUSIP_STOCK_PRICE_PATH = os.path.join(DATA_PATH, 'price_cusip')
    TICKER_STOCK_PRICE_PATH = os.path.join(DATA_PATH, 'price_ticker')

    CUSIP_STOCK_RETURN_PATH = os.path.join(DATA_PATH, 'return_cusip')
    TICKER_STOCK_RETURN_PATH = os.path.join(DATA_PATH, 'return_ticker')

    CUSIP_MARKET_VALUE_PATH = os.path.join(DATA_PATH, 'market_value_cusip')
    TICKER_MARKET_VALUE_PATH = os.path.join(DATA_PATH, 'market_value_ticker')

    TEMP_PATH = os.path.join(ROOT_PATH, 'temp')
    RESULT_PATH = os.path.join(ROOT_PATH, 'result')

    COMMERCIAL_QUARTER_PATH = os.path.join(DATA_PATH, 'commercial', 'commercial_csv')
    COMMERCIAL_YEAR_PATH = os.path.join(DATA_PATH, 'commercial', 'commercial_csv_yearly')

    PSCORE_MATCH_RESULT = os.path.join(TEMP_PATH, 'pscore')


    DISTANCE_PATH = os.path.join(DATA_PATH, 'fips_distance_data')
    POST2010_DISTANCE_FILE = os.path.join(DISTANCE_PATH, 'sf12010countydistancemiles.dta')
    POST2000_DISTANCE_FILE = os.path.join(DISTANCE_PATH, '')
    OLD_DISTANCE_FILE = os.path.join(DISTANCE_PATH, '')
