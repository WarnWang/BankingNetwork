#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step5_generate_runup
# @Date: 12/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd
import numpy as np

from constants import Constants as const


def calculate_runup(stock_symbol, event_date, period_start_days=-210, period_end_days=-11):
    """
        Bidder’s buy-and-hold abnormal return (BHAR) during the period (−210, −11). The market index is the CRSP
        value-weighted return.
    """

    if period_start_days > period_end_days:
        return np.nan, 'Invalid period days'

    ff_4_factor_df = pd.read_pickle(os.path.join(const.DATA_PATH,
                                                 'fama_french_3factors_mom', '4_factors.p'))[const.FF_MKT_RF]

    if os.path.isfile(os.path.join(const.CUSIP_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol))):
        stock_info = 'cusip'
        stock_price = pd.read_pickle(os.path.join(const.CUSIP_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol)))
    elif os.path.isfile(os.path.join(const.TICKER_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol))):
        stock_info = 'ticker'
        stock_price = pd.read_pickle(os.path.join(const.TICKER_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol)))
    else:
        return np.nan, 'stock_price_not_find'

    data_df = pd.concat([stock_price, ff_4_factor_df], axis=1, join='inner')
    if data_df.empty:
        return np.nan, 'Not enough data'

    trading_days = data_df.index

    if data_df.empty or trading_days[-1] < event_date:
        return np.nan, 'Not enough data'
    event_trading_date = trading_days[trading_days >= event_date][0]
    post_event_days = trading_days[trading_days > event_trading_date]
    before_event_days = trading_days[trading_days < event_trading_date]

    if len(before_event_days) < -period_start_days or len(post_event_days) < period_end_days:
        return np.nan, 'Not enough date to calculate'

    def get_detail_date(index):
        if index < 0:
            return before_event_days[index]
        else:
            return post_event_days[index - 1]

    event_start_date = get_detail_date(period_start_days)
    event_end_date = get_detail_date(period_end_days)

    s_pct = (stock_price.loc[event_end_date] - stock_price.loc[event_start_date]) / stock_price.loc[event_start_date]
    m_pct = (ff_4_factor_df.loc[event_end_date]
             - ff_4_factor_df.loc[event_start_date]) / ff_4_factor_df.loc[event_start_date]

    return s_pct - m_pct, stock_info


def calculate_runup_info(row):
    acq_cusip = row[const.ACQUIRER_CUSIP]
    acq_ticker = row[const.ACQUIRER_TICKER]

    tar_cusip = row[const.TARGET_CUSIP]
    tar_ticker = row[const.TARGET_TICKER]

    event_data = row[const.ANNOUNCED_DATE]

    result_dict = {}
    for model in [const.TARGET, const.ACQUIRER]:
        if model == const.TARGET:
            cusip = tar_cusip
            ticker = tar_ticker
        else:
            cusip = acq_cusip
            ticker = acq_ticker

        car_value, description = calculate_runup(cusip, event_date=event_data,
                                                 period_start_days=-210, period_end_days=-11)
        key = '{}_RunUp'.format(model)
        if np.isnan(car_value):
            car_value, description = calculate_runup(ticker, event_date=event_data,
                                                     period_start_days=-210, period_end_days=-11)
        result_dict[key] = car_value

    return pd.Series(result_dict)


if __name__ == '__main__':
    # this step is used to format cusip data
    mna_df = pd.read_pickle(os.path.join(const.TEMP_PATH, '20180812_SDC_MnA_add_car_1986_2016.p'))
    car_df = mna_df.merge(mna_df.apply(calculate_runup_info, axis=1), left_index=True, right_index=True)
    car_df.to_pickle(os.path.join(const.TEMP_PATH, '20180812_SDC_MnA_add_runup_1986_2016.p'))
    car_df.to_csv(os.path.join(const.RESULT_PATH, '20180812_SDC_MnA_add_runup_1986_2016.csv'), index=False)
