#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step4_generate_car
# @Date: 12/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


""" Here CAR means cumulative abnormal return """

import os

import pandas as pd
import numpy as np
import statsmodels.api as sm

from constants import Constants as const

ff_4_factor_df = pd.read_pickle(os.path.join(const.DATA_PATH, 'fama_french_3factors_mom', '4_factors.p'))


def car(stock_symbol, event_date, factor_number, period_start_days, period_end_days):
    """
    To calculate the cumulative abnormal return.
    Input stock ticker, event date, factor number, and test interval.
    Factor number must be 1, 3 or 4. The default value of test interval is set to 5.
    @:param stock_symbol could be cusip or stock ticker

    if calculate CAR, period_start_days should be -2 and period_end_days should be 2
    if calculate RUN up, period_start_days should be -210 and period_end_days should be -11
    """
    if factor_number not in {1, 3, 4}:
        # print('Factor number must be 1, 3, 4')
        return np.nan, 'Invalid factor number'

    if period_start_days > period_end_days:
        return np.nan, 'Invalid period days'

    useful_col = [const.FF_MKT_RF, const.FF_SMB, const.FF_HML, const.FF_MOM][:factor_number]
    ff_factor_df = ff_4_factor_df[useful_col]
    if os.path.isfile(os.path.join(const.CUSIP_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol))):
        stock_info = 'cusip'
        stock_price = pd.read_pickle(os.path.join(const.CUSIP_STOCK_RETURN_PATH, '{}.p'.format(stock_symbol)))
    elif os.path.isfile(os.path.join(const.TICKER_STOCK_PRICE_PATH, '{}.p'.format(stock_symbol))):
        stock_info = 'ticker'
        stock_price = pd.read_pickle(os.path.join(const.TICKER_STOCK_RETURN_PATH, '{}.p'.format(stock_symbol)))
    else:
        return np.nan, 'stock_price_not_find'

    data = pd.merge(ff_factor_df, pd.DataFrame(stock_price), right_index=True, left_index=True, how='inner')
    trading_days = data.index

    if data.empty or trading_days[-1] < event_date:
        return np.nan, 'Not enough data'
    event_trading_date = trading_days[trading_days >= event_date][0]
    post_event_days = trading_days[trading_days > event_trading_date]
    before_event_days = trading_days[trading_days < event_trading_date]

    if len(before_event_days) < -period_start_days or len(post_event_days) < period_end_days:
        return np.nan, 'Not enough date to calculate'

    if len(before_event_days) <= 40:
        return np.nan, 'Not enough training data'

    elif len(before_event_days) < 210:
        training_data = data.loc[before_event_days[:-10]]

    else:
        training_data = data.loc[before_event_days[-210:-10]]

    def get_detail_date(index):
        if index < 0:
            return before_event_days[index]
        else:
            return post_event_days[index - 1]

    event_start_date = get_detail_date(period_start_days)
    event_end_date = get_detail_date(period_end_days)

    testing_data = data.loc[event_start_date:event_end_date, useful_col]
    olsmd = sm.OLS(training_data[const.CRSP_RETURN], training_data[useful_col])
    olsres = olsmd.fit()
    CAR = sum(data.loc[event_start_date:event_end_date, const.CRSP_RETURN] - olsres.predict(testing_data))
    return CAR, stock_info


def calculate_car_info(row):
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

        for factor_number in [1, 3, 4]:
            car_value, description = car(cusip, event_date=event_data, factor_number=factor_number,
                                         period_start_days=-2, period_end_days=2)
            key = '{}_CAR_{}Factor'.format(model, factor_number)
            if np.isnan(car_value):
                car_value, description = car(ticker, event_date=event_data, factor_number=factor_number,
                                             period_start_days=-2, period_end_days=2)
            result_dict[key] = car_value

    return pd.Series(result_dict)


if __name__ == '__main__':
    # this step is used to format cusip data
    mna_df = pd.read_excel(os.path.join(const.DATA_PATH, '20170808_SDC_MnA_clean_1986_2016.xlsx'))
    mna_df[const.ACQUIRER_CUSIP] = mna_df[const.ACQUIRER_CUSIP].dropna().apply(str)
    mna_df[const.TARGET_CUSIP] = mna_df[const.TARGET_CUSIP].dropna().apply(str)
    mna_df.to_pickle(os.path.join(const.TEMP_PATH, '20180812_SDC_MnA_clean_1986_2016.p'))

    car_df = mna_df.merge(mna_df.apply(calculate_car_info, axis=1), left_index=True, right_index=True)
    car_df.to_pickle(os.path.join(const.TEMP_PATH, '20180812_SDC_MnA_add_car_1986_2016.p'))
    car_df.to_csv(os.path.join(const.RESULT_PATH, '20180812_SDC_MnA_add_car_1986_2016.csv'), index=False)
