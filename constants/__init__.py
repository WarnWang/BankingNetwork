#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: __init__.py
# @Date: 11/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


from constants.path_info import PathInfo


class Constants(PathInfo):
    CRSP_TICKER = 'TICKER'
    CRSP_DATE = 'date'
    CRSP_CUSIP = 'CUSIP'
    CRSP_PRICE = 'PRC'
    CRSP_RETURN = 'Return'
    CRSP_SHROUT = 'SHROUT'

    # market value of common equity (MVE)
    CRSP_MVE = 'MVE'

    COMPUSTAT_DATE = 'datadate'
    COMPUSTAT_QUARTER = 'datacqtr'
    COMPUSTAT_LIABILITY = 'ltq'
    COMPUSTAT_CUSIP = 'cusip'
    COMPUSTAT_TIC = 'tic'
    COMPUSTAT_NET_INCOME = 'niq'

    FF_MKT_RF = 'Mkt-RF'
    FF_SMB = 'SMB'
    FF_HML = 'HML'
    FF_RF = 'RF'
    FF_MOM = 'Mom'

    ACQUIRER_CUSIP = 'Acquirer_CUSIP'
    ACQUIRER_TICKER = 'Acquirer_Ticker'
    ANNOUNCED_DATE = 'Date_Announced'
    ACQUIRER_MVE = 'Acquirer_Market_Value_mil'
    ACQUIRER_LT = 'Acquirer_Total_Liabilities_mil'
    ACQUIRER_NI = 'Acquirer_Net_Income_mil'
    ACQUIRER_TA = 'Acquirer_Total_Assets_mil'

    TARGET_TICKER = 'Target_Ticker_Symbol'
    TARGET_CUSIP = 'Target_CUSIP'
    TARGET_NI = 'Target_Net_Income_Last_Twelve_Months_mil'

    ACQUIRER = 'Acquirer'
    TARGET = 'Target'
