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
    COMPUSTAT_TA = 'atq'

    FF_MKT_RF = 'Mkt-RF'
    FF_SMB = 'SMB'
    FF_HML = 'HML'
    FF_RF = 'RF'
    FF_MOM = 'Mom'

    ANNOUNCED_DATE = 'Date_Announced'
    YEAR = 'Year_Announced'
    QUARTER = 'Quarter'
    STATUS = 'Status'

    COMPLETED = 'Completed'

    ACQUIRER_CUSIP = 'Acquirer_CUSIP'
    ACQUIRER_TICKER = 'Acquirer_Ticker'
    ACQUIRER_NAME = 'Acquirer_Name'
    ACQUIRER_MVE = 'Acquirer_Market_Value_mil'
    ACQUIRER_LT = 'Acquirer_Total_Liabilities_mil'
    ACQUIRER_NI = 'Acquirer_Net_Income_mil'
    ACQUIRER_TA = 'Acquirer_Total_Assets_mil'
    ACQUIRER_ROA = 'Acquirer_ROA'
    ACQUIRER_TOBINQ = 'Acquirer_TobinQ'

    TARGET_TICKER = 'Target_Ticker_Symbol'
    TARGET_CUSIP = 'Target_CUSIP'
    TARGET_NAME = 'Target_Name'
    TARGET_NI = 'Target_Net_Income_Last_Twelve_Months_mil'
    TARGET_TA = 'Target_Total_Assets_mil'

    LINK_TABLE_RSSD9001 = 'link_table_rssd9001'
    REAL = 'real'

    ACQUIRER = 'Acquirer'
    TARGET = 'Target'

    COMMERCIAL_DATE = 'DATE_VALID'
    COMMERCIAL_RSSD9364 = 'RSSD9364'
    COMMERCIAL_RSSD9001 = 'RSSD9001'
    COMMERCIAL_ID = 'id'

    NET_INTEREST_INCOME = 'RIAD4074'
    TOTAL_ASSETS = 'RCFD2170'
    TOTAL_LIABILITIES = 'RCON2950'
    TOTAL_EQUITY_CAPITAL = 'RCON3210'
    NET_INCOME_LOSS = 'RIAD4340'
    LEVERAGE_RATIO = 'Leverage_Ratio'
    ROA = 'ROA'
    ROE = 'ROE'
