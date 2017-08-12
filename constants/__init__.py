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

    FF_MKT_RF = 'Mkt-RF'
    FF_SMB = 'SMB'
    FF_HML = 'HML'
    FF_RF = 'RF'
    FF_MOM = 'Mom'

    ACQUIRER_CUSIP = 'Acquirer_CUSIP'
    ACQUIRER_TICKER = 'Acquirer_Ticker'
    TARGET_TICKER = 'Target_Ticker'
    TARGET_CUSIP = 'Target_CUSIP'
    ANNOUNCED_DATE = 'Date_Announced'

    ACQUIRER = 'Acquirer'
    TARGET = 'Target'
