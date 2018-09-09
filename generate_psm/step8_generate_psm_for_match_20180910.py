#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step8_generate_psm_for_match_20180910
# @Date: 9/9/2018
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

"""
python3 -m generate_psm.step8_generate_psm_for_match_20180910
"""

import os

import pandas as pd

from constants import Constants as const

PSM_COV_LIST = [const.INTEREST_INCOME_RATIO, const.TOTAL_ASSETS, const.NET_INCOME_LOSS, const.LEVERAGE_RATIO, const.ROA]

if __name__ == '__main__':
    psm_data = pd.read_stata(os.path.join(const.DATA_PATH, '20180908_revision', '20180908_psm_add_missing_rssd.dta'))
    real_psm_data = psm_data[(psm_data['Target_real'] == 1) & (psm_data['Acquirer_real'] == 1)]

    psm_group = real_psm_data.groupby([const.QUARTER, const.YEAR])

