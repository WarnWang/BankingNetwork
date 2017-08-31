#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: step2_try_to_do_pscore
# @Date: 31/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com

import os

import pandas as pd

from constants import Constants as const

df = pd.read_excel(os.path.join(const.DATA_PATH, '20170829_CAR_Control_Ind_IV12_DirExe.xlsx'))

"""
ROA_call ROA
ROE_call ROE
"""
