#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Filename: path_info
# @Date: 11/8/2017
# @Author: Mark Wang
# @Email: wangyouan@gamil.com


import os


class PathInfo(object):
    ROOT_PATH = '/home/zigan/Documents/WangYouan/research/banking'

    DATA_PATH = os.path.join(ROOT_PATH, 'data')
    TEMP_PATH = os.path.join(ROOT_PATH, 'temp')
    RESULT_PATH = os.path.join(ROOT_PATH, 'result')
