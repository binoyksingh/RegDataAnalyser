#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 21:31:35 2018

@author: lojinilogesparan
"""

import csv
from datetime import datetime

from Modules import FX_HISTDATA_Module
from Modules_DB_Writers import FX_HISTDATA_DB_Writer_Module

fx_histdata_db_writer = FX_HISTDATA_DB_Writer_Module.FX_HIST_DATA_DB_WRITER()

fx_rates_file = "/Users/sarthakagarwal/PycharmProjects/FXRatesData/FX_RATES_31_03_2018.csv"
dateformat = '%d/%m/%Y'  # '%m/%d/%Y for Bank NBI and %d/%m/%Y for Intl'

with open(fx_rates_file, 'rb') as csvfile:
    data = csv.reader(csvfile, delimiter=',', quotechar='|')
    rowCount = 0
    for row in data:
        if rowCount > 0:
            print('Row=' + str(rowCount))

            # ------------------------------
            # Building Table 2
            fx_histdata_rec = FX_HISTDATA_Module.FX_HISTDATA()
            rawdate = datetime.strptime(row[6], dateformat)
            formatted_date = datetime.strftime(rawdate, "%Y-%m-%d")
            fx_histdata_rec.setCurrencyCode(row[0])
            fx_histdata_rec.setCurrencyName(row[1])
            fx_histdata_rec.setBaseCurrencyCode(row[2])
            fx_histdata_rec.setBaseCurrencyName(row[3])
            fx_histdata_rec.setUnitsPerBaseCcy(row[4])
            fx_histdata_rec.setRateDate(formatted_date)


            print fx_histdata_rec.getAttrArray()

            # Writing to Table 2
            fx_histdata_db_writer.Write_FX_HISTDATA(fx_histdata_rec)

        rowCount += 1
