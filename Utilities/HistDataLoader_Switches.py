#!/usr/bin/python

import pymysql, sys
import logging
from decimal import Decimal

class HistDataLoader_Switches:

    LoadFXHistData = "N"
    LoadInterestRatesHistData = "N"
    LoadEquitiesHistData = "N"
    LoadBondHistData = "N"

    def __init__(self, LoadFXHistData, LoadInterestRatesHistData,LoadEquitiesHistData , LoadBondHistData):

        self.LoadFXHistData = LoadFXHistData
        self.LoadInterestRatesHistData = LoadInterestRatesHistData
        self.LoadEquitiesHistData = LoadEquitiesHistData
        self.LoadBondHistData = LoadBondHistData

    def getSwitches(self):
        single_record_array = [self.LoadFXHistData, self.LoadInterestRatesHistData, self.LoadEquitiesHistData, self.LoadBondHistData]

        return single_record_array