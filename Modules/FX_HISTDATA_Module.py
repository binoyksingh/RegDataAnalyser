#!/usr/bin/python

import sys

class FX_HISTDATA:

    # Define list of attributes
    CURRENCY_CODE=""
    CURRENCY_NAME=""
    BASE_CURRENCY_CODE=""
    BASE_CURRENCY_NAME=""
    UNITS_PER_BASE_CCY=0.00
    BASE_CCY_PER_UNIT=0.00
    RATE_DATE=""

    # def __init__(self) :
    #    print ("calling constructor - table 6")

    def setCurrencyCode(self, CURRENCY_CODE):
        self.CURRENCY_CODE = CURRENCY_CODE

    def setCurrencyName(self, CURRENCY_NAME):
        self.CURRENCY_NAME = CURRENCY_NAME

    def setBaseCurrencyCode(self, BASE_CURRENCY_CODE):
        self.BASE_CURRENCY_CODE = BASE_CURRENCY_CODE

    def setBaseCurrencyName(self, BASE_CURRENCY_NAME):
        self.BASE_CURRENCY_NAME = BASE_CURRENCY_NAME

    def setUnitsPerBaseCcy(self, UNITS_PER_BASE_CCY):
        self.UNITS_PER_BASE_CCY = UNITS_PER_BASE_CCY

    def setRateDate(self, RATE_DATE):
        self.RATE_DATE = RATE_DATE

    def getAttrArray(self):
        single_record_array = [self.CURRENCY_CODE,
                               self.CURRENCY_NAME,
                               self.BASE_CURRENCY_CODE,
                               self.BASE_CURRENCY_NAME,
                               self.UNITS_PER_BASE_CCY,
                               self.RATE_DATE]
        return single_record_array