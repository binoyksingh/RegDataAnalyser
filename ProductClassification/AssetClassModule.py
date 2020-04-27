#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 21:31:35 2018

@author: agarsar
"""

from enum import Enum

class AssetClass(Enum):

    UNCLASSIFIED = 1
    BLANK_AT_SOURCE = 2
    EQUITY = 3
    FOREIGN_EXCHANGE = 4
    WARRANTS = 5
    FIXED_INCOME = 6
    INTEREST_RATE = 7
    MONEY_MARKET_INSTRUMENTS = 8
    CREDIT = 9
    COMMODITY = 10

    @classmethod
    def getDesc(assetclass):

        '''# Type checking
        if not isinstance(assetclass, AssetClass):
            raise TypeError('AssetClass not recongnised')
        '''
        if (assetclass == AssetClass.UNCLASSIFIED):
            return "Unclassified"

        if (assetclass == AssetClass.BLANK_AT_SOURCE):
            return "Blank at source"

        if (assetclass == AssetClass.EQUITY):
            return "Equity"

        if (assetclass == AssetClass.FOREIGN_EXCHANGE):
            return "Foreign Exchange"

        if (assetclass == AssetClass.WARRANTS):
            return "Warrants"

        if (assetclass == AssetClass.FIXED_INCOME):
            return "Fixed Income"

        if (assetclass == AssetClass.INTEREST_RATE):
            return "Interest Rate"

        if (assetclass == AssetClass.MONEY_MARKET_INSTRUMENTS):
            return "Money Market Instruments"

        if (assetclass == AssetClass.CREDIT):
            return "Credit"

        if (assetclass == AssetClass.COMMODITY):
            return "Commodity"



print(2+3)

