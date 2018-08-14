#!/usr/bin/python

import pymysql, sys
from decimal import Decimal

class RTS27_Table2:

    def __init__(self, SOURCE_COMPANY_NAME, FILENAME,FILE_ID , ISIN, TRADE_DATE, VENUE, INSTRUMENT_NAME, INSTRUMENT_CLASSIFICATION, CURRENCY):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.VENUE = VENUE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION
        self.CURRENCY = CURRENCY

    def getAttrArray(self):
        single_record_array = [self.SOURCE_COMPANY_NAME, self.FILENAME, self.FILE_ID, self.ISIN, self.TRADE_DATE,
                                       self.VENUE, self.INSTRUMENT_NAME, self.INSTRUMENT_CLASSIFICATION, self.CURRENCY]
        return single_record_array

class RTS27_Table6:

    # Define list of attributes
    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    INSTRUMENT_NAME =""
    NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = ""
    NUMBER_OF_TRANSACTIONS_EXECUTED = 0
    TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = 0.00
    NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = 0
    NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = 0
    MEDIAN_TRANSACTION_SIZE = 0.00
    MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = 0.0
    NUMBER_OF_DESIGNATED_MARKET_MAKER = 0.0
    CURRENCY = ""

    def __init__(self) :
        print ("calling constructor")

    # Constructor with params
    def setAttributes(SOURCE_COMPANY_NAME, FILENAME,FILE_ID, ISIN, TRADE_DATE, INSTRUMENT_NAME,
                 NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE, NUMBER_OF_TRANSACTIONS_EXECUTED,
                 TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED, NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN,
                 NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED, MEDIAN_TRANSACTION_SIZE,
                 MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE, NUMBER_OF_DESIGNATED_MARKET_MAKER, CURRENCY):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = int(NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE)
        self.NUMBER_OF_TRANSACTIONS_EXECUTED = int(NUMBER_OF_TRANSACTIONS_EXECUTED)
        self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = Decimal(TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED)
        self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = int(NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN)
        self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = int(NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED)
        self.MEDIAN_TRANSACTION_SIZE = Decimal(MEDIAN_TRANSACTION_SIZE)
        self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = Decimal(MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE)
        self.NUMBER_OF_DESIGNATED_MARKET_MAKER = int(NUMBER_OF_DESIGNATED_MARKET_MAKER.replace('NULL','0'))
        self.CURRENCY = CURRENCY
        return self

    def getAttrArray(self):
        single_record_array = [ self.SOURCE_COMPANY_NAME,
                                self.FILENAME,
                                self.FILE_ID,
                                self.ISIN,
                                self.TRADE_DATE,
                                self.INSTRUMENT_NAME,
                                self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE,
                                self.NUMBER_OF_TRANSACTIONS_EXECUTED,
                                self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED,
                                self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN,
                                self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED,
                                self.MEDIAN_TRANSACTION_SIZE,
                                self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE,
                                self.NUMBER_OF_DESIGNATED_MARKET_MAKER,
                                self.CURRENCY ]
        return single_record_array