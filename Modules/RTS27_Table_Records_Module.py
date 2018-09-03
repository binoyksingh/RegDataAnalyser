#!/usr/bin/python

import pymysql, sys
from decimal import Decimal

class RTS27_Table2:

    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    VENUE = ""
    INSTRUMENT_NAME = ""
    INSTRUMENT_CLASSIFICATION = ""
    CURRENCY = ""

    def setAttributes(self, SOURCE_COMPANY_NAME, FILENAME,FILE_ID , ISIN, TRADE_DATE, VENUE, INSTRUMENT_NAME, INSTRUMENT_CLASSIFICATION, CURRENCY):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.VENUE = VENUE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION
        self.CURRENCY = CURRENCY

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
            self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setISIN(self, ISIN):
            self.ISIN = ISIN

    def setFileName(self, FILENAME):
            self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate(self, TRADE_DATE):
        self.TRADE_DATE = TRADE_DATE

    def setVenue(self, VENUE):
        self.VENUE = VENUE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

    def setInstrumentClassification(self, INSTRUMENT_CLASSIFICATION):
        self.INSTRUMENT_CLASSIFICATION = INSTRUMENT_CLASSIFICATION

    def setCurrency(self,  CURRENCY):
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
    NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = 0.0
    NUMBER_OF_TRANSACTIONS_EXECUTED = 0
    TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = 0.00
    NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = 0
    NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = 0
    MEDIAN_TRANSACTION_SIZE = 0.00
    MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = 0.0
    NUMBER_OF_DESIGNATED_MARKET_MAKER = 0.0
    CURRENCY = ""

    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setFileName(self, FILENAME):
        self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate (self, TRADE_DATE) :
        self.TRADE_DATE = TRADE_DATE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

    def setNumberOfOrderOrRequestForQuote (self, NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE) :
        if (NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE == ''):
            print "found a blank one"

        if (NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != ""
            and NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != " "
            and NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE != "N/A"
            and self.is_number(NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE)) :
            self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE

        else :
            self.NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE = 0

    def setNumberOfTransactionsExecuted (self, NUMBER_OF_TRANSACTIONS_EXECUTED) :
        if (NUMBER_OF_TRANSACTIONS_EXECUTED != ""
            and NUMBER_OF_TRANSACTIONS_EXECUTED != " "
            and NUMBER_OF_TRANSACTIONS_EXECUTED != "N/A"
            and self.is_number(NUMBER_OF_TRANSACTIONS_EXECUTED)):
            self.NUMBER_OF_TRANSACTIONS_EXECUTED = NUMBER_OF_TRANSACTIONS_EXECUTED
        else :
            self.NUMBER_OF_TRANSACTIONS_EXECUTED = 0

    def setTotalValueOfTransactionsExecuted (self, TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED) :
        if (TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != ""
            and TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != " "
            and TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED != "N/A"
            and self.is_number(TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED)) :
            self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED
        else :
            self.TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED = 0.0

    def setNumberOfOrdersOrRequestCancelledOrWithdrawn (self, NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN) :
        if (NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN !=""
            and NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN !=" "
            and NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN != "N/A"
            and self.is_number(NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN)):
            self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN
        else :
            self.NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN = 0

    def setNumberOfOrdersOrRequestModified (self, NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED) :
        if (NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != ""
            and NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != " "
            and NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED != "N/A"
            and self.is_number(NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED)) :
            self.NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED
        else :
            NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED = 0

    def setMedianTransactionSize(self, MEDIAN_TRANSACTION_SIZE):
        if (MEDIAN_TRANSACTION_SIZE!=""
            and MEDIAN_TRANSACTION_SIZE!=" "
            and MEDIAN_TRANSACTION_SIZE != "N/A"
            and self.is_number(MEDIAN_TRANSACTION_SIZE)):
            self.MEDIAN_TRANSACTION_SIZE = MEDIAN_TRANSACTION_SIZE
        else :
            MEDIAN_TRANSACTION_SIZE = 0.0

    def setMedianSizeOfAllOrdersOrRequestsForQuote(self, MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE):
        if (MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE!=""
            and MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE!=" "
            and MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE != "N/A"
            and self.is_number(MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE)) :
            self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE
        else :
            self.MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE = 0.0

    def setNumberOfDesignatedMarketMaker (self, NUMBER_OF_DESIGNATED_MARKET_MAKER) :
        if (NUMBER_OF_DESIGNATED_MARKET_MAKER != ""
            and NUMBER_OF_DESIGNATED_MARKET_MAKER != " "
            and NUMBER_OF_DESIGNATED_MARKET_MAKER != "N/A"
            and self.is_number(NUMBER_OF_DESIGNATED_MARKET_MAKER)):
            self.NUMBER_OF_DESIGNATED_MARKET_MAKER = NUMBER_OF_DESIGNATED_MARKET_MAKER
        else :
            self.NUMBER_OF_DESIGNATED_MARKET_MAKER = 0

    def setCurrency (self, CURRENCY) :
        self.CURRENCY = CURRENCY

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


class RTS27_Table4:

    # Define list of attributes
    SOURCE_COMPANY_NAME = ""
    FILENAME = ""
    FILE_ID = ""
    ISIN = ""
    TRADE_DATE = ""
    INSTRUMENT_NAME =""
    SIMPLE_AVERAGE_TRANSACTION_PRICE = 0.00
    VOLUME_WEIGHTED_TRANSACTION_PRICE = 0.00
    HIGHEST_EXECUTED_PRICE = 0.00
    LOWEST_EXECUTED_PRICE = 0.00
    CURRENCY = ""

    #def __init__(self) :
    #    print ("calling constructor - table 6")
    
    def is_number(self,s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def setSourceCompanyName(self, SOURCE_COMPANY_NAME):
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME

    def setCurrency(self, CURRENCY):
        self.CURRENCY = CURRENCY

    def setISIN(self, ISIN):
        self.ISIN = ISIN

    def setFileName(self, FILENAME):
        self.FILENAME = FILENAME

    def setFileId(self, FILE_ID):
        self.FILE_ID = FILE_ID

    def setTradeDate(self, TRADE_DATE):
        self.TRADE_DATE = TRADE_DATE

    def setInstrumentName(self, INSTRUMENT_NAME):
        self.INSTRUMENT_NAME = INSTRUMENT_NAME[:255]

    def setSimpleAverageTransactionPrice(self, SIMPLE_AVERAGE_TRANSACTION_PRICE):
        if (SIMPLE_AVERAGE_TRANSACTION_PRICE != "" and SIMPLE_AVERAGE_TRANSACTION_PRICE != "N/A" and self.is_number(SIMPLE_AVERAGE_TRANSACTION_PRICE)):
            self.SIMPLE_AVERAGE_TRANSACTION_PRICE = SIMPLE_AVERAGE_TRANSACTION_PRICE
        else :
            self.SIMPLE_AVERAGE_TRANSACTION_PRICE = 0.0

    def setVolumeWeightedTransactionPrice(self, VOLUME_WEIGHTED_TRANSACTION_PRICE):
        if (VOLUME_WEIGHTED_TRANSACTION_PRICE != "" and VOLUME_WEIGHTED_TRANSACTION_PRICE != "N/A" and self.is_number(VOLUME_WEIGHTED_TRANSACTION_PRICE)):
            self.VOLUME_WEIGHTED_TRANSACTION_PRICE = VOLUME_WEIGHTED_TRANSACTION_PRICE
        else:
            self.VOLUME_WEIGHTED_TRANSACTION_PRICE = 0.0

    def setHighestExecutedPrice(self, HIGHEST_EXECUTED_PRICE):
        if (HIGHEST_EXECUTED_PRICE != "" and HIGHEST_EXECUTED_PRICE!="N/A" and self.is_number(HIGHEST_EXECUTED_PRICE)):
            self.HIGHEST_EXECUTED_PRICE = HIGHEST_EXECUTED_PRICE
        else:
            self.HIGHEST_EXECUTED_PRICE = 0.0

    def setLowestExecutedPrice(self, LOWEST_EXECUTED_PRICE):
        if (LOWEST_EXECUTED_PRICE != "" and LOWEST_EXECUTED_PRICE != "N/A" and self.is_number(LOWEST_EXECUTED_PRICE)):
            self.LOWEST_EXECUTED_PRICE = LOWEST_EXECUTED_PRICE
        else:
            self.LOWEST_EXECUTED_PRICE = 0.0

    # Constructor with params
    def setAttributes(SOURCE_COMPANY_NAME, FILENAME,FILE_ID, ISIN, TRADE_DATE, INSTRUMENT_NAME,
                        SIMPLE_AVERAGE_TRANSACTION_PRICE, VOLUME_WEIGHTED_TRANSACTION_PRICE,
                        HIGHEST_EXECUTED_PRICE, LOWEST_EXECUTED_PRICE, CURRENCY) :
        self.SOURCE_COMPANY_NAME = SOURCE_COMPANY_NAME
        self.FILENAME = FILENAME
        self.FILE_ID = FILE_ID
        self.ISIN = ISIN
        self.TRADE_DATE = TRADE_DATE
        self.INSTRUMENT_NAME = INSTRUMENT_NAME
        self.SIMPLE_AVERAGE_TRANSACTION_PRICE = Decimal(SIMPLE_AVERAGE_TRANSACTION_PRICE)
        self.VOLUME_WEIGHTED_TRANSACTION_PRICE = Decimal(VOLUME_WEIGHTED_TRANSACTION_PRICE)
        self.HIGHEST_EXECUTED_PRICE = Decimal(HIGHEST_EXECUTED_PRICE)
        self.LOWEST_EXECUTED_PRICE = Decimal(LOWEST_EXECUTED_PRICE)
        self.CURRENCY = CURRENCY
        return self

    def getAttrArray(self):
        single_record_array = [ self.SOURCE_COMPANY_NAME,
                                self.FILENAME,
                                self.FILE_ID,
                                self.ISIN,
                                self.TRADE_DATE,
                                self.INSTRUMENT_NAME,
                                self.SIMPLE_AVERAGE_TRANSACTION_PRICE,
                                self.VOLUME_WEIGHTED_TRANSACTION_PRICE,
                                self.HIGHEST_EXECUTED_PRICE,
                                self.LOWEST_EXECUTED_PRICE,
                                self.CURRENCY ]
        return single_record_array