#!/usr/bin/python

from Modules_DB_Writers.Generic_DB_Writer_Module import Generic_DB_Writer

class FIRDS_DB_Writer(Generic_DB_Writer):

    def getInsertSQLString(self):

        insert_sql_string = "INSERT INTO `MIFID_FIRDS` (`INSTRUMENT_IDENTIFICATION_CODE`, `INSTRUMENT_FULL_NAME`,`INSTRUMENT_CLASSIFICATION`,`ISSUER_IDENTIFIER`,`TRADING_VENUE`," \
                            "`INSTRUMENT_SHORT_NAME`,`TERMINATION_DATE`,`NOTIONAL_CURRENCY1`,`EXPIRY_DATE`," \
                            "`DELIVERY_TYPE`,`FX_TYPE`,`UNDERLYING_ISIN`,`OPTION_TYPE`," \
                            "`STRIKE_PRICE_AMT`,`OPTION_EXERCISE_STYLE`,`RELEVANT_COMPETENT_AUTHORITY`," \
                            "`DEBT_TOTAL_ISSUED_NOMINAL_AMOUNT`,`DEBT_MATURTY_DATE`,`DEBT_FIXED_RATE`,`FR_DATE`," \
                            "`FILE_NAME`) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s )"
        return insert_sql_string


