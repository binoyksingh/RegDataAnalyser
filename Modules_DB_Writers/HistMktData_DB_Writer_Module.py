#!/usr/bin/python

from Modules_DB_Writers.Generic_DB_Writer_Module_PostGres import Generic_DB_Writer_Postgres

class HistMktData_DB_Writer(Generic_DB_Writer_Postgres):

    def getInsertSQLString(self):

        insert_sql_string = "INSERT INTO hist_market_data (price_date, price_ts,currency_pair,isin,isin_desc,isda_asset_class_id," \
                                  "isda_asset_class_desc,fx_spot_price_open_bid_quote,fx_spot_price_close_bid_quote, mkt_data_detail," \
                                  "source_file_name, source_desc)" \
                                  " VALUES %s"
        return insert_sql_string