use mifid;
DROP TABLE MIFID_RTS27_TABLE3;
CREATE TABLE MIFID_RTS27_TABLE3 (
    ENTRY_ID INT NOT NULL AUTO_INCREMENT primary key,
    SOURCE_COMPANY_NAME VARCHAR(255),
    FILENAME VARCHAR(255),
    FILE_ID varchar(255),
    ISIN varchar(255),
    TRADE_DATE date,
    INSTRUMENT_NAME varchar(255),
    CURRENCY_PAIR varchar(255),
    CURRENCY varchar(255),
    TENOR_DAYS int,
    SIMPLE_AVG_EXECUTED_PRICE decimal(20,8),
    TOTAL_VALUE_EXECUTED decimal (20,2),
    TCA_PERFORMED boolean,
    RAW_PRICE  decimal(20,8),
    CONVERTED_PRICE  decimal(20,8),
    TIME_OF_EXECUTION_UTC timestamp,
    TRANSACTION_SIZE decimal (20,2),
    TRADING_SYSTEM VARCHAR(255),
    TRADING_MODE VARCHAR(255),
    TRADING_PLATFORM VARCHAR(255),
    BEST_BID_OFFER_OR_SUITABLE_REFERENCE varchar(255),
    MID_MARKET_RATE decimal(20,8),
    ABS_PRICE_DIFF decimal (20,8),
    MARKUP_AMOUNT decimal (20,2),
    MARKUP_USD decimal (20,2),
    ENTRY_TIMESTAMP timestamp default CURRENT_TIMESTAMP()
);