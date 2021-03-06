use mifid;
DROP TABLE MIFID_RTS27_TABLE6;
CREATE TABLE MIFID_RTS27_TABLE6 (
    ENTRY_ID INT NOT NULL AUTO_INCREMENT primary key,
    SOURCE_COMPANY_NAME VARCHAR(255),
    FILENAME VARCHAR(255),
    FILE_ID varchar(255),
    ISIN varchar(255),
    TRADE_DATE date,
    INSTRUMENT_NAME varchar(255),
    NUMBER_OF_ORDER_OR_REQUEST_FOR_QUOTE int,
    NUMBER_OF_TRANSACTIONS_EXECUTED int,
    TOTAL_VALUE_OF_TRANSACTIONS_EXECUTED decimal (20,2),
    NUMBER_OF_ORDERS_OR_REQUEST_CANCELLED_OR_WITHDRAWN int,
    NUMBER_OF_ORDERS_OR_REQUEST_MODIFIED int,
    MEDIAN_TRANSACTION_SIZE decimal(20,2),
    MEDIAN_SIZE_OF_ALL_ORDERS_OR_REQUESTS_FOR_QUOTE decimal(20,2),
    NUMBER_OF_DESIGNATED_MARKET_MAKER int,
    CURRENCY varchar(255),
    ENTRY_TIMESTAMP timestamp default CURRENT_TIMESTAMP()
);