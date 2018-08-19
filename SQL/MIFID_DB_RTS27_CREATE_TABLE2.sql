use mifid;
DROP TABLE MIFID_RTS27_TABLE2;
CREATE TABLE MIFID_RTS27_TABLE2 (
    ENTRY_ID INT NOT NULL AUTO_INCREMENT primary key,
    SOURCE_COMPANY_NAME VARCHAR(255),
    FILENAME VARCHAR(255),
    FILE_ID varchar(255),
    ISIN varchar(255),
    TRADE_DATE date,
    VENUE varchar(255),
    INSTRUMENT_NAME varchar(255),
    INSTRUMENT_CLASSIFICATION varchar(255),
    CURRENCY varchar(255),
    ENTRY_TIMESTAMP timestamp default CURRENT_TIMESTAMP()
);