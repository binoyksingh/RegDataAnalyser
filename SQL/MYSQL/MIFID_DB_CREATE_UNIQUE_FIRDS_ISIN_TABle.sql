DROP TABLE UNIQUE_FIRDS_ISIN2;
CREATE TABLE UNIQUE_FIRDS_ISIN2 (
   ISIN VARCHAR(250),
   ENTRY_ID int,
   INSTRUMENT_FULL_NAME varchar (500),
   INSTRUMENT_CLASSIFICATION varchar(20),
   ISSUER_IDENTIFIER varchar (100),
   TRADING_VENUE  varchar(100),
   TERMINATION_DATE date,
   EXPIRY_DATE date,
   FR_DATE date,
   isin_rank int
);

