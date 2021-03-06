# This query takes 430 seconds and inserts 6million records per quarter
INSERT INTO UNIQUE_FIRDS_ISIN2
(
   ISIN ,
   ENTRY_ID,
   INSTRUMENT_FULL_NAME ,
   INSTRUMENT_CLASSIFICATION ,
   ISSUER_IDENTIFIER ,
   TRADING_VENUE  ,
   TERMINATION_DATE ,
   EXPIRY_DATE,
   FR_DATE
)
SELECT UNIQ_ISIN_DESC.INSTRUMENT_IDENTIFICATION_CODE, UNIQ_ISIN_DESC.ENTRY_ID,
    UNIQ_ISIN_DESC.INSTRUMENT_FULL_NAME, UNIQ_ISIN_DESC.INSTRUMENT_CLASSIFICATION,UNIQ_ISIN_DESC.ISSUER_IDENTIFIER,
    UNIQ_ISIN_DESC.TRADING_VENUE, UNIQ_ISIN_DESC.TERMINATION_DATE, UNIQ_ISIN_DESC.EXPIRY_DATE, UNIQ_ISIN_DESC.FR_DATE
from
(
    SELECT FIRDS_ALL.INSTRUMENT_IDENTIFICATION_CODE, FIRDS_ALL.ENTRY_ID,
    FIRDS_ALL.INSTRUMENT_FULL_NAME, FIRDS_ALL.INSTRUMENT_CLASSIFICATION,FIRDS_ALL.ISSUER_IDENTIFIER,
    FIRDS_ALL.TRADING_VENUE, FIRDS_ALL.TERMINATION_DATE, FIRDS_ALL.EXPIRY_DATE, FIRDS_ALL.FR_DATE
    FROM
    (
        select * from MIFID_FIRDS
    )
    as FIRDS_ALL
    JOIN
    (
        select INSTRUMENT_IDENTIFICATION_CODE, max(entry_id )MAX_ENTRY_ID
        from MIFID_FIRDS
        where trading_venue not in ('SSFX','360T')
        #and INSTRUMENT_IDENTIFICATION_CODE in ('EZ5915119X18','US9128283U26')
        group by INSTRUMENT_IDENTIFICATION_CODE
    ) UNIQ_ISIN on ((FIRDS_ALL.INSTRUMENT_IDENTIFICATION_CODE = UNIQ_ISIN.INSTRUMENT_IDENTIFICATION_CODE) and
                    (FIRDS_ALL.ENTRY_ID = UNIQ_ISIN.MAX_ENTRY_ID))
) UNIQ_ISIN_DESC