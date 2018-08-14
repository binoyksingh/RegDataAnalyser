import pymysql

def connect():
    print ('Trying to connect')
    #connection = pymysql.connect(unix_socket='/cloudsql/mifid-data-analyser:us-central1:mifid-data-analyser' , user='root',password='root',db='guestbook')
    connection = pymysql.connect(host='127.0.0.1', user='root',password='root',db='mifid')
    print ('Connection Success')
    try:
        with connection.cursor() as cursor:
        # Read something
        #        sql_string = "SELECT * from entries;"
        #        cursor.execute(sql_string)
        #        #print(cursor.description)
        #        for row in cursor:
        #                print (row)
        #        cursor.close()
            insert_rts27__table2_sql_string = "INSERT INTO `MIFID_RTS27_TABLE2` (`SOURCE_COMPANY_NAME`, `FILENAME`,`FILE_ID`,`ISIN`,`TRADE_DATE`,`VENUE`," \
                                      "`INSTRUMENT_NAME`,`INSTRUMENT_CLASSIFICATION`,`CURRENCY`) " \
                                      " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )"
            cursor.execute(insert_rts27__table2_sql_string, ('HSBC','TEST_FILE_NAME','HSBC_001','US0001111','2018-03-01','XOFF','GBP/USD 20180381','cfi_code_01','USD'))
            connection.commit()

    except pymysql.InternalError as e:
        print ('Error:')
        print(e)

    finally:
        connection.close()

if __name__ == '__main__':
    connect()