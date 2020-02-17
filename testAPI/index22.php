<?php
/**
 * Files will be stored at e.g. https://storage.googleapis.com/<appspot site url>/testthis.txt
 */
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/env.php';
use Google\Cloud\Storage\StorageClient;
$app = array();
$app['bucket_name'] = "mifid-data-analyser.appspot.com";
$app['mysql_user'] = $mysql_user;
$app['mysql_password'] = $mysql_password;
$app['mysql_dbname'] = "mifid";
$app['project_id'] = getenv('GCLOUD_PROJECT');
/**
 * Upload a file.
 *
 * @param string $bucketName the name of your Google Cloud bucket.
 * @param string $objectName the name of the object.
 * @param string $source the path to the file to upload.
 *
 * @return Psr\Http\Message\StreamInterface
 */

function get_transactions_by_date($conn, $dateFrom, $dateTo)
{
		$sql = "select ENTRY_ID, SOURCE_COMPANY_NAME, ISIN, TRADE_DATE, INSTRUMENT_NAME, CURRENCY_PAIR,
                CURRENCY, TENOR_DAYS, SIMPLE_AVG_EXECUTED_PRICE, TOTAL_VALUE_EXECUTED, TCA_PERFORMED, RAW_PRICE,
                CONVERTED_PRICE, TIME_OF_EXECUTION_UTC, TRANSACTION_SIZE, BEST_BID_OFFER_OR_SUITABLE_REFERENCE,
                MID_MARKET_RATE, ABS_PRICE_DIFF, MARKUP_AMOUNT, MARKUP_USD, ENTRY_TIMESTAMP
                from MIFID_RTS27_TABLE3 where TRADE_DATE => ".$dateFrom." and TRADE_DATE <= ".$dateTo.";

        if (!$result = $conn->query($sql)) {
            echo "Sorry, the website is experiencing problems.";
            exit;
        }

        // Create an Array of Transactions and a JSON Message
        $rows = array();
        while ($transaction = $result->fetch_assoc()) {
        $rows[] = $transaction;
        }
        print json_encode($rows);
}

function get_transaction_dates($conn)
{
		$sql = "select distinct TRADE_DATE from MIFID_RTS27_TABLE3 ;

        if (!$result = $conn->query($sql)) {
            echo "Sorry, the website is experiencing problems.";
            exit;
        }

        // Create an Array of Transactions and a JSON Message
        $rows = array();
        while ($dates = $result->fetch_assoc()) {
        $rows[] = $dates;
        }
        print json_encode($rows);
}

$servername = null;
$username = $app['mysql_user'];
$password = $app['mysql_password'];
$dbname = $app['mysql_dbname'];
$dbport = null;

// Get Relevant Parameters
if (isset($_GET['dateFrom']) && $_GET['dateFrom']!="")
{
        $dateFrom = $_GET['dateFrom'];
}

if (isset($_GET['dateTo']) && $_GET['dateTo']!="")
{
        $dateTo = $_GET['dateTo'];
}

// Create connection
$conn = new mysqli($servername, $username, $password, $dbname,
	$dbport, "/cloudsql/mifid-data-analyser:us-central1:mifid-data-analyser");
// Check connection
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}
//echo "\n----Connected successfully\n";
echo "\n----Message Start\n";

$sql = "select ENTRY_ID, SOURCE_COMPANY_NAME, ISIN, TRADE_DATE, INSTRUMENT_NAME, CURRENCY_PAIR,
            CURRENCY, TENOR_DAYS, SIMPLE_AVG_EXECUTED_PRICE, TOTAL_VALUE_EXECUTED, TCA_PERFORMED, RAW_PRICE,
            CONVERTED_PRICE, TIME_OF_EXECUTION_UTC, TRANSACTION_SIZE, BEST_BID_OFFER_OR_SUITABLE_REFERENCE,
            MID_MARKET_RATE, ABS_PRICE_DIFF, MARKUP_AMOUNT, MARKUP_USD, ENTRY_TIMESTAMP
             from MIFID_RTS27_TABLE3 where TCA_PERFORMED=true LIMIT 10";
if (!$result = $conn->query($sql)) {
    echo "Sorry, the website is experiencing problems.";
    exit;
}

// Create an Array of Transactions and a JSON Message
$rows = array();
while ($transaction = $result->fetch_assoc()) {
    $rows[] = $transaction;
}
print json_encode($rows);

echo "\n----Message Finish\n";

?>
