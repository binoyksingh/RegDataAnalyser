<?php
/**
 * Files will be stored at e.g. https://storage.googleapis.com/<appspot site url>/testthis.txt
 * Sammple API Calls
 * 1. Get FX Transactions by Date
 * http://tactick.co.uk/API/public/transactions/FX/data?tradeDate=20180102
 *
 * 2. Get Dates for FX Transactions
 * http://tactick.co.uk/API/public/transactions/FX/dates
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

function get_transactions_by_date($conn, $tradeDate)
{
        echo "\n Calling get_transactions_by_date. Trade Date".$tradeDate;

		$sql = "select ENTRY_ID, SOURCE_COMPANY_NAME, ISIN, TRADE_DATE, INSTRUMENT_NAME, CURRENCY_PAIR,
                CURRENCY, TENOR_DAYS, SIMPLE_AVG_EXECUTED_PRICE, TOTAL_VALUE_EXECUTED, TCA_PERFORMED, RAW_PRICE,
                CONVERTED_PRICE, TIME_OF_EXECUTION_UTC, TRANSACTION_SIZE, BEST_BID_OFFER_OR_SUITABLE_REFERENCE,
                MID_MARKET_RATE, ABS_PRICE_DIFF, MARKUP_AMOUNT, MARKUP_USD, ENTRY_TIMESTAMP
                from MIFID_RTS27_TABLE3 where (TRADE_DATE = '".$tradeDate."')";

        echo "About to execute SQL : ".$sql;
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

function validate_token($conn, $token)
{
        echo "\n Validating Token :".$token;
		$sql = "select * from
                 USER_AUTHENTICATION where (TOKEN_ID = '".$token."')";

        echo "About to execute SQL : ".$sql;
        if (!$result = $conn->query($sql)) {
            echo "Sorry, the website is experiencing problems.";
            exit;
        }

        $num_rows = mysqli_num_rows($result);
        if ($num_rows > 0) {
            return 'TokenFound';
        }
        else {
            return 'TokenNotFound';
        }
}

function get_transaction_dates($conn)
{
		$sql = "select distinct TRADE_DATE from MIFID_RTS27_TABLE3" ;

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

echo "\n Calling Transactions.php";
print_r($_REQUEST);


// Get Relevant Parameters
if (isset($_GET['tradeDate']))
{
        $tradeDate = $_GET['tradeDate'];
}

if (isset($_GET['apiKey']))
{
        $token = $_GET['apiKey'];
}

$request_method=$_SERVER["REQUEST_METHOD"];
$server_name =  $_SERVER['SERVER_NAME'];
$request_uri = $_SERVER['REQUEST_URI'];
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);


echo "\n request_method :" .$request_method;
echo "\n server_name :" .$server_name;
echo "\n request_uri :" .$request_uri;
echo "\n tradeDate :" .$tradeDate;
echo "\n token is  :" .$token;

// Create connection
echo "\n----Message Start\n";

$conn = new mysqli($servername, $username, $password, $dbname,
	$dbport, "/cloudsql/mifid-data-analyser:us-central1:mifid-data-analyser");

// Check connection
if ($conn->connect_error) {
    die("Connection failed: " . $conn->connect_error);
}

$auth = validate_token($conn, $token);
echo "\n Auth is ".$auth;

if ($auth == "TokenFound"){
    echo ("User Authenticated");
}
else {
    die ("Token Not Found");
}

switch($request_method)
{
    case 'GET':
        // Retrive Products
        if((!empty($_GET["tradeDate"]))&&(!empty($_GET["tradeDate"])))
        {
            echo "\n----Both DateFrom and DateTo are set, calling get_transactions_by_date\n";
            get_transactions_by_date($conn,$tradeDate);
        }
        break;
    default:
        // Invalid Request Method
        header("HTTP/1.0 405 Method Not Allowed");
        break;
}

echo "\n----Message Finish\n";

?>
