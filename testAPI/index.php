<?php

function redirect($url, $statusCode = 303)
{
   header('Location: ' . $url, true, $statusCode);
   die();
}
echo "\n Calling Index.php and redirecting";

redirect('https://mifid-data-analyser.appspot.com/transactions.php', false);
//redirect('transactions.php', false);

?>