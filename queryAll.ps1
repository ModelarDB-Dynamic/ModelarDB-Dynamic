
$query = "select  DATEDIFF(ss, '1970-01-01 00:00:00', timestamp) as A, value from datapoint where tid = "
#$response = invoke-restmethod -uri http://127.0.0.1:9999 -method post -body $query


$basePath = "C:\Users\Kenneth\Downloads\low_freq_generated\"
$numberOfTids = 20

$dataTranformerFunction = {if($_) {if($_.A){$_.A + " " + $_.VALUE}}}

for ($tid = 0; $tid -le $numberOfTids; $tid++){
echo $tid
  $myPath = $basePath + $tid + ".csv"
  $response = invoke-restmethod -uri http://127.0.0.1:9999 -method post -body ($query + $tid.ToString() + " order by A asc")
 # $data = $response.result





  $response.result | ForEach-Object $dataTranformerFunction | out-file -FilePath $myPath
}