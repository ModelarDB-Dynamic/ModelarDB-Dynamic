$basePath = "C:\Users\Kenneth\Downloads\low_freq_generated\"

for ($i = 0; $i -le 20; $i++){
$myPath = $basePath + $i + ".csv"
curl -d "select * from datapoint order by timestamp asc" localhost:9999 | out-file -FilePath $myPath 
#echo "hello" | out-file -FilePath $myPath 
}