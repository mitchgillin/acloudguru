# content-introduction-amazon-athena
<ol>
  <li>First Command Query 
  <pre> CREATE DATABASE real_estate_analytics ****
  </li>
       
  <li> Second Command Query 
<pre>
CREATE EXTERNAL TABLE IF NOT EXISTS `real_estate_analytics`.`manhattan_sold_houses` (
  `price` double,
  `bedrooms` double,
  `bathrooms` double,
  `sqft` double,
  `status` string,
  `address` string
)
PARTITIONED BY (neighborhood string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ',',
  'quoteChar' = '"'
)
LOCATION 's3://nyc-housing-analytics/manhattan-datafeed/'
TBLPROPERTIES ('has_encrypted_data' = 'false', 'skip.header.line.count'='1');
</pre> </li>
<li> Third Command Query 
 <pre> MSCK REPAIR TABLE manhattan_sold_houses </pre> </li> 
<li> Fourth Command Query  
<pre> SELECT DISTINCT(neighborhood) from manhattan_sold_houses </pre></li> 
</ol> 
