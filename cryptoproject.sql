
SELECT * FROM Crypto.pricedata
LIMIT 10;
CREATE TEMPORARY TABLE temp1
SELECT column_name,DATA_TYPE from INFORMATION_SCHEMA. COLUMNS where table_schema = 'Crypto' and table_name = 'pricedata';

SELECT CAST(event_date AS DATE) FROM pricedata;

ALTER TABLE pricedata
MODIFY event_date DATE;


CREATE TEMPORARY TABLE transactions
SELECT *,COUNT(event_date) OVER() AS total_transaction FROM pricedata
WHERE event_date BETWEEN "2018-01-01" AND "2021-12-31"
ORDER BY event_date asc;


SELECT name,eth_price,usd_price,event_date FROM sales
ORDER BY usd_price desc
LIMIT 5;


SELECT event_date,usd_price,
AVG(usd_price) OVER(ORDER BY event_date ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) AS moving_average
FROM pricedata;

SELECT name, AVG(usd_price) OVER() AS average_price
FROM pricedata
ORDER BY average_price desc;

CREATE TEMPORARY TABLE temp2
SELECT DATE_FORMAT(event_date,"%a") AS day_,COUNT(event_date) OVER(PARTITION BY DATE_FORMAT(event_date,"%a")) AS sale_count,
AVG(usd_price) OVER(PARTITION BY DATE_FORMAT(event_date,"%a")) AS avg_sale 
FROM pricedata;

SELECT DISTINCT(day_),sale_count,avg_sale FROM temp2
ORDER BY sale_count asc;


SELECT CONCAT(name,' was sold for ',usd_price,' to ',buyer_address,' from ',seller_address,' on ',event_date) AS summary
FROM pricedata;



CREATE VIEW view1 AS(
SELECT * FROM pricedata
WHERE LEFT(buyer_address,6) = "0x1919");

SELECT * FROM view1;



SELECT ROUND(eth_price,-2) AS eth, 
COUNT(*) AS count,
RPAD('', COUNT(*), '*') AS bar 
FROM pricedata
GROUP BY eth
ORDER BY eth;


SELECT name,MIN(usd_price) as price,
CASE
	WHEN MIN(usd_price) != -1 THEN "Low"
    ELSE "High"
END AS status
FROM pricedata
GROUP BY name
UNION
SELECT name,MAX(usd_price) as price,
CASE
	WHEN MAX(usd_price) = 0 THEN "Low"
	WHEN MAX(usd_price) != -1 THEN "High"
    ELSE "Low"
END AS status
FROM pricedata
GROUP BY name
ORDER BY price desc;


CREATE TEMPORARY TABLE temp3
SELECT DATE_FORMAT(event_date,"%Y") as year,DATE_FORMAT(event_date,'%b') as month,name,usd_price,COUNT(name) OVER(PARTITION BY DATE_FORMAT(event_date,"%Y"),DATE_FORMAT(event_date,'%b')) 
AS "month_total" FROM pricedata;
 
CREATE TEMPORARY TABLE temp4 
SELECT year,month,name,MAX(usd_price) OVER(PARTITION BY name) as month_max_price,count(name) AS montly_count
FROM temp3
GROUP BY year,month,name,usd_price
ORDER BY count(name) DESC;


CREATE TEMPORARY TABLE temp5
SELECT year,month,name,month_max_price,SUM(montly_count) 
as monthly_num 
FROM temp4 
GROUP BY year,month,name,month_max_price
ORDER BY count(name) DESC;

CREATE TABLE temp6
SELECT year,month,name,month_max_price,monthly_num, 
DENSE_RANK() OVER(PARTITION BY year,month ORDER BY monthly_num DESC) AS Rank_
FROM temp5
ORDER BY monthly_num DESC;

SELECT * FROM temp6
WHERE Rank_ = 1
ORDER BY year,month;


WITH volume AS (
SELECT year,month,SUM(usd_price) OVER(PARTITION BY year,month) AS total_volume 
FROM temp3)
SELECT year,month,ROUND(total_volume,-2) AS total_volume FROM volume
GROUP BY year,month,total_volume;


SELECT count(*) FROM transactions
WHERE buyer_address ="0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685" or seller_address = "0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685";



CREATE TEMPORARY TABLE average_
SELECT event_date,usd_price, AVG(usd_price) OVER(PARTITION BY event_date) AS daily_average 
FROM pricedata;

CREATE TEMPORARY TABLE average_new
WITH new_avg AS(
SELECT * FROM average_ 
WHERE NOT usd_price <= 0.1*daily_average)
SELECT event_date,usd_price, AVG(usd_price) OVER(PARTITION BY event_date) AS new_daily_average 
FROM new_avg;


CREATE TEMPORARY TABLE this
SELECT event_date,usd_price,new_daily_average, 
CASE
	WHEN usd_price>new_daily_average THEN "Profit"
    ELSE "Loss"
END AS "Profitability"
FROM average_new;

CREATE TEMPORARY TABLE test
WITH something AS (
SELECT t.event_date,p.buyer_address,t.usd_price,t.new_daily_average
FROM this t
JOIN pricedata p
ON  t.event_date=p.event_date AND  t.usd_price=p.usd_price)
SELECT DISTINCT(buyer_address),SUM(usd_price) OVER(PARTITION BY buyer_address) AS total_made,
SUM(new_daily_average) OVER(PARTITION BY buyer_address) AS min_to_make
FROM something;

SELECT *,
CASE
	WHEN total_made>min_to_make THEN "Wallet In Profit"
    ELSE "Wallet In Loss"
END AS "Profitability"
FROM test;
