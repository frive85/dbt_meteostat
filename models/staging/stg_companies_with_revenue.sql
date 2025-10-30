WITH order_sales AS (
SELECT
	orderid,
	SUM((unitprice * quantity * (1-discount))) AS sales
FROM {{source("northwind","order_details")}} od 
GROUP BY orderid
),
customer_sales AS (
SELECT
	customerid,
	ROUND(SUM(sales)::NUMERIC,2) AS revenue
FROM order_sales
JOIN {{source("northwind","orders")}}
USING (orderid)
GROUP BY customerid
)
SELECT 
	*
FROM customer_sales 
JOIN {{source("northwind","customers")}}
USING (customerid)
ORDER BY revenue DESC