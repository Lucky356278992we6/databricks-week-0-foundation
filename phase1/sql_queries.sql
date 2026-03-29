1.Load and Transform Data:
SELECT 
    p.product_id,
    p.name,
    p.category,
    COALESCE(SUM(s.quantity), 0) AS total_quantity,
    COALESCE(SUM(s.revenue), 0) AS total_revenue,
    COALESCE(SUM(i.stock), 0) AS total_stock
FROM products p

LEFT JOIN sales s 
ON p.product_id = s.product_id

LEFT JOIN inventory i 
ON p.product_id = i.product_id

GROUP BY p.product_id, p.name, p.category;


2.Handling Null Values:
SELECT *
FROM customers_raw
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL;

3.Total Purchases by Customer:
SELECT 
    customer_id,
    SUM(purchase_amount) AS total_purchase
FROM customer_purchases
GROUP BY customer_id
ORDER BY customer_id;


4.Running Payroll:
SELECT 
    e.employee_id,
    e.name,
    e.position,
    
    CASE 
        WHEN p.hours_worked <= 40 
            THEN p.hours_worked * p.hourly_rate
        ELSE 
            (40 * p.hourly_rate) + 
            ((p.hours_worked - 40) * p.hourly_rate * 1.5)
    END AS pay

FROM employees e
JOIN payroll p
ON e.employee_id = p.employee_id;


5.Food and Beverage Sales:
SELECT 
    p.product_id,
    p.name,
    p.category,
    COALESCE(SUM(s.quantity), 0) AS total_quantity,
    COALESCE(SUM(s.revenue), 0) AS total_revenue,
    COALESCE(SUM(i.stock), 0) AS total_stock

FROM products p

LEFT JOIN sales s 
ON p.product_id = s.product_id

LEFT JOIN inventory i 
ON p.product_id = i.product_id

GROUP BY 
    p.product_id, 
    p.name, 
    p.category;
  
