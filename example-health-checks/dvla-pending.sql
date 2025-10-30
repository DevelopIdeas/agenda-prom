SELECT COUNT(*) as total 
FROM customer 
WHERE (dvla_checked IS NULL OR dvla_checked = 0) 
  AND vehicle_reg IS NOT NULL 
  AND vehicle_reg <> '' 
  AND created_at < DATE_SUB(NOW(), INTERVAL 2 HOUR)