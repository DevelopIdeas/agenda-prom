SELECT COUNT(*) as total 
FROM customer 
WHERE updated_at < DATE_SUB(NOW(), INTERVAL 24 HOUR) 
  AND status = 'active'