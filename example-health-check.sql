SELECT COUNT(*) as total 
FROM job_queue 
WHERE status = 'pending' 
  AND created_at < DATE_SUB(NOW(), INTERVAL 1 HOUR)