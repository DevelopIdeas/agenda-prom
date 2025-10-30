SELECT COUNT(*) as total 
FROM sync_log 
WHERE status = 'failed' 
  AND created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)