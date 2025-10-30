SELECT COUNT(*) as total 
FROM tcg_notes 
WHERE sent_status IS NULL 
  OR sent_status = 'pending' 
  AND created_at < DATE_SUB(NOW(), INTERVAL 30 MINUTE)