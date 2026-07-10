SELECT COUNT(*) AS total_delitos FROM delitos;
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'delitos';

SELECT * FROM delitos LIMIT 10;