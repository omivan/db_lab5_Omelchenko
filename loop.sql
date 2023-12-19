
DO $$ 
DECLARE
  i INT := 1;
  max_company_id INT;
BEGIN
  SELECT COALESCE(MAX(company_id), 0) INTO max_company_id FROM company;

  LOOP
    EXIT WHEN i > 10;
    INSERT INTO company (company_id, location, name)
    VALUES
      (max_company_id + i, 
       CASE i
         WHEN 1 THEN 'United States'
         WHEN 2 THEN 'South Korea'
         WHEN 3 THEN 'South Korea'
         ELSE 'Ukraine'
       END,
       'Test_company_' || (max_company_id + i));
    i := i + 1;
  END LOOP;

END $$;