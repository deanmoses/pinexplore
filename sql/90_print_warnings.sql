SELECT 'WARNING: ' || check_name || ' (' || cnt || ' rows)'
FROM checks.warnings WHERE cnt > 0;
