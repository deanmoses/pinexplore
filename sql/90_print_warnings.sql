-- Print all accumulated warnings.
-- Runs last so it captures warnings from all prior layers.

SELECT 'WARNING: ' || check_name || ' (' || cnt || ' rows)'
FROM _warnings WHERE cnt > 0;
