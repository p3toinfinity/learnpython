-- Example dbt model for testing
-- This is a simple model to verify dbt can build models successfully

SELECT 
    1 as id,
    'Hello from dbt!' as message,
    CURRENT_TIMESTAMP() as created_at

