-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA staging TO current_user;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO current_user;
GRANT ALL PRIVILEGES ON SCHEMA marts TO current_user;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO current_user;