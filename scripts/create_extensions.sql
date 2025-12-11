-- Enable PostGIS for geospatial operations (if needed)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable pg_trgm for fuzzy string matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;