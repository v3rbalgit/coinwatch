-- TimescaleDB Diagnostics and Maintenance Script
-- This script contains queries to check the health and status of TimescaleDB continuous aggregates
-- and commands to optimize their performance.

-- 1. Check if the base hypertable exists and its configuration
SELECT * FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'market_data'
AND hypertable_name = 'kline_data';

-- 2. List all tables and views in the market_data schema
\dt market_data.*
\dv market_data.*

-- 3. Check all background jobs (includes continuous aggregate policies)
-- This shows all scheduled jobs, their intervals, and next run times
SELECT
    job_id,
    application_name,
    schedule_interval,
    next_start,
    proc_schema,
    proc_name,
    owner,
    scheduled,
    config
FROM timescaledb_information.jobs
WHERE application_name LIKE '%Refresh Continuous Aggregate Policy%'
ORDER BY job_id;

-- 4. Check job statistics to verify policies are running successfully
-- Shows total runs, successes, and failures for each continuous aggregate refresh job
SELECT
    job_id,
    total_runs,
    total_successes,
    total_failures
FROM timescaledb_information.job_stats
WHERE job_id IN (
    SELECT job_id
    FROM timescaledb_information.jobs
    WHERE application_name LIKE '%Refresh Continuous Aggregate Policy%'
);

-- 5. Optimize continuous aggregate policies
-- These commands adjust the refresh windows to include more recent data
-- Modify the end_offset to reduce the gap between real-time data and aggregated data

-- For 1-hour aggregates (job_id 1018)
-- Changes refresh window to between 15 minutes and 3 hours ago
SELECT alter_job(1018, config => '{"end_offset": "15 minutes", "start_offset": "03:00:00"}');

-- For 4-hour aggregates (job_id 1019)
-- Changes refresh window to between 1 hour and 12 hours ago
SELECT alter_job(1019, config => '{"end_offset": "01:00:00", "start_offset": "12:00:00"}');

-- For daily aggregates (job_id 1020)
-- Changes refresh window to between 6 hours and 3 days ago
SELECT alter_job(1020, config => '{"end_offset": "6 hours", "start_offset": "3 days"}');

-- 6. Manual refresh commands (use only when needed)
-- These commands will refresh the entire time range of each continuous aggregate
-- Warning: These can be resource-intensive on large datasets

-- Refresh 1-hour aggregates
CALL refresh_continuous_aggregate('market_data.kline_1h', NULL, NULL);

-- Refresh 4-hour aggregates
CALL refresh_continuous_aggregate('market_data.kline_4h', NULL, NULL);

-- Refresh daily aggregates
CALL refresh_continuous_aggregate('market_data.kline_1d', NULL, NULL);

-- 7. Verify changes took effect
-- After making changes, rerun the jobs query to confirm new configuration
SELECT
    job_id,
    application_name,
    schedule_interval,
    config
FROM timescaledb_information.jobs
WHERE application_name LIKE '%Refresh Continuous Aggregate Policy%'
ORDER BY job_id;

-- Note: After adjusting the policies, monitor the system's performance
-- If you notice increased load, you might need to adjust the refresh windows
-- to find the right balance between data freshness and system resources.
