-- =====================================================
-- Configuration
-- =====================================================

DO $$
BEGIN
    RAISE NOTICE 'Generating average tables...';
END $$;

-- 2013-09-04 00:00:01 UTC
CREATE OR REPLACE FUNCTION train_end()
RETURNS TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT to_timestamp(1377986401 + 3 * 86400);
$$;

-- =====================================================
-- Generate Plug Average
-- =====================================================

CREATE OR REPLACE PROCEDURE generate_plug_average(
    p_window_size INTEGER,
    p_interval    INTERVAL
)
LANGUAGE plpgsql
AS
$$
BEGIN

    RAISE NOTICE 'Generating plug average (% minutes)...', p_window_size;

    INSERT INTO plug_average_expected
    SELECT
        p_window_size,
        time_bucket(p_interval, timestamp),
        house_id,
        household_id,
        plug_id,
        AVG(value)
    FROM measurements
    WHERE timestamp >= train_end()
    GROUP BY
        time_bucket(p_interval, timestamp),
        house_id,
        household_id,
        plug_id;

    INSERT INTO plug_average
    SELECT
        p_window_size,
        time_bucket(p_interval, timestamp),
        house_id,
        household_id,
        plug_id,
        AVG(value)
    FROM measurements
    WHERE timestamp < train_end()
    GROUP BY
        time_bucket(p_interval, timestamp),
        house_id,
        household_id,
        plug_id;

END;
$$;

CALL generate_plug_average(1,   INTERVAL '1 minute');
CALL generate_plug_average(5,   INTERVAL '5 minutes');
CALL generate_plug_average(10,  INTERVAL '10 minutes');
CALL generate_plug_average(15,  INTERVAL '15 minutes');
CALL generate_plug_average(20,  INTERVAL '20 minutes');
CALL generate_plug_average(30,  INTERVAL '30 minutes');
CALL generate_plug_average(60,  INTERVAL '60 minutes');
CALL generate_plug_average(120, INTERVAL '120 minutes');

-- =====================================================
-- Generate House Average
-- =====================================================

CREATE OR REPLACE PROCEDURE generate_house_average(
    p_window_size INTEGER
)
LANGUAGE plpgsql
AS
$$
BEGIN

    RAISE NOTICE 'Generating house average (% minutes)...', p_window_size;

    INSERT INTO house_average_expected
    SELECT
        window_size,
        timestamp,
        house_id,
        SUM(average_load)
    FROM plug_average_expected
    WHERE window_size = p_window_size
    GROUP BY
        window_size,
        timestamp,
        house_id;

    INSERT INTO house_average
    SELECT
        window_size,
        timestamp,
        house_id,
        SUM(average_load)
    FROM plug_average
    WHERE window_size = p_window_size
    GROUP BY
        window_size,
        timestamp,
        house_id;

END;
$$;

CALL generate_house_average(1);
CALL generate_house_average(5);
CALL generate_house_average(10);
CALL generate_house_average(15);
CALL generate_house_average(20);
CALL generate_house_average(30);
CALL generate_house_average(60);
CALL generate_house_average(120);

-- =====================================================
-- Cleanup
-- =====================================================

DROP FUNCTION train_end();

DROP PROCEDURE generate_plug_average;
DROP PROCEDURE generate_house_average;

DROP TABLE measurements;

DO $$
BEGIN
    RAISE NOTICE 'Average generation completed.';
END $$;