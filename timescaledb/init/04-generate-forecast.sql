-- =====================================================
-- Generate Forecast
-- =====================================================

CREATE OR REPLACE PROCEDURE generate_forecast(
    p_window_size INTEGER,
    p_interval    INTERVAL
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_max_plug_timestamp  TIMESTAMPTZ;
    v_max_house_timestamp TIMESTAMPTZ;
BEGIN

    RAISE NOTICE 'Generating forecast (% minutes)...', p_window_size;

    -- Maximum timestamp of expected data

    SELECT MAX(timestamp)
    INTO v_max_plug_timestamp
    FROM plug_average_expected
    WHERE window_size = p_window_size;

    SELECT MAX(timestamp)
    INTO v_max_house_timestamp
    FROM house_average_expected
    WHERE window_size = p_window_size;

    -- Plug Forecast

    INSERT INTO plug_forecast_expected (
        window_size,
        timestamp,
        house_id,
        household_id,
        plug_id,
        forecast_load
    )
    WITH historical AS (
        SELECT
            window_size,
            house_id,
            household_id,
            plug_id,
            timestamp::time AS slice_time,
            percentile_cont(0.5)
                WITHIN GROUP (ORDER BY average_load)
                AS median_load
        FROM plug_average
        WHERE window_size = p_window_size
        GROUP BY
            window_size,
            house_id,
            household_id,
            plug_id,
            timestamp::time
    )
    SELECT
        p.window_size,
        p.timestamp + p_interval * 2,
        p.house_id,
        p.household_id,
        p.plug_id,
        (p.average_load + h.median_load) / 2 AS forecast_load
    FROM plug_average_expected p
    JOIN historical h
        ON  h.window_size   = p.window_size
        AND h.house_id       = p.house_id
        AND h.household_id   = p.household_id
        AND h.plug_id        = p.plug_id
        AND h.slice_time     = (p.timestamp + p_interval * 2)::time
    WHERE p.window_size = p_window_size
      AND p.timestamp + p_interval * 2 <= v_max_plug_timestamp;

    -- House Forecast

    INSERT INTO house_forecast_expected (
        window_size,
        timestamp,
        house_id,
        forecast_load
    )
    WITH historical AS (
        SELECT
            window_size,
            house_id,
            timestamp::time AS slice_time,
            percentile_cont(0.5)
                WITHIN GROUP (ORDER BY average_load)
                AS median_load
        FROM house_average
        WHERE window_size = p_window_size
        GROUP BY
            window_size,
            house_id,
            timestamp::time
    )
    SELECT
        p.window_size,
        p.timestamp + p_interval * 2,
        p.house_id,
        (p.average_load + h.median_load) / 2 AS forecast_load
    FROM house_average_expected p
    JOIN historical h
        ON  h.window_size = p.window_size
        AND h.house_id     = p.house_id
        AND h.slice_time   = (p.timestamp + p_interval * 2)::time
    WHERE p.window_size = p_window_size
      AND p.timestamp + p_interval * 2 <= v_max_house_timestamp;

END;
$$;

CALL generate_forecast(1,   INTERVAL '1 minute');
CALL generate_forecast(5,   INTERVAL '5 minutes');
CALL generate_forecast(10,  INTERVAL '10 minutes');
CALL generate_forecast(15,  INTERVAL '15 minutes');
CALL generate_forecast(20,  INTERVAL '20 minutes');
CALL generate_forecast(30,  INTERVAL '30 minutes');
CALL generate_forecast(60,  INTERVAL '60 minutes');
CALL generate_forecast(120, INTERVAL '120 minutes');