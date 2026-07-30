CREATE TABLE plug_average (
    window_size    INTEGER NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,

    house_id       INTEGER NOT NULL,
    household_id   INTEGER NOT NULL,
    plug_id        INTEGER NOT NULL,

    average_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        timestamp,
        house_id,
        household_id,
        plug_id
    )
);

SELECT create_hypertable('plug_average', by_range('timestamp', INTERVAL '1 day'));

CREATE TABLE house_average (
    window_size    INTEGER NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,

    house_id       INTEGER NOT NULL,

    average_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        timestamp,
        house_id
    )
);

SELECT create_hypertable('house_average', by_range('timestamp', INTERVAL '1 day'));

CREATE TABLE plug_forecast (
    window_size    INTEGER NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,

    house_id       INTEGER NOT NULL,
    household_id   INTEGER NOT NULL,
    plug_id        INTEGER NOT NULL,

    forecast_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        house_id,
        household_id,
        plug_id,
        timestamp
    )
);

SELECT create_hypertable('plug_forecast', by_range('timestamp', INTERVAL '1 day'));

CREATE TABLE house_forecast (
    window_size    INTEGER NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,

    house_id       INTEGER NOT NULL,

    forecast_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        house_id,
        timestamp
    )
);

SELECT create_hypertable('house_forecast', by_range('timestamp', INTERVAL '1 day'));