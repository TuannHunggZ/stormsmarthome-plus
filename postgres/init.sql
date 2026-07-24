CREATE TABLE plug_average (
    window_size    INTEGER NOT NULL,
    slice_index    BIGINT NOT NULL,

    house_id       INTEGER NOT NULL,
    household_id   INTEGER NOT NULL,
    plug_id        INTEGER NOT NULL,

    average_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        slice_index,
        house_id,
        household_id,
        plug_id
    )
);

CREATE TABLE house_average (
    window_size    INTEGER NOT NULL,
    slice_index    BIGINT NOT NULL,

    house_id       INTEGER NOT NULL,

    average_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        slice_index,
        house_id
    )
);

CREATE TABLE plug_forecast (
    window_size    INTEGER NOT NULL,
    slice_index    BIGINT NOT NULL,

    house_id       INTEGER NOT NULL,
    household_id   INTEGER NOT NULL,
    plug_id        INTEGER NOT NULL,

    forecast_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        house_id,
        household_id,
        plug_id,
        slice_index
    )
);

CREATE TABLE house_forecast (
    window_size    INTEGER NOT NULL,
    slice_index    BIGINT NOT NULL,

    house_id       INTEGER NOT NULL,

    forecast_load   DOUBLE PRECISION NOT NULL,

    PRIMARY KEY (
        window_size,
        house_id,
        slice_index
    )
);