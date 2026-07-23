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