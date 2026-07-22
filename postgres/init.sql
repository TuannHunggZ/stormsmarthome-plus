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

CREATE INDEX idx_slice_index ON plug_average(slice_index);

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

CREATE INDEX idx_house_slice_index ON house_average(slice_index);