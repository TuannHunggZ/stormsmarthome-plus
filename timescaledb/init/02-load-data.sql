COPY measurements_raw (
    id,
    timestamp,
    value,
    property,
    plug_id,
    household_id,
    house_id
)
FROM '/import/house-0-9_day-4.csv'
WITH (
    FORMAT csv,
    HEADER false
);

COPY measurements_raw (
    id,
    timestamp,
    value,
    property,
    plug_id,
    household_id,
    house_id
)
FROM '/import/historical_house-0-9_day-1-3.csv'
WITH (
    FORMAT csv,
    HEADER false
);

INSERT INTO measurements (
    id,
    timestamp,
    value,
    plug_id,
    household_id,
    house_id
)
SELECT
    id,
    to_timestamp(timestamp),
    value,
    plug_id,
    household_id,
    house_id
FROM measurements_raw
WHERE property = 1;

DROP TABLE measurements_raw;