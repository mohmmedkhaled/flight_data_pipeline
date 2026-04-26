CREATE TABLE flight_data (
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(24),
    callsign VARCHAR(50),
    origin_country VARCHAR(100),
    time_position BIGINT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);