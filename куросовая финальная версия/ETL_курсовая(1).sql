CREATE OR REPLACE FUNCTION load_data_from_csv(
    file1_path TEXT,
    file2_path TEXT
)
RETURNS VOID AS $$
BEGIN
    -- Create temporary tables
    CREATE TEMP TABLE TempFile1 (
        flight_name TEXT,
        gate_name TEXT,
        status_name TEXT,
        departure_airport_name TEXT,
        departure_city TEXT,
        departure_country TEXT,
        departure_address TEXT,
        departure_time TEXT,
        arrival_airport_name TEXT,
        arrival_city TEXT,
        arrival_country TEXT,
        arrival_address TEXT,
        arrival_time TEXT,
        airline_name TEXT
    );

    CREATE TEMP TABLE TempFile2 (
        first_name TEXT,
        last_name TEXT,
        passport_details TEXT,
        ticket_class TEXT,
        price TEXT,
        flight_name TEXT,
        description TEXT,
        weight TEXT
    );
    
    -- Load data into temporary tables
    EXECUTE format('COPY TempFile1 FROM %L DELIMITER '','' CSV HEADER ENCODING ''LATIN1''', file1_path);
    EXECUTE format('COPY TempFile2 FROM %L DELIMITER '','' CSV HEADER ENCODING ''LATIN1''', file2_path);

   -- business checks
--1
DELETE FROM TempFile2
WHERE flight_name IN (
    SELECT DISTINCT t2.flight_name
    FROM TempFile2 t2
    INNER JOIN TempFile1 t1 ON t2.flight_name = t1.flight_name
    WHERE t1.departure_airport_name !~ '^[A-Z]{3}$' OR t1.arrival_airport_name !~ '^[A-Z]{3}$'
);

DELETE FROM TempFile1
WHERE departure_airport_name !~ '^[A-Z]{3}$' OR arrival_airport_name !~ '^[A-Z]{3}$';


--2
DELETE FROM TempFile1
WHERE flight_name IN (
    SELECT flight_name
    FROM TempFile2
    WHERE CAST(weight AS DECIMAL) < 0
  );

  DELETE FROM TempFile2
  WHERE CAST(weight AS DECIMAL) < 0;
  
  
--3
DELETE FROM TempFile2
WHERE flight_name IN (
    SELECT flight_name
    FROM TempFile1
    WHERE arrival_airport_name = departure_airport_name
);

DELETE FROM TempFile1
WHERE arrival_airport_name = departure_airport_name;
    

    -- 1. Insert into Locations
    INSERT INTO Locations (country, city, address)
    SELECT DISTINCT 
        CAST(t1.departure_country AS VARCHAR(32)), 
        CAST(t1.departure_city AS VARCHAR(32)), 
        CAST(t1.departure_address AS VARCHAR(64))
    FROM TempFile1 t1
    LEFT JOIN Locations l
    ON t1.departure_country = l.country AND t1.departure_city = l.city AND t1.departure_address = l.address
    WHERE l.location_id IS NULL AND t1.departure_country IS NOT NULL AND t1.departure_city IS NOT NULL AND t1.departure_address IS NOT NULL
    UNION
    SELECT DISTINCT 
        CAST(t1.arrival_country AS VARCHAR(32)), 
        CAST(t1.arrival_city AS VARCHAR(32)), 
        CAST(t1.arrival_address AS VARCHAR(64))
    FROM TempFile1 t1
    LEFT JOIN Locations l
    ON t1.arrival_country = l.country AND t1.arrival_city = l.city AND t1.arrival_address = l.address
    WHERE l.location_id IS NULL AND t1.arrival_country IS NOT NULL AND t1.arrival_city IS NOT NULL AND t1.arrival_address IS NOT NULL;

    -- 2. Insert into Airports
    INSERT INTO Airports (airport_name, location_id)
    SELECT DISTINCT 
        CAST(t1.departure_airport_name AS VARCHAR(32)),
        l.location_id
    FROM TempFile1 t1
    JOIN Locations l ON t1.departure_country = l.country AND t1.departure_city = l.city AND t1.departure_address = l.address
    LEFT JOIN Airports a ON t1.departure_airport_name = a.airport_name AND a.location_id = l.location_id
    WHERE a.airport_id IS NULL AND t1.departure_airport_name IS NOT NULL
    UNION
    SELECT DISTINCT 
        CAST(t1.arrival_airport_name AS VARCHAR(32)),
        l.location_id
    FROM TempFile1 t1
    JOIN Locations l ON t1.arrival_country = l.country AND t1.arrival_city = l.city AND t1.arrival_address = l.address
    LEFT JOIN Airports a ON t1.arrival_airport_name = a.airport_name AND a.location_id = l.location_id
    WHERE a.airport_id IS NULL AND t1.arrival_airport_name IS NOT NULL;

    -- 3. Insert into Gates
    INSERT INTO Gates (gate_name, airport_id)
    SELECT DISTINCT 
        CAST(t1.gate_name AS VARCHAR(32)), 
        a.airport_id
    FROM TempFile1 t1
    JOIN Airports a ON t1.departure_airport_name = a.airport_name
    LEFT JOIN Gates g ON t1.gate_name = g.gate_name AND g.airport_id = a.airport_id
    WHERE g.gate_id IS NULL AND t1.gate_name IS NOT NULL;

    -- 4. Insert into Airlines
    INSERT INTO Airlines (airline_name)
    SELECT DISTINCT 
        CAST(t1.airline_name AS VARCHAR(32))
    FROM TempFile1 t1
    LEFT JOIN Airlines al ON t1.airline_name = al.airline_name
    WHERE al.airline_id IS NULL AND t1.airline_name IS NOT NULL;

    -- 5. Insert into Flight_Statuses
    INSERT INTO Flight_Statuses (status_name)
    SELECT DISTINCT 
        CAST(t1.status_name AS VARCHAR(32))
    FROM TempFile1 t1
    LEFT JOIN Flight_Statuses fs ON t1.status_name = fs.status_name
    WHERE fs.status_id IS NULL AND t1.status_name IS NOT NULL;

    -- 6. Insert into Passengers
    INSERT INTO Passengers (first_name, last_name, passport_details)
    SELECT DISTINCT 
        CAST(t2.first_name AS VARCHAR(32)), 
        CAST(t2.last_name AS VARCHAR(32)), 
        CAST(t2.passport_details AS VARCHAR(32))
    FROM TempFile2 t2
    LEFT JOIN Passengers p ON t2.first_name = p.first_name AND t2.last_name = p.last_name AND t2.passport_details = p.passport_details
    WHERE p.passenger_id IS NULL AND t2.first_name IS NOT NULL AND t2.last_name IS NOT NULL AND t2.passport_details IS NOT NULL;

    -- 7. Insert into Flights
    INSERT INTO Flights (flight_name, departure_time, arrival_time, arrival_airport, gate_id, status_id, airline_id)
    SELECT DISTINCT 
        CAST(t1.flight_name AS VARCHAR(32)), 
        TO_TIMESTAMP(t1.departure_time, 'MM/DD/YYYY HH24:MI') AS departure_time,
        TO_TIMESTAMP(t1.arrival_time, 'MM/DD/YYYY HH24:MI') AS arrival_time,
        arr_airports.airport_id,
        g.gate_id, 
        fs.status_id, 
        al.airline_id
    FROM TempFile1 t1
    JOIN Gates g ON t1.gate_name = g.gate_name 
         AND t1.departure_airport_name = (SELECT airport_name FROM Airports WHERE Airports.airport_id = g.airport_id)
    JOIN Flight_Statuses fs ON t1.status_name = fs.status_name
    JOIN Airlines al ON t1.airline_name = al.airline_name
    JOIN Airports dep_airports ON t1.departure_airport_name = dep_airports.airport_name
    JOIN Airports arr_airports ON t1.arrival_airport_name = arr_airports.airport_name
    LEFT JOIN Flights f ON t1.flight_name = f.flight_name
    WHERE f.flight_id IS NULL AND t1.flight_name IS NOT NULL AND t1.departure_time IS NOT NULL AND t1.arrival_time IS NOT NULL;

    -- 8. Insert into Tickets
    INSERT INTO Tickets (ticket_class, price, flight_id, passenger_id)
    SELECT DISTINCT 
           CAST(t2.ticket_class AS VARCHAR(32)), 
           CAST(t2.price AS DECIMAL(10, 2)), 
           f.flight_id, 
           p.passenger_id
    FROM TempFile2 t2
    JOIN Flights f ON t2.flight_name = f.flight_name
    JOIN Passengers p ON t2.first_name = p.first_name AND t2.last_name = p.last_name AND t2.passport_details = p.passport_details
    LEFT JOIN Tickets t ON CAST(t2.ticket_class AS VARCHAR(32)) = t.ticket_class AND CAST(t2.price AS DECIMAL(10, 2)) = t.price AND f.flight_id = t.flight_id AND p.passenger_id = t.passenger_id
    WHERE t.ticket_id IS NULL AND t2.ticket_class IS NOT NULL AND t2.price IS NOT NULL;

    -- 9. Insert into Baggage
    INSERT INTO Baggage (weight, description, ticket_id)
    SELECT DISTINCT 
           CAST(t2.weight AS DECIMAL(10, 2)), 
           CAST(t2.description AS VARCHAR(32)), 
           t.ticket_id
    FROM TempFile2 t2
    JOIN Passengers p ON t2.first_name = p.first_name AND t2.last_name = p.last_name AND t2.passport_details = p.passport_details
    JOIN Flights f ON t2.flight_name = f.flight_name
    JOIN Tickets t ON t2.ticket_class = t.ticket_class AND CAST(t2.price AS DECIMAL(10, 2)) = t.price AND f.flight_id = t.flight_id AND p.passenger_id = t.passenger_id
    LEFT JOIN Baggage b ON t.ticket_id = b.ticket_id
    WHERE b.baggage_id IS NULL AND t2.weight IS NOT NULL;

    -- Drop temporary tables
    DROP TABLE TempFile1;
    DROP TABLE TempFile2;

END;
$$ LANGUAGE plpgsql;


-- call the function
SELECT load_data_from_csv('D:/Databases/sem4/file1.csv', 'D:/Databases/sem4/file2.csv');
	
SELECT * FROM baggage
SELECT * FROM locations
SELECT * FROM passengers
SELECT * FROM tickets
SELECT * FROM Flight_Statuses 
SELECT * FROM flights
SELECT  * FROM gates
SELECT  * FROM airlines
SELECT  * FROM airports




