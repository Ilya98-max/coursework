-- view all extensions
SELECT * FROM pg_extension;

-- Install the required extension

CREATE EXTENSION IF NOT EXISTS postgres_fdw;


-- Create a foreign server that connects to 'datawarehouse'
CREATE SERVER same_server_postgres
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'Airport', port '5432');

-- Create a user mapping for the current user
CREATE USER MAPPING FOR CURRENT_USER
    SERVER same_server_postgres
    OPTIONS (user 'postgres', password '220073dsi');


--Import tables from the remote database into the local schema
IMPORT FOREIGN SCHEMA public
FROM SERVER  same_server_postgres
INTO public;

--check if import works
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';

--create function
CREATE OR REPLACE FUNCTION transferring_data()
RETURNS void AS $$
BEGIN
    --Transferring data from locations to DimLocation
    INSERT INTO DimLocation (location_id, city, country, address)
    SELECT l.location_id, l.city, l.country, l.address
    FROM public.locations l
    LEFT JOIN DimLocation dl ON l.location_id = dl.location_id
    WHERE dl.location_id IS NULL;

    -- Transferring data from airports to DimAirport
    INSERT INTO DimAirport (airport_id, airport_name, location_id)
    SELECT a.airport_id, a.airport_name, a.location_id
    FROM public.airports a
    LEFT JOIN DimAirport da ON a.airport_id = da.airport_id
    WHERE da.airport_id IS NULL;
	
	-- Transferring data from flight_statuses в DimFlightStatus
    INSERT INTO DimFlightStatus (status_id, status_name)
    SELECT fs.status_id, fs.status_name
    FROM public.flight_statuses fs
    LEFT JOIN DimFlightStatus dfs ON fs.status_id = dfs.status_id
    WHERE dfs.status_id IS NULL;
	
	-- Transferring data from airlines to DimAirline
    INSERT INTO DimAirline (airline_id, airline_name)
    SELECT a.airline_id, a.airline_name
    FROM public.airlines a
    LEFT JOIN DimAirline da ON a.airline_id = da.airline_id
    WHERE da.airline_id IS NULL;
	
	-- Transferring data from passengers to DimPassenger
	INSERT INTO DimPassenger (passenger_id, first_name, last_name, passport_details)
    SELECT p.passenger_id, p.first_name, p.last_name, p.passport_details
    FROM public.passengers p
    LEFT JOIN DimPassenger dp ON p.passenger_id = dp.passenger_id
    WHERE dp.passenger_id IS NULL;
	
	
	-- Transferring data from gates to DimGate
    INSERT INTO DimGate (gate_id, airport_id, gate_name)
    SELECT g.gate_id, g.airport_id, g.gate_name
    FROM public.gates g
    LEFT JOIN DimGate dg ON g.gate_id = dg.gate_id
    WHERE dg.gate_id IS NULL;
	
	
	-- Transferring data from flights to DimFlights
    INSERT INTO DimFlights (flight_id, gate_id, status_id, departure_time, arrival_time, arrival_airport, flight_name, airline_id)
    SELECT f.flight_id, f.gate_id, f.status_id, f.departure_time, f.arrival_time, f.arrival_airport, f.flight_name, f.airline_id
    FROM public.flights f
    LEFT JOIN DimFlights df ON f.flight_id = df.flight_id
    WHERE df.flight_id IS NULL;
	
	
	 -- Transferring data from tickets to DimTickets
    INSERT INTO DimTickets (ticket_id, flight_id, passenger_id, price, ticket_class)
    SELECT t.ticket_id, t.flight_id, t.passenger_id, t.price, t.ticket_class
    FROM public.tickets t
    LEFT JOIN DimTickets dt ON t.ticket_id = dt.ticket_id
    WHERE dt.ticket_id IS NULL;
	
	 -- Transferring data from baggage to DimBaggage
    INSERT INTO DimBaggage (baggage_id, weight, description, ticket_id)
    SELECT b.baggage_id, b.weight, b.description, b.ticket_id
    FROM public.baggage b
    LEFT JOIN DimBaggage db ON b.baggage_id = db.baggage_id
    WHERE db.baggage_id IS NULL;
	
	 -- Transferring data to DimDate
    INSERT INTO DimDate (date, day, month, year, quarter, week_of_year)
    SELECT DISTINCT
        DATE(f.departure_time) AS date,
        EXTRACT(DAY FROM f.departure_time) AS day,
        EXTRACT(MONTH FROM f.departure_time) AS month,
        EXTRACT(YEAR FROM f.departure_time) AS year,
        EXTRACT(QUARTER FROM f.departure_time) AS quarter,
        EXTRACT(WEEK FROM f.departure_time) AS week_of_year
    FROM public.flights f
    LEFT JOIN DimDate dd ON DATE(f.departure_time) = dd.date
    WHERE f.departure_time IS NOT NULL
    AND dd.date IS NULL
	ORDER BY DATE(f.departure_time) ASC;


-- Transferring data to FlightsFact
INSERT INTO FlightsFact (
    gate_id,
    status_id,
    departure_date_id,
    arrival_airport_id,
    flight_name,
    airline_id,
    PassengersCount,
    BaggageWeight
)
SELECT 
    f.gate_id,
    f.status_id,
    d.date_id,
    f.arrival_airport,
    f.flight_name,
    f.airline_id,
    COUNT(t.ticket_id) AS PassengersCount,
    SUM(b.weight) AS BaggageWeight
FROM 
    DimFlights f
JOIN 
    DimTickets t ON f.flight_id = t.flight_id
JOIN 
    DimBaggage b ON t.ticket_id = b.ticket_id
JOIN 
    DimDate d ON DATE(f.departure_time) = d.date
LEFT JOIN 
    FlightsFact ff ON f.flight_id = ff.flight_id
WHERE 
    ff.flight_id IS NULL
GROUP BY 
    f.flight_id, f.gate_id, f.status_id, d.date_id, f.arrival_airport, f.flight_name, f.airline_id;
	


-- Transferring data to FlightsFact
INSERT INTO TicketsFact (ticket_id, flight_id, passenger_id, price, ticket_class, ticket_count_by_class)
    SELECT 
        t.ticket_id,
        t.flight_id,
        t.passenger_id,
        t.price,
        t.ticket_class,
        COUNT(*) OVER (PARTITION BY t.flight_id, t.ticket_class) AS ticket_count_by_class
    FROM 
        DimTickets t
    LEFT JOIN 
        TicketsFact tf ON t.ticket_id = tf.ticket_id
    WHERE 
        tf.ticket_id IS NULL;

END;
$$ LANGUAGE plpgsql;


-- call the function
SELECT transferring_data();


select * from dimlocation
select * from dimairline
select * from dimairport
select * from dimbaggage
select * from dimdate
select * from dimflights
select * from dimgate
select * from dimflightstatus
select * from dimtickets
select * from dimpassenger
select * from flightsfact
select * from ticketsfact









