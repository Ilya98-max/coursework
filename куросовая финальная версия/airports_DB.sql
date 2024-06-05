CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(32) NOT NULL,
    country VARCHAR(32) NOT NULL,
    address VARCHAR(64) NOT NULL
);

CREATE TABLE airports (
    airport_id SERIAL PRIMARY KEY,
    airport_name VARCHAR(32) NOT NULL,
    location_id INT,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);


CREATE TABLE passengers (
    passenger_id SERIAL PRIMARY KEY,
    first_name VARCHAR(32) NOT NULL,
    last_name VARCHAR(32) NOT NULL,
    passport_details VARCHAR(32) NOT NULL
);

CREATE TABLE airlines (
    airline_id SERIAL PRIMARY KEY,
    airline_name VARCHAR(32) NOT NULL
);

CREATE TABLE gates (
    gate_id SERIAL PRIMARY KEY,
    airport_id INT NOT NULL,
    gate_name VARCHAR(32) NOT NULL,
    FOREIGN KEY (airport_id) REFERENCES airports(airport_id)
);


CREATE TABLE Flight_Statuses (
    status_id SERIAL PRIMARY KEY,
    status_name VARCHAR(32) NOT NULL
);

CREATE TABLE Flights (
    flight_id SERIAL PRIMARY KEY,
    gate_id INT NOT NULL,
    status_id INT NOT NULL,
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    arrival_airport INT NOT NULL, 
    flight_name VARCHAR(32) NOT NULL,
    airline_id INT NOT NULL,
    FOREIGN KEY (gate_id) REFERENCES Gates(gate_id),
    FOREIGN KEY (status_id) REFERENCES Flight_Statuses(status_id),
    FOREIGN KEY (arrival_airport) REFERENCES Airports(airport_id), 
    FOREIGN KEY (airline_id) REFERENCES Airlines(airline_id) 
);


CREATE TABLE Tickets (
    ticket_id SERIAL PRIMARY KEY,
    flight_id INT NOT NULL,
    passenger_id INT NOT NULL,
    price DECIMAL NOT NULL,
    ticket_class VARCHAR(32) NOT NULL,
    FOREIGN KEY (flight_id) REFERENCES flights(flight_id),
    FOREIGN KEY (passenger_id) REFERENCES passengers(passenger_id)
);

CREATE TABLE Baggage (
    baggage_id SERIAL PRIMARY KEY,
    weight DECIMAL NOT NULL,
    description VARCHAR(255),
    ticket_id INT NOT NULL,
    FOREIGN KEY (ticket_id) REFERENCES tickets(ticket_id)
);




