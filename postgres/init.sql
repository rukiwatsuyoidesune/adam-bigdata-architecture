CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150)
);

CREATE TABLE restaurants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    category VARCHAR(100)
);

CREATE TABLE menu_items (
    id SERIAL PRIMARY KEY,
    restaurant_id INTEGER REFERENCES restaurants(id),
    name VARCHAR(150) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    restaurant_id INTEGER REFERENCES restaurants(id),
    menu_item_id INTEGER REFERENCES menu_items(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, email) VALUES
('Andi Pratama', 'andi@example.com'),
('Budi Santoso', 'budi@example.com'),
('Citra Dewi', 'citra@example.com'),
('Dina Putri', 'dina@example.com'),
('Eko Wijaya', 'eko@example.com');

INSERT INTO restaurants (name, category) VALUES
('Nasi Nusantara', 'Indonesian'),
('Burger House', 'Fast Food'),
('Sushi World', 'Japanese'),
('Pizza Corner', 'Italian');

INSERT INTO menu_items (restaurant_id, name, price) VALUES
(1, 'Chicken Fried Rice', 25000),
(1, 'Beef Rendang Rice', 35000),
(2, 'Classic Cheeseburger', 40000),
(2, 'Chicken Burger', 35000),
(3, 'Salmon Sushi', 45000),
(3, 'Chicken Teriyaki', 38000),
(4, 'Pepperoni Pizza', 55000),
(4, 'Margherita Pizza', 50000);

INSERT INTO orders
(customer_id, restaurant_id, menu_item_id, quantity, description)
VALUES
(1, 1, 1, 2, 'Please make it less spicy and add extra sauce'),
(2, 2, 3, 1, 'Extra cheese please'),
(3, 3, 5, 2, 'No wasabi and less soy sauce'),
(4, 1, 2, 1, 'Please add extra chili'),
(5, 4, 7, 1, 'Extra cheese and crispy crust');
