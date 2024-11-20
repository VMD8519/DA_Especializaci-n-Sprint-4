########## NIVEL 1 ###########
 -- Creación de la base de datos
    
CREATE DATABASE IF NOT EXISTS transactions_S4;
USE transactions_S4;


 -- Creación de la tabla companies
CREATE TABLE IF NOT EXISTS companies (
        company_id VARCHAR(20) PRIMARY KEY,
        company_name VARCHAR(255),
        phone VARCHAR(15),
        email VARCHAR(100),
        country VARCHAR(100),
        website VARCHAR(255)
    );
    
-- Creación de la tabla credit_cards  
  CREATE TABLE IF NOT EXISTS credit_cards (
	id VARCHAR(20) PRIMARY KEY,
    user_id INT,
    iban VARCHAR(50),
    pan VARCHAR (30), 
    pin VARCHAR (4),
	cvv VARCHAR (4),
    track1 VARCHAR (100),
    track2 VARCHAR (100),
    expiring_date VARCHAR (30)
    );
    

-- Creación de la tabla users 
    CREATE TABLE IF NOT EXISTS users (
        id INT PRIMARY KEY,
        name VARCHAR(100),
        surname VARCHAR(100),
        phone VARCHAR(150),
        email VARCHAR(150),
        birth_date VARCHAR(100),
        country VARCHAR(150),
        city VARCHAR(150),
        postal_code VARCHAR(100),
        address VARCHAR(255)
    );

-- Creación de la tabla products
 CREATE TABLE IF NOT EXISTS products( 
 id INT PRIMARY KEY,
 product_name VARCHAR(50),
 price DECIMAL(10, 2),
 colour VARCHAR(50),
 weight DECIMAL(3, 2),
 warehouse_id VARCHAR(30)
 );
 
 -- Creación de la tabla transactions
 CREATE TABLE IF NOT EXISTS transactions (
 id VARCHAR(255) PRIMARY KEY,
 card_id VARCHAR(20),
 business_id VARCHAR(20),
 timestamp TIMESTAMP,
 amount DECIMAL(10, 2),
 declined BOOLEAN,
 product_ids VARCHAR (200),
 user_id INT,
 lat FLOAT,
 longitude FLOAT, 
 
 FOREIGN KEY (card_id) REFERENCES credit_cards(id),
 FOREIGN KEY (business_id) REFERENCES companies(company_id),
 FOREIGN KEY ( user_id ) REFERENCES users (id)
 );
 
 
-- Insertar datos en las tablas
-- VERIFICACIONES 
SHOW VARIABLES LIKE 'secure_file_priv';
set global local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

-------- TABLA USERS ------

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_ca.csv"
INTO TABLE users 
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_uk.csv"
INTO TABLE users 
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\users_usa.csv"
INTO TABLE users 
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


select * from users;

-------- TABLA COMPANIES ------

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\companies.csv"
INTO TABLE companies
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

select * from companies;

-------- TABLA CREDIT_CARDS ------

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\credit_cards.csv"
INTO TABLE credit_cards
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


select * from credit_cards;

-------- TABLA PRODUCTS ------
ALTER TABLE products MODIFY price VARCHAR(100);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products.csv"
INTO TABLE products
FIELDS TERMINATED BY  ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from products;

-------- TABLA TRANSACTIONS ------

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions.csv"
INTO TABLE transactions
FIELDS TERMINATED BY  ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT* FROM transactions;

-- ----EJERCICIO 1 
-- Realiza una subconsulta que muestre a todos los usuarios con más de 30 transacciones utilizando al menos 2 tablas.

SELECT  user_id, count(amount) AS Cant_Transacciones
FROM transactions
WHERE user_id IN (select id FROM users)
GROUP BY 1
HAVING count(amount)>30;

-- ----EJERCICIO 2
-- Muestra la media de amount por IBAN de las tarjetas de crédito en la compañía Donec Ltd., utiliza por lo menos 2 tablas.

-- Verificación que la empresa se encuentra en la tabla companies
SELECT* FROM companies
WHERE company_name = 'Donec Ltd';

-- Calculo de la media 
SELECT iban, ROUND(avg(amount),2) AS Media_amount
FROM transactions
JOIN companies
ON company_id= business_id
JOIN credit_cards
ON card_id=credit_cards.id
WHERE company_name='Donec Ltd'
GROUP BY 1;


########## NIVEL 2 ###########
-- Crea una nueva tabla que refleje el estado de las tarjetas de crédito basado en si las últimas tres transacciones fueron declinadas y genera la siguiente consulta:

-- Creacion de una VIEW para Dpto. Riesgo y Seguridad

CREATE VIEW VistaRiesgoySeguridad AS
SELECT card_id, sum(declined) AS Cant_declined, 
CASE WHEN sum(declined) >= 3 THEN "Bloqueadas"
    ELSE "Activas"END AS Status_card
FROM (SELECT card_id, declined, timestamp
FROM (SELECT card_id, declined, timestamp,  ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS Numeracion
    FROM transactions
) AS Tabla_numeracion
WHERE Numeracion <= 3 
ORDER BY card_id, timestamp DESC) AS Aux_Limit_Transactions
GROUP BY 1
ORDER BY 1 ASC;

SELECT * FROM VistaRiesgoySeguridad;

-- ----EJERCICIO 1
-- ¿Cuántas tarjetas están activas?

SELECT count(*)
FROM VistaRiesgoySeguridad
WHERE Status_card = 'Activas';

########## NIVEL 3 ###########
-- Crea una tabla con la que podamos unir los datos del nuevo archivo products.csv con la base de datos creada, teniendo en cuenta que desde transaction tienes product_ids. Genera la siguiente consulta:

-- Creacion TABLA PUENTE/INTERMEDIA
CREATE TABLE IF NOT EXISTS Products_Transactions (
transaction_id VARCHAR (255),
products_id INT,
FOREIGN KEY (products_id) REFERENCES products (id),
FOREIGN KEY (transaction_id) REFERENCES transactions (id)
);

-- Carga de datos en la TABLA PUENTE/INTERMEDIA

INSERT INTO Products_Transactions (transaction_id, products_id) 
SELECT transactions.id AS transaction_id, products.id AS product_id
FROM transactions
JOIN products
ON FIND_IN_SET(products.id, REPLACE(transactions.product_ids, ' ', '')) > 0;

SELECT * FROM Products_Transactions;

-- ----EJERCICIO 1
-- Necesitamos conocer el número de veces que se ha vendido cada producto.

SELECT products_id, product_name, count(transaction_id) AS Cant_vendida, declined
FROM Products_Transactions
JOIN transactions
ON transaction_id=id
JOIN products
ON products_id=products.id
WHERE declined=0
GROUP BY 1,2,4
ORDER BY 1 ASC;

########## ACTUALIZACIÓN DE FORMATO Y TIPO DE DATOS PENDIENTES ###########
-- A) TABLA CREDIT_CARDS

-- --- Consultamos datos ingresados 
    SELECT expiring_date FROM credit_cards;

-- ---- Convertir las fechas de strings a date
SET SQL_safe_updates = 0;

UPDATE credit_cards
SET expiring_date = STR_TO_DATE(expiring_date, '%m/%d/%y')
WHERE expiring_date IS NOT NULL;

SET SQL_safe_updates = 1;

SELECT expiring_date  FROM credit_cards;

-- ----- Modificacion del tipo de dato de la columna 'expiring_date'
ALTER TABLE credit_cards
MODIFY COLUMN expiring_date DATE; 

-- B) TABLA PRODUCTS

-- --- Consultamos datos ingresados 
    SELECT price FROM products;

-- ---- Eliminación de los signos de moneda
SET SQL_safe_updates = 0;

UPDATE products
SET price = REPLACE (price, '$', '')
WHERE price IS NOT NULL;

SET SQL_safe_updates = 1;


SELECT price FROM products;

-- ----- Modificacion del tipo de dato de la columna 'price'
ALTER TABLE products
MODIFY COLUMN price DECIMAL(10, 2); 