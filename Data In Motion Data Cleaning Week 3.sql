#########START###############
SELECT 
	*
FROM
	data_in_motion_supply_chain_case_study.unclean_supply_chain;
    
######## DUPLICATE TABLE ###########
CREATE TABLE 
	my_supply_chain
LIKE 
	data_in_motion_supply_chain_case_study.unclean_supply_chain;

INSERT
	my_supply_chain
SELECT
	*
FROM
	data_in_motion_supply_chain_case_study.unclean_supply_chain;
    
########## VIEW_NEW TABLE #################
SELECT
	*
FROM
	data_in_motion_supply_chain_case_study.my_supply_chain;
    
#####################Remove Duplicates: Identify and remove any duplicate rows in the dataset.########################
/* Check for Duplicates */
SELECT
    shipment_id,
    COUNT(*) AS shipment_count
FROM
    my_supply_chain
GROUP BY
    shipment_id
HAVING shipment_count > 1;

/* Remove Duplicates */
#### Create ne table
CREATE TABLE
	my_supply_chain_1
LIKE
	data_in_motion_supply_chain_case_study.unclean_supply_chain;
    
ALTER TABLE 
	my_supply_chain_1
ADD
	row_num INT;
    
##### Inserting only single values to the new table created
INSERT INTO 
	my_supply_chain_1
SELECT 
	*,
ROW_NUMBER()OVER(
PARTITION BY shipment_id, product_id, supplier_id, shipment_date, delivery_date, product_name, quantity, price, supplier_name, supplier_country
) AS row_num
FROM
	my_supply_chain;

DELETE
FROM 
	my_supply_chain_1
WHERE 
	row_num > 1;
### check table
SELECT
	*
FROM
	data_in_motion_supply_chain_case_study.my_supply_chain_1;

/* Handle Missing Values: Fill missing delivery_date with the average delivery time calculated from existing data. 
Fill missing supplier_name and supplier_country using the most frequent values for each supplier_id.
*/
#### CONVERT TO DATE_TIME FORMART
SELECT 
    shipment_date,
    STR_TO_DATE(shipment_date, '%Y-%m-%d') AS converted_shipment_date
FROM 
    data_in_motion_supply_chain_case_study.my_supply_chain_1;

UPDATE my_supply_chain_1
SET shipment_date = NULL
WHERE shipment_date = '';

UPDATE my_supply_chain_1
SET delivery_date = NULL
WHERE delivery_date= '';


UPDATE my_supply_chain_1
SET shipment_date = STR_TO_DATE(shipment_date, '%Y-%m-%d');
UPDATE my_supply_chain_1
SET delivery_date = STR_TO_DATE(delivery_date, '%Y-%m-%d');

ALTER TABLE my_supply_chain_1
MODIFY COLUMN shipment_date DATE;

ALTER TABLE my_supply_chain_1
MODIFY COLUMN delivery_date DATE;

##### AVERAGE OF DELIVERY DATE THAT IS NOT NULL
SET @avg_delivery_date = (
    SELECT DATE_FORMAT(DATE_ADD(MIN(delivery_date), INTERVAL DATEDIFF(MAX(delivery_date), MIN(delivery_date)) / 2 DAY), '%Y-%m-%d')
    FROM my_supply_chain_1
    WHERE delivery_date IS NOT NULL
);
##### UPDATE WITH THE AVERAGE
UPDATE my_supply_chain_1
SET delivery_date = @avg_delivery_date
WHERE delivery_date IS NULL;

##### AVERAGE OF SHIPMENT DATE THAT IS NOT NULL
SET @avg_shipment_date = (
    SELECT DATE_FORMAT(DATE_ADD(MIN(shipment_date), INTERVAL DATEDIFF(MAX(shipment_date), MIN(shipment_date)) / 2 DAY), '%Y-%m-%d')
    FROM my_supply_chain_1
    WHERE delivery_date IS NOT NULL
);
##### UPDATE SHIPMENT DATE WITH THE AVERAGE
UPDATE my_supply_chain_1
SET shipment_date = @avg_shipment_date
WHERE shipment_date IS NULL;

/* Fill missing supplier_name and supplier_country using the most frequent values for each supplier_id.*/
UPDATE my_supply_chain_1
SET supplier_name = NULL
WHERE supplier_name = '';

UPDATE my_supply_chain_1
SET supplier_country = NULL
WHERE supplier_country = '';

SELECT supplier_name, COUNT(supplier_id) AS supplier_id_count
FROM my_supply_chain_1
GROUP BY supplier_name
ORDER BY 2 DESC;

SELECT supplier_id, supplier_country
    FROM my_supply_chain_1
    WHERE supplier_country IS NOT NULL
    GROUP BY supplier_id, supplier_country
    ORDER BY COUNT(*) DESC
    LIMIT 1;
    
    

############# CHECKING FOR SUPPLIER NAME MODE
-- CTE to determine the most frequent supplier_name for each supplier_id
WITH frequent_names_cte AS (
    SELECT 
        supplier_id, 
        supplier_name,
        COUNT(*) AS name_count
    FROM my_supply_chain_1
    WHERE supplier_name IS NOT NULL
    GROUP BY supplier_id, supplier_name
    ORDER BY COUNT(*) DESC
)
-- Select the most frequent supplier_name for each supplier_id
SELECT 
    supplier_name
FROM frequent_names_cte
GROUP BY supplier_name
HAVING COUNT(*) = 1;

#### CHECKING FOR SUPPLIER COUNTRY MODE
-- CTE to determine the most frequent supplier_country for each supplier_id
WITH frequent_countries_cte AS (
    SELECT 
        supplier_id, 
        supplier_country,
        COUNT(*) AS country_count
    FROM my_supply_chain_1
    WHERE supplier_country IS NOT NULL
    GROUP BY supplier_id, supplier_country
    ORDER BY COUNT(*) DESC
)
-- Select the most frequent supplier_country for each supplier_id
SELECT 
    supplier_id, 
    supplier_country
FROM frequent_countries_cte
GROUP BY supplier_id,supplier_country
HAVING COUNT(*) = 1;


SELECT supplier_id, supplier_country
    FROM my_supply_chain_1;

##################### UPDATING THE SUPPLIER NAME
-- Update supplier_name using the most frequent values
UPDATE my_supply_chain_1 t1
JOIN (
    -- CTE to determine the most frequent supplier_name for each supplier_id
    WITH frequent_names_cte AS (
        SELECT 
            supplier_id, 
            supplier_name,
            COUNT(*) AS name_count
        FROM my_supply_chain_1
        WHERE supplier_name IS NOT NULL
        GROUP BY supplier_id, supplier_name
        ORDER BY COUNT(*) DESC
    )
    -- Select the most frequent supplier_name for each supplier_id
    SELECT 
        supplier_id, 
        supplier_name
    FROM frequent_names_cte
    GROUP BY supplier_id, supplier_name
    HAVING COUNT(*) = 1
) t2
ON t1.supplier_id = t2.supplier_id
SET t1.supplier_name = t2.supplier_name
WHERE t1.supplier_name IS NULL;

##################### UPDATING THE SUPPLIER COUNTRY
-- Update supplier_country using the most frequent values
UPDATE my_supply_chain_1 t1
JOIN (
    -- CTE to determine the most frequent supplier_country for each supplier_id
    WITH frequent_countries_cte AS (
        SELECT 
            supplier_id, 
            supplier_country,
            COUNT(*) AS country_count
        FROM my_supply_chain_1
        WHERE supplier_country IS NOT NULL
        GROUP BY supplier_id, supplier_country
        ORDER BY COUNT(*) DESC
    )
    -- Select the most frequent supplier_country for each supplier_id
    SELECT 
        supplier_id, 
        supplier_country
    FROM frequent_countries_cte
    GROUP BY supplier_id, supplier_country
    HAVING COUNT(*) = 1
) t2
ON t1.supplier_id = t2.supplier_id
SET t1.supplier_country = t2.supplier_country
WHERE t1.supplier_country IS NULL;

SELECT 
       *
FROM my_supply_chain_1;

/* Correct Inconsistent Data: Standardize the format of the shipment_date and delivery_date columns to YYYY-MM-DD. ALREADY STANDARDIZED */ 

################################# IQR METHOD #################################

####QUNATITY
-- Calculate Q1 and Q3 for quantity
WITH sorted_quantity AS (
    SELECT 
        quantity,
        ROW_NUMBER() OVER (ORDER BY quantity) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE quantity IS NOT NULL
),
quartiles_quantity AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN quantity END) AS Q1_quantity,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN quantity END) AS Q3_quantity
    FROM sorted_quantity
)
-- Calculate lower and upper bounds for quantity
SELECT 
    Q1_quantity - 1.5 * (Q3_quantity - Q1_quantity) AS lower_bound_quantity,
    Q3_quantity + 1.5 * (Q3_quantity - Q1_quantity) AS upper_bound_quantity
FROM quartiles_quantity;

-- Detect Outliers in QUANTITY
WITH sorted_quantity AS (
    SELECT 
        quantity,
        ROW_NUMBER() OVER (ORDER BY quantity) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE quantity IS NOT NULL
),
quartiles_quantity AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN quantity END) AS Q1_quantity,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN quantity END) AS Q3_quantity
    FROM sorted_quantity
),
bounds_quantity AS (
    SELECT 
        Q1_quantity - 1.5 * (Q3_quantity - Q1_quantity) AS lower_bound_quantity,
        Q3_quantity + 1.5 * (Q3_quantity - Q1_quantity) AS upper_bound_quantity
    FROM quartiles_quantity
)
-- Detect outliers based on quantity bounds
SELECT *
FROM my_supply_chain_1 t
JOIN bounds_quantity bq ON 1 = 1
WHERE t.quantity < bq.lower_bound_quantity
   OR t.quantity > bq.upper_bound_quantity;
   
-- Remove Outliers in QUANTITY
WITH sorted_quantity AS (
    SELECT 
        quantity,
        ROW_NUMBER() OVER (ORDER BY quantity) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE quantity IS NOT NULL
),
quartiles_quantity AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN quantity END) AS Q1_quantity,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN quantity END) AS Q3_quantity
    FROM sorted_quantity
),
bounds_quantity AS (
    SELECT 
        Q1_quantity - 1.5 * (Q3_quantity - Q1_quantity) AS lower_bound_quantity,
        Q3_quantity + 1.5 * (Q3_quantity - Q1_quantity) AS upper_bound_quantity
    FROM quartiles_quantity
)
-- Remove outliers based on quantity bounds
DELETE FROM my_supply_chain_1
WHERE quantity < (SELECT lower_bound_quantity FROM bounds_quantity)
   OR quantity > (SELECT upper_bound_quantity FROM bounds_quantity);


#### PRICE
-- Calculate Q1 and Q3 for price
WITH sorted_price AS (
    SELECT 
        price,
        ROW_NUMBER() OVER (ORDER BY price) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE price IS NOT NULL
),
quartiles_price AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN price END) AS Q1_price,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN price END) AS Q3_price
    FROM sorted_price
)
-- Calculate lower and upper bounds for price
SELECT 
    Q1_price - 1.5 * (Q3_price - Q1_price) AS lower_bound_price,
    Q3_price + 1.5 * (Q3_price - Q1_price) AS upper_bound_price
FROM quartiles_price;

-- DETECT OUTLIERS IN PRICE
WITH sorted_price AS (
    SELECT 
        price,
        ROW_NUMBER() OVER (ORDER BY price) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE price IS NOT NULL
),
quartiles_price AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN price END) AS Q1_price,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN price END) AS Q3_price
    FROM sorted_price
),
bounds_price AS (
    SELECT 
        Q1_price - 1.5 * (Q3_price - Q1_price) AS lower_bound_price,
        Q3_price + 1.5 * (Q3_price - Q1_price) AS upper_bound_price
    FROM quartiles_price
)
-- Detect outliers based on price bounds
SELECT *
FROM my_supply_chain_1 t
JOIN bounds_price bp ON 1 = 1
WHERE t.price < bp.lower_bound_price
   OR t.price > bp.upper_bound_price;

-- REMOVE OUTLIERS IN PRICE
WITH sorted_price AS (
    SELECT 
        price,
        ROW_NUMBER() OVER (ORDER BY price) AS row_num,
        COUNT(*) OVER () AS total_count
    FROM my_supply_chain_1
    WHERE price IS NOT NULL
),
quartiles_price AS (
    SELECT 
        MAX(CASE WHEN row_num = CEIL(total_count * 0.25) THEN price END) AS Q1_price,
        MAX(CASE WHEN row_num = CEIL(total_count * 0.75) THEN price END) AS Q3_price
    FROM sorted_price
),
bounds_price AS (
    SELECT 
        Q1_price - 1.5 * (Q3_price - Q1_price) AS lower_bound_price,
        Q3_price + 1.5 * (Q3_price - Q1_price) AS upper_bound_price
    FROM quartiles_price
)
-- Remove outliers based on price bounds
DELETE FROM my_supply_chain_1
WHERE price < (SELECT lower_bound_price FROM bounds_price)
   OR price > (SELECT upper_bound_price FROM bounds_price);
   
   
########### Create New Features: Calculate delivery_time as the difference between delivery_date and shipment_date. Calculate total_value as the product of quantity and price.##
ALTER TABLE my_supply_chain_1
ADD COLUMN delivery_time INT;

ALTER TABLE my_supply_chain_1
ADD COLUMN total_value DECIMAL(10, 2);

-- Step 2: Calculate `delivery_time` as the difference between `delivery_date` and `shipment_date`
UPDATE my_supply_chain_1
SET delivery_time = DATEDIFF(delivery_date, shipment_date);

-- Step 3: Calculate `total_value` as the product of `quantity` and `price`
UPDATE my_supply_chain_1
SET total_value = quantity * price;

-- Step 4: Verify the updates
SELECT *
FROM my_supply_chain_1;






