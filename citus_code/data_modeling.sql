-- create warehouse table 
CREATE TABLE warehouse (
    w_id INT PRIMARY KEY,
    w_name VARCHAR(10),
    w_street1 VARCHAR(20),
    w_street2 VARCHAR(20),
    w_city VARCHAR(20),
    w_state CHAR(2),
    w_zip CHAR(9),
    w_tax DECIMAL(4,4),
    w_ytd DECIMAL(12,2)
);
SELECT create_distributed_table('warehouse', 'w_id');

-- create district table 
CREATE TABLE district (
    d_w_id INT,
    d_id INT,
    d_name VARCHAR(10),
    d_street1 VARCHAR(20),
    d_street2 VARCHAR(20),
    d_city VARCHAR(20),
    d_state CHAR(2),
    d_zip CHAR(9),
    d_tax DECIMAL(4,4),
    d_ytd DECIMAL(12,2),
    d_next_o_id INT,
    PRIMARY KEY (d_w_id, d_id),
    FOREIGN KEY (d_w_id) REFERENCES warehouse(w_id)
);
SELECT create_distributed_table('district', 'd_w_id');


-- create customer table and distribute on w_id
CREATE TABLE customer (
    c_w_id INT,
    c_d_id INT,
    c_id INT,
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
    c_street1 VARCHAR(20),
    c_street2 VARCHAR(20),
    c_city VARCHAR(20),
    c_state CHAR(2),
    c_zip CHAR(9),
    c_phone CHAR(16),
    c_since TIMESTAMP,
    c_credit CHAR(2),
    c_credit_lim DECIMAL(12,2),
    c_discount DECIMAL(5,4),
    c_balance DECIMAL(12,2),
    c_ytd_payment FLOAT,
    c_payment_cnt INT,
    c_delivery_cnt INT,
    c_data VARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id),
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district(d_w_id, d_id)
);

SELECT create_distributed_table('customer', 'c_w_id');

-- create orders table and distribute on w_id
CREATE TABLE orders (
    o_w_id INT,
    o_d_id INT,
    o_id INT,
    o_c_id INT,
    o_carrier_id INT CHECK (o_carrier_id >= 1 AND o_carrier_id <= 10),
    o_ol_cnt DECIMAL(2,0),
    o_all_local DECIMAL(1,0),
    o_entry_d TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer(c_w_id, c_d_id, c_id)
);

SELECT create_distributed_table('orders', 'o_w_id');

-- create item table 
CREATE TABLE item (
    i_id INT PRIMARY KEY,
    i_name VARCHAR(24),
    i_price DECIMAL(5,2),
    i_im_id INT,
    i_data VARCHAR(50)
);
SELECT create_reference_table('item');

-- create order line table and distribute on w_id
CREATE TABLE order_line (
    ol_w_id INT,
    ol_d_id INT,
    ol_o_id INT,
    ol_number INT,
    ol_i_id INT,
    ol_delivery_d TIMESTAMP,
    ol_amount DECIMAL(7,2),
    ol_supply_w_id INT,
    ol_quantity DECIMAL(2,0),
    ol_dist_info CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
    -- FOREIGN KEY (ol_i_id) REFERENCES item(i_id),
    -- FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES orders(o_w_id, o_d_id, o_id)
);
SELECT create_distributed_table('order_line', 'ol_w_id');
ALTER TABLE order_line ADD FOREIGN KEY(ol_w_id, ol_d_id, ol_o_id) REFERENCES orders(o_w_id, o_d_id, o_id);
ALTER TABLE order_line ADD FOREIGN KEY(ol_i_id) REFERENCES item(i_id);

-- create stock table 
CREATE TABLE stock (
    s_w_id INT,
    s_i_id INT,
    s_quantity DECIMAL(4,0),
    s_ytd DECIMAL(8,2),
    s_order_cnt INT,
    s_remote_cnt INT,
    s_dist_01 CHAR(24),
    s_dist_02 CHAR(24),
    s_dist_03 CHAR(24),
    s_dist_04 CHAR(24),
    s_dist_05 CHAR(24),
    s_dist_06 CHAR(24),
    s_dist_07 CHAR(24),
    s_dist_08 CHAR(24),
    s_dist_09 CHAR(24),
    s_dist_10 CHAR(24),
    s_data VARCHAR(50),
    PRIMARY KEY (s_w_id, s_i_id)
    -- FOREIGN KEY (s_i_id) REFERENCES item(i_id),
    -- FOREIGN KEY (s_w_id) REFERENCES warehouse(w_id)
);
SELECT create_distributed_table('stock', 's_w_id');
ALTER TABLE stock ADD FOREIGN KEY(s_i_id) REFERENCES item(i_id);
ALTER TABLE stock ADD FOREIGN KEY (s_w_id) REFERENCES warehouse(w_id); 

-- Create the update_home_order_line function
CREATE OR REPLACE FUNCTION update_home_order_line()
RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
BEGIN
  IF NEW.ol_supply_w_id = NEW.ol_w_id THEN
    NEW.is_home_order_line = TRUE;
  ELSE
    NEW.is_home_order_line = FALSE;
  END IF;
  RETURN NEW;
END;
$fn$;

-- -- Distribute the function by ol_w_id
-- SELECT create_distributed_function('update_home_order_line', 'ol_w_id');

-- Create the update_all_local function
CREATE OR REPLACE FUNCTION update_all_local()
RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
BEGIN
  IF (SELECT COUNT(*) FROM order_line WHERE ol_w_id = NEW.o_w_id AND ol_d_id = NEW.o_d_id AND ol_o_id = NEW.o_id AND is_home_order_line = FALSE) = 0 THEN
    NEW.o_all_local = TRUE;
  ELSE
    NEW.o_all_local = FALSE;
  END IF;
  RETURN NEW;
END;
$fn$;

-- -- Distribute the function by o_w_id
-- SELECT create_distributed_function('update_all_local', 'o_w_id');

SELECT run_command_on_all_nodes(
  $cmd$
    CREATE TRIGGER update_home_order_line_trigger
    BEFORE INSERT OR UPDATE ON order_line
    FOR EACH ROW
    EXECUTE FUNCTION update_home_order_line();
  $cmd$
);


SELECT run_command_on_all_nodes(
  $cmd$
    CREATE TRIGGER update_all_local_trigger
    BEFORE INSERT OR UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION update_all_local();
  $cmd$
);