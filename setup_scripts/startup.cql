CREATE KEYSPACE IF NOT EXISTS supplier WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 3};
DROP TABLE IF EXISTS supplier.warehouses;
DROP TABLE IF EXISTS supplier.districts;
DROP TABLE IF EXISTS supplier.customers;
DROP TABLE IF EXISTS supplier.orders_by_customer;
DROP TABLE IF EXISTS supplier.orders;
DROP TABLE IF EXISTS supplier.items;
DROP TABLE IF EXISTS supplier.order_lines;
DROP TABLE IF EXISTS supplier.stocks;
DROP TYPE  IF EXISTS supplier.address;
DROP TYPE  IF EXISTS supplier.name;

CREATE TYPE supplier.name
    (
    FIRST VARCHAR,
    MIDDLE VARCHAR,
    LAST VARCHAR,
    );

CREATE TYPE supplier.address
    (
    STREET_1 VARCHAR,
    STREET_2 VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    ZIP VARCHAR,
    );

CREATE TABLE IF NOT EXISTS supplier.warehouses
(
    W_ID      INT,
    W_NAME    VARCHAR,
    W_TAX     DECIMAL,
    W_YTD     DECIMAL,
    W_ADDRESS address,
    PRIMARY KEY (W_ID)
);

CREATE TABLE IF NOT EXISTS supplier.districts
(
    D_W_ID      INT,
    D_ID        INT,
    D_NAME      VARCHAR,
    D_ADDRESS   address,
    D_TAX       DECIMAL,
    D_YTD       DECIMAL,
    D_NEXT_O_ID INT,
    PRIMARY KEY (D_W_ID, D_ID)
);

CREATE TABLE IF NOT EXISTS supplier.customers
(
    C_W_ID         INT,
    C_D_ID         INT,
    C_ID           INT,
    C_NAME         name,
    C_ADDRESS      address,
    C_PHONE        VARCHAR,
    C_SINCE        TIMESTAMP,
    C_CREDIT       VARCHAR,
    C_CREDIT_LIM   DECIMAL,
    C_DISCOUNT     DECIMAL,
    C_BALANCE      DECIMAL,
    C_YTD_PAYMENT  FLOAT,
    C_PAYMENT_CNT  INT,
    C_DELIVERY_CNT INT,
    C_DATA         VARCHAR,
    PRIMARY KEY ((C_W_ID), C_ID)
);

CREATE TABLE IF NOT EXISTS supplier.orders
(
    O_W_ID       INT,
    O_D_ID       INT,
    O_ID         INT,
    O_C_ID       INT,
    O_CARRIER_ID INT,
    O_OL_CNT     DECIMAL,
    O_ALL_LOCAL  DECIMAL,
    O_ENTRY_D    TIMESTAMP,
    PRIMARY KEY ((O_W_ID), O_ID)
);

CREATE TABLE IF NOT EXISTS supplier.orders_by_customer
(
    O_W_ID       INT,
    O_D_ID       INT,
    O_ID         INT,
    O_C_ID       INT,
    O_CARRIER_ID INT,
    O_OL_CNT     DECIMAL,
    O_ALL_LOCAL  DECIMAL,
    O_ENTRY_D    TIMESTAMP,
    PRIMARY KEY ((O_C_ID), O_ID)
) WITH CLUSTERING ORDER BY (O_ID DESC);

CREATE TABLE IF NOT EXISTS supplier.items
(
    I_ID    INT,
    I_NAME  VARCHAR,
    I_PRICE DECIMAL,
    I_IM_ID INT,
    I_DATA  VARCHAR,
    PRIMARY KEY (I_ID),
);

CREATE TABLE IF NOT EXISTS supplier.order_lines
(
    OL_W_ID        INT,
    OL_D_ID        INT,
    OL_O_ID        INT,
    OL_NUMBER      INT,
    OL_I_ID        INT,
    OL_DELIVERY_D  TIMESTAMP,
    OL_AMOUNT      DECIMAL,
    OL_SUPPLY_W_ID INT,
    OL_QUANTITY    DECIMAL,
    OL_DIST_INFO   VARCHAR,
    PRIMARY KEY ((OL_W_ID), OL_D_ID, OL_O_ID,  OL_NUMBER)
);

CREATE TABLE IF NOT EXISTS supplier.stocks
(
    S_W_ID       INT,
    S_I_ID       INT,
    S_QUANTITY   DECIMAL,
    S_YTD        DECIMAL,
    S_ORDER_CNT  INT,
    S_REMOTE_CNT INT,
    S_DIST_01    VARCHAR,
    S_DIST_02    VARCHAR,
    S_DIST_03    VARCHAR,
    S_DIST_04    VARCHAR,
    S_DIST_05    VARCHAR,
    S_DIST_06    VARCHAR,
    S_DIST_07    VARCHAR,
    S_DIST_08    VARCHAR,
    S_DIST_09    VARCHAR,
    S_DIST_10    VARCHAR,
    S_DATA       VARCHAR,
    PRIMARY KEY ((S_W_ID), S_I_ID, S_QUANTITY)
) WITH CLUSTERING ORDER BY (S_I_ID ASC, S_QUANTITY DESC);

