DROP TABLE IF EXISTS dim_currency;
CREATE TABLE dim_currency (
    currency_id INT,
    currency_code VARCHAR(100),
    currency_name VARCHAR(100),
);
INSERT INTO dim_currency
    (currency_id, currency_code, currency_name)
VALUES
    (3, EUR, Euro)
    (1, GBP, Great British Pound)
    (2, USD, United States Dollar)
;