DELETE FROM dim_currency;
INSERT INTO dim_currency
    (currency_id, currency_code, currency_name)
VALUES
    ('3', 'EUR', 'Euro'),
    ('1', 'GBP', 'Great British Pound'),
    ('2', 'USD', 'United States Dollar');