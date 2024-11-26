INSERT INTO dim_currency
    (currency_id, currency_code, currency_name)
VALUES
    ('3', 'EUR', 'Euro'),
    ('1', 'GBP', 'Great British Pound'),
    ('2', 'USD', 'United States Dollar')
ON CONFLICT (currency_id) DO UPDATE
SET
    currency_code = EXCLUDED.currency_code,
    currency_name = EXCLUDED.currency_name;