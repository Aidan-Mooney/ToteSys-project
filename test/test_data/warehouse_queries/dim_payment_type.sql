DELETE FROM dim_payment_type;
INSERT INTO dim_payment_type
    (payment_type_id, payment_type_name)
VALUES
    ('1', 'SALES_RECEIPT'),
    ('2', 'SALES_REFUND'),
    ('3', 'PURCHASE_PAYMENT'),
    ('4', 'PURCHASE_REFUND')
ON CONFLICT (payment_type_id) DO UPDATE
SET
    payment_type_name = EXCLUDED.payment_type_name;