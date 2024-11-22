DROP TABLE IF EXISTS dim_payment_type;
CREATE TABLE dim_payment_type (
    payment_type_id INT,
    payment_type_name VARCHAR(100),
);
INSERT INTO dim_payment_type
    (payment_type_id, payment_type_name)
VALUES
    (1, SALES_RECEIPT)
    (2, SALES_REFUND)
    (3, PURCHASE_PAYMENT)
    (4, PURCHASE_REFUND)
;