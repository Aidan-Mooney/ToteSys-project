START TRANSACTION;

DELETE FROM fact_payment;
DELETE FROM fact_purchase_order;
DELETE FROM fact_sales_order;

DELETE FROM dim_counterparty;
DELETE FROM dim_currency;
DELETE FROM dim_design;
DELETE FROM dim_location;
DELETE FROM dim_payment_type;
DELETE FROM dim_staff;
DELETE FROM dim_transaction;

COMMIT;