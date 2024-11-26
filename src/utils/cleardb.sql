START TRANSACTION;

DELETE * FROM dim_counterparty;
DELETE * FROM dim_currency;
DELETE * FROM dim_data;

COMMIT;