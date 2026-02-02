-- PURCHASE CDC
INSERT INTO raw.purchase_cdc
(purchase_id, purchase_partition, buyer_id, prod_item_id, prod_item_partition, producer_id,
 order_date, release_date, purchase_total_value, purchase_status, op, event_ts, ingestion_ts)
VALUES
-- P1 pago
(1, 202301, 10, 1001, 202301, 500, '2023-01-06', '2023-01-06', 100.00, 'APROVADA', 'c',
 '2023-01-06 10:00:00', '2023-01-07 02:00:00'),
-- P2 inicialmente aprovado, depois corrigido para cancelado (chega tarde)
(2, 202301, 11, 1002, 202301, 501, '2023-01-05', '2023-01-05', 200.00, 'APROVADA', 'c',
 '2023-01-05 09:00:00', '2023-01-07 02:10:00'),
(2, 202301, 11, 1002, 202301, 501, '2023-01-05', '2023-01-05', 200.00, 'CANCELADA', 'u',
 '2023-01-05 12:00:00', '2023-01-10 01:00:00'),
-- P3 inicialmente sem release, depois recebe release (chega tarde)
(3, 202301, 12, 1003, 202301, 502, '2023-01-06', NULL, 150.00, 'INICIADA', 'c',
 '2023-01-06 08:00:00', '2023-01-07 02:20:00'),
(3, 202301, 12, 1003, 202301, 502, '2023-01-06', '2023-01-06', 150.00, 'APROVADA', 'u',
 '2023-01-06 08:05:00', '2023-01-10 01:10:00');

-- EXTRA INFO CDC
INSERT INTO raw.purchase_extra_info_cdc
(purchase_id, purchase_partition, subsidiary, op, event_ts, ingestion_ts)
VALUES
(1, 202301, 'S1', 'c', '2023-01-06 10:00:00', '2023-01-07 02:01:00'),
(2, 202301, 'S1', 'c', '2023-01-05 09:00:00', '2023-01-07 02:11:00'),
-- correção de subsidiária de P2 (chega tarde) - não importa no fim pq P2 cancelou
(2, 202301, 'S2', 'u', '2023-01-05 09:30:00', '2023-01-10 01:05:00'),
(3, 202301, 'S2', 'c', '2023-01-06 08:00:00', '2023-01-07 02:21:00');

-- PRODUCT ITEM CDC
INSERT INTO raw.product_item_cdc
(prod_item_id, prod_item_partition, purchase_id, purchase_partition, product_id, item_quantity, purchase_value,
 op, event_ts, ingestion_ts)
VALUES
(1001, 202301, 1, 202301, 9001, 1, 100.00, 'c', '2023-01-06 10:00:00', '2023-01-07 02:00:30'),
(1002, 202301, 2, 202301, 9002, 1, 200.00, 'c', '2023-01-05 09:00:00', '2023-01-07 02:10:30'),
(1003, 202301, 3, 202301, 9003, 1, 150.00, 'c', '2023-01-06 08:00:00', '2023-01-07 02:20:30');
