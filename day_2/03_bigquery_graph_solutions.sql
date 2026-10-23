-- ============================================================================
-- Module 1 Lab 3: BigQuery Property Graph Analytics & Supply Chain Recall Traceability
-- Project : argolisyo
-- Dataset : cymbal_gold / cymbal-lakehouse.elevate_data
-- Standard: BigQuery GoogleSQL GQL (ISO/IEC 39075 GQL compliant)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Step 1: Verify Baseline Relational Source Tables (4 Node Tables + 3 Edge Tables)
-- ----------------------------------------------------------------------------
SELECT 'supplier_nodes' AS table_name, COUNT(*) AS row_count FROM `argolisyo.cymbal-lakehouse.elevate_data.supplier_nodes`
UNION ALL
SELECT 'batch_lot_nodes', COUNT(*) FROM `argolisyo.cymbal-lakehouse.elevate_data.batch_lot_nodes`
UNION ALL
SELECT 'store_nodes', COUNT(*) FROM `argolisyo.cymbal-lakehouse.elevate_data.store_nodes`
UNION ALL
SELECT 'customer_nodes', COUNT(*) FROM `argolisyo.cymbal-lakehouse.elevate_data.customer_nodes`
ORDER BY table_name;

SELECT 'produced_batch_edges' AS table_name, COUNT(*) AS row_count FROM `argolisyo.cymbal-lakehouse.elevate_data.produced_batch_edges`
UNION ALL
SELECT 'shipped_to_edges', COUNT(*) FROM `argolisyo.cymbal-lakehouse.elevate_data.shipped_to_edges`
UNION ALL
SELECT 'sold_lot_to_customer_edges', COUNT(*) FROM `argolisyo.cymbal-lakehouse.elevate_data.sold_lot_to_customer_edges`
ORDER BY table_name;

-- ----------------------------------------------------------------------------
-- Part 1 / Challenge 1.1: BigQuery Property Graph DDL Registration (BRD UC 2.4)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROPERTY GRAPH `argolisyo.cymbal_gold.supply_chain_traceability_graph`
  NODE TABLES (
    `argolisyo.cymbal-lakehouse.elevate_data.supplier_nodes` AS Supplier
      KEY (supplier_id)
      LABEL Supplier
      PROPERTIES (supplier_id, facility_name, country, city, contact_email, risk_score, certified),
    `argolisyo.cymbal-lakehouse.elevate_data.batch_lot_nodes` AS BatchLot
      KEY (batch_id)
      LABEL BatchLot
      PROPERTIES (batch_id, item_id, item_name, unit_price_usd, supplier_id, manufacturing_date, qa_inspection_status, harmful_material_defect, lot_size_units, currency, recall_status),
    `argolisyo.cymbal-lakehouse.elevate_data.store_nodes` AS Store
      KEY (store_id)
      LABEL Store
      PROPERTIES (store_id, store_name, city, country, region, manager_name, overnight_cash_float_usd),
    `argolisyo.cymbal-lakehouse.elevate_data.customer_nodes` AS Customer
      KEY (customer_id)
      LABEL Customer
      PROPERTIES (customer_id, name, loyalty_tier, email, phone_number, preferred_store, global_region)
  )
  EDGE TABLES (
    `argolisyo.cymbal-lakehouse.elevate_data.produced_batch_edges` AS PRODUCED_BATCH
      KEY (edge_id)
      SOURCE KEY (source_supplier_id) REFERENCES Supplier (supplier_id)
      DESTINATION KEY (target_batch_id) REFERENCES BatchLot (batch_id)
      LABEL PRODUCED_BATCH
      PROPERTIES (production_date, qc_status),
    `argolisyo.cymbal-lakehouse.elevate_data.shipped_to_edges` AS SHIPPED_TO
      KEY (edge_id)
      SOURCE KEY (source_batch_id) REFERENCES BatchLot (batch_id)
      DESTINATION KEY (target_store_id) REFERENCES Store (store_id)
      LABEL SHIPPED_TO
      PROPERTIES (quantity_shipped, shipment_date),
    `argolisyo.cymbal-lakehouse.elevate_data.sold_lot_to_customer_edges` AS PURCHASED_LOT_BY_CUSTOMER
      KEY (edge_id)
      SOURCE KEY (batch_id) REFERENCES BatchLot (batch_id)
      DESTINATION KEY (target_customer_id) REFERENCES Customer (customer_id)
      LABEL PURCHASED_LOT_BY_CUSTOMER
      PROPERTIES (store_id, transaction_id, purchase_timestamp, serial_number, unit_price_usd, currency, payment_method, exposure_flag)
  );

-- ----------------------------------------------------------------------------
-- Part 2 / Challenge 2.1: Variable-Length Distribution Routing & Path Extraction
-- Expected: 180 rows
-- ----------------------------------------------------------------------------
GRAPH `argolisyo.cymbal_gold.supply_chain_traceability_graph`
MATCH p = (lot:BatchLot)-[s:SHIPPED_TO]->{1,3}(store:Store)
RETURN
  lot.batch_id AS batch_id,
  lot.item_id AS item_id,
  lot.item_name AS item_name,
  store.store_id AS store_id,
  store.store_name AS store_name,
  store.city AS city,
  store.country AS country,
  PATH_LENGTH(p) AS distribution_hops,
  TO_JSON(p) AS shipment_path
ORDER BY store_id ASC, batch_id ASC;

-- ----------------------------------------------------------------------------
-- Part 2 / Challenge 2.2: High-Risk Supplier to VIP Loyalty Customer Recall Triage
-- Expected: 38 rows
-- ----------------------------------------------------------------------------
GRAPH `argolisyo.cymbal_gold.supply_chain_traceability_graph`
MATCH (sup:Supplier)-[:PRODUCED_BATCH]->(lot:BatchLot)-[sale:PURCHASED_LOT_BY_CUSTOMER]->(cust:Customer)
WHERE sup.risk_score >= 0.75
  AND cust.loyalty_tier IN ('PLATINUM', 'GOLD')
  AND (
    lot.qa_inspection_status = 'FLAGGED_HARMFUL_MATERIAL'
    OR lot.recall_status LIKE '%RECALL%'
    OR sale.exposure_flag = 'CONTAMINATED_HARMFUL'
  )
RETURN
  cust.customer_id AS customer_id,
  cust.name AS customer_name,
  cust.loyalty_tier AS loyalty_tier,
  cust.email AS customer_email,
  cust.phone_number AS customer_phone,
  cust.global_region AS global_region,
  lot.batch_id AS batch_id,
  lot.item_id AS item_id,
  lot.item_name AS item_name,
  lot.harmful_material_defect AS harmful_material_defect,
  lot.recall_status AS recall_status,
  sale.serial_number AS serial_number,
  sale.transaction_id AS transaction_id,
  sale.purchase_timestamp AS purchase_timestamp,
  sup.supplier_id AS supplier_id,
  sup.facility_name AS supplier_facility,
  sup.risk_score AS supplier_risk_score
ORDER BY loyalty_tier DESC, supplier_risk_score DESC, customer_name ASC;

-- ----------------------------------------------------------------------------
-- Part 2 / Challenge 2.3: Indirect Customer Exposure & Supplier Blast Radius (Diamond Pattern)
-- Expected: 83 rows
-- ----------------------------------------------------------------------------
GRAPH `argolisyo.cymbal_gold.supply_chain_traceability_graph`
MATCH (c1:Customer {customer_id: 'CUST_30697'})<-[:PURCHASED_LOT_BY_CUSTOMER]-(b1:BatchLot)<-[:PRODUCED_BATCH]-(sup:Supplier)-[:PRODUCED_BATCH]->(b2:BatchLot)-[:PURCHASED_LOT_BY_CUSTOMER]->(c2:Customer)
WHERE sup.risk_score >= 0.75
  AND c1.customer_id != c2.customer_id
RETURN
  c1.customer_id AS index_customer_id,
  c1.name AS index_customer_name,
  b1.batch_id AS index_batch_id,
  b1.item_name AS index_product_name,
  sup.supplier_id AS supplier_id,
  sup.facility_name AS common_supplier_facility,
  sup.risk_score AS supplier_risk_score,
  b2.batch_id AS secondary_batch_id,
  b2.item_name AS secondary_product_name,
  c2.customer_id AS secondary_customer_id,
  c2.name AS secondary_customer_name,
  c2.loyalty_tier AS secondary_loyalty_tier,
  c2.email AS secondary_customer_email,
  c2.phone_number AS secondary_customer_phone;

-- ----------------------------------------------------------------------------
-- Part 3 / Challenge 3.1: Emergency Recall Contact Ledger via GRAPH_TABLE()
-- Expected: 44 rows for batch LOT-202505-PROD4691-01
-- ----------------------------------------------------------------------------
SELECT
  batch_id,
  harmful_material_defect,
  item_name,
  unit_price_usd,
  customer_id,
  customer_name,
  global_region,
  email,
  phone_number,
  store_id,
  transaction_id,
  purchase_timestamp,
  serial_number
FROM GRAPH_TABLE(
  `argolisyo.cymbal_gold.supply_chain_traceability_graph`
  MATCH (lot:BatchLot {batch_id: 'LOT-202505-PROD4691-01'})-[sale:PURCHASED_LOT_BY_CUSTOMER]->(cust:Customer)
  WHERE lot.qa_inspection_status = 'FLAGGED_HARMFUL_MATERIAL'
  COLUMNS (
    lot.batch_id AS batch_id,
    lot.harmful_material_defect AS harmful_material_defect,
    lot.item_name AS item_name,
    lot.unit_price_usd AS unit_price_usd,
    cust.customer_id AS customer_id,
    cust.name AS customer_name,
    cust.global_region AS global_region,
    cust.email AS email,
    cust.phone_number AS phone_number,
    sale.store_id AS store_id,
    sale.transaction_id AS transaction_id,
    sale.purchase_timestamp AS purchase_timestamp,
    sale.serial_number AS serial_number
  )
)
ORDER BY purchase_timestamp DESC;

-- ----------------------------------------------------------------------------
-- Part 3 / Challenge 3.2: Supplier QA Defect Rate & Quality Scorecard (BRD UC 2.4)
-- Expected: 5 suppliers (SUP_001 defect_rate_pct = 100.0)
-- ----------------------------------------------------------------------------
SELECT
  supplier_id,
  facility_name,
  country,
  risk_score,
  COUNT(batch_id) AS total_lots_produced,
  COUNTIF(qa_inspection_status = 'PASSED_CLEAN') AS passed_clean_lots,
  COUNTIF(qa_inspection_status = 'FLAGGED_HARMFUL_MATERIAL') AS flagged_harmful_lots,
  ROUND(100.0 * COUNTIF(qa_inspection_status = 'FLAGGED_HARMFUL_MATERIAL') / COUNT(batch_id), 2) AS defect_rate_pct
FROM GRAPH_TABLE(
  `argolisyo.cymbal_gold.supply_chain_traceability_graph`
  MATCH (sup:Supplier)-[:PRODUCED_BATCH]->(lot:BatchLot)
  COLUMNS (
    sup.supplier_id AS supplier_id,
    sup.facility_name AS facility_name,
    sup.country AS country,
    sup.risk_score AS risk_score,
    lot.batch_id AS batch_id,
    lot.qa_inspection_status AS qa_inspection_status
  )
)
GROUP BY supplier_id, facility_name, country, risk_score
ORDER BY defect_rate_pct DESC, total_lots_produced DESC;

-- ----------------------------------------------------------------------------
-- Part 3 / Challenge 3.3: Comprehensive 360° Supply Chain Recall Traceability View
-- Target View: argolisyo.cymbal_gold.supply_chain_recall_traceability_360
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `argolisyo.cymbal_gold.supply_chain_recall_traceability_360` AS
SELECT
  g.transaction_id,
  g.hardware_serial_number,
  g.purchase_timestamp,
  g.product_sku,
  g.product_name,
  g.unit_price_usd,
  g.currency,
  CASE
    WHEN g.qa_inspection_status = 'FLAGGED_HARMFUL_MATERIAL'
      OR g.batch_recall_status LIKE '%RECALL%'
      OR UPPER(CAST(g.exposure_flag AS STRING)) IN ('TRUE', 'CONTAMINATED_HARMFUL')
    THEN TRUE
    ELSE FALSE
  END AS is_active_recall,
  g.qa_inspection_status,
  g.defect_description,
  g.batch_recall_status,
  CASE
    WHEN g.supplier_risk_score >= 0.75 THEN 'CRITICAL_HIGH_RISK'
    WHEN g.supplier_risk_score >= 0.30 THEN 'MODERATE_RISK'
    ELSE 'LOW_RISK'
  END AS supplier_risk_category,
  g.supplier_id,
  g.supplier_facility,
  g.supplier_country,
  g.supplier_risk_score,
  g.is_supplier_certified,
  g.batch_id,
  g.manufacturing_date,
  g.store_id,
  st.store_name,
  st.city AS store_city,
  st.country AS store_country,
  st.region AS store_region,
  g.customer_id,
  g.customer_name,
  g.customer_loyalty_tier,
  g.customer_email,
  g.customer_phone,
  g.customer_region
FROM GRAPH_TABLE(
  `argolisyo.cymbal_gold.supply_chain_traceability_graph`
  MATCH (sup:Supplier)-[:PRODUCED_BATCH]->(lot:BatchLot)-[sale:PURCHASED_LOT_BY_CUSTOMER]->(cust:Customer)
  COLUMNS (
    sale.transaction_id AS transaction_id,
    sale.serial_number AS hardware_serial_number,
    sale.purchase_timestamp AS purchase_timestamp,
    sale.store_id AS store_id,
    sale.exposure_flag AS exposure_flag,
    lot.item_id AS product_sku,
    lot.item_name AS product_name,
    lot.unit_price_usd AS unit_price_usd,
    lot.currency AS currency,
    lot.qa_inspection_status AS qa_inspection_status,
    lot.harmful_material_defect AS defect_description,
    lot.recall_status AS batch_recall_status,
    lot.batch_id AS batch_id,
    lot.manufacturing_date AS manufacturing_date,
    sup.supplier_id AS supplier_id,
    sup.facility_name AS supplier_facility,
    sup.country AS supplier_country,
    sup.risk_score AS supplier_risk_score,
    sup.certified AS is_supplier_certified,
    cust.customer_id AS customer_id,
    cust.name AS customer_name,
    cust.loyalty_tier AS customer_loyalty_tier,
    cust.email AS customer_email,
    cust.phone_number AS customer_phone,
    cust.global_region AS customer_region
  )
) AS g
LEFT JOIN `argolisyo.cymbal-lakehouse.elevate_data.store_nodes` AS st
  ON g.store_id = st.store_id;

-- Verification Query on 360° Recall View for batch LOT-202505-PROD4691-01 (44 rows)
SELECT
  transaction_id,
  hardware_serial_number,
  product_name,
  batch_id,
  customer_id,
  customer_name,
  customer_loyalty_tier,
  customer_email,
  customer_phone,
  store_id,
  store_name,
  store_city,
  store_country,
  is_active_recall,
  supplier_risk_category
FROM `argolisyo.cymbal_gold.supply_chain_recall_traceability_360`
WHERE is_active_recall = TRUE
  AND batch_id = 'LOT-202505-PROD4691-01'
ORDER BY purchase_timestamp DESC;
