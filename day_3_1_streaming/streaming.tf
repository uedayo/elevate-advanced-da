# =====================================================================
# Data for resources already created in shared infra
# =====================================================================

# Project number for service agent IAM permissions
data "google_project" "project" {
  project_id = var.project_id
}

# Module service account
data "google_service_account" "cymbal_sa_data" {
  account_id = "cymbal-sa-data"
}

# BigQuery datasets
data "google_bigquery_dataset" "cymbal_bronze" {
  dataset_id = var.dataset_id_bronze
  project    = var.project_id
}

data "google_bigquery_dataset" "cymbal_silver" {
  dataset_id = var.dataset_id_silver
  project    = var.project_id
}

data "google_bigquery_dataset" "cymbal_gold" {
  dataset_id = var.dataset_id_gold
  project    = var.project_id
}

# VPC network and subnetwork
data "google_compute_network" "cymbal_network" {
  name    = "cymbal-retail-vpc"
  project = var.project_id
}

data "google_compute_subnetwork" "cymbal_subnet" {
  name    = "cymbal-retail-subnet-${var.gcp_region}"
  region  = var.gcp_region
  project = var.project_id
}

# =====================================================================
# Lab 2.1 resources
# =====================================================================

data "external" "bootstrap_address" {
  program = [
    "python3",
    "-c",
    "import subprocess, json; val=subprocess.check_output(['gcloud', 'managed-kafka', 'clusters', 'describe', '${var.kafka_cluster_id}', '--location=${var.gcp_region}', '--format=value(bootstrapAddress)', '--project=${var.project_id}']).decode().strip(); print(json.dumps({'bootstrap_address': val}))"
  ]
}

# Compute Engine instance for Kafka client
resource "google_compute_instance" "kafka_client" {
  name         = "kafka-client"
  machine_type = "e2-standard-2"
  zone         = "${var.gcp_region}-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-13"
    }
  }

  network_interface {
    subnetwork = data.google_compute_subnetwork.cymbal_subnet.id
  }

  service_account {
    email  = data.google_service_account.cymbal_sa_data.email
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    mkdir -p /opt/kafka-client
    gcloud storage cp gs://kaijun-elevate-kafka-client/stream_pos_generator.py /opt/kafka-client/stream_pos_generator.py
    gcloud storage cp gs://kaijun-elevate-kafka-client/stream_pos_requirements.txt /opt/kafka-client/stream_pos_requirements.txt
    apt-get update
    apt install -y python3-pip
    pip install -r /opt/kafka-client/stream_pos_requirements.txt --break-system-packages
    chmod -R 755 /opt/kafka-client

    # Write systemd service file
    cat <<'SERVICE_EOF' > /etc/systemd/system/retail-producer.service
[Unit]
Description=Streaming POS Producer Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kafka-client
ExecStart=/usr/bin/python3 /opt/kafka-client/stream_pos_generator.py --bootstrap_servers ${data.external.bootstrap_address.result.bootstrap_address} --topic pos-transactions
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICE_EOF

    systemctl daemon-reload
    systemctl enable retail-producer.service
    systemctl start retail-producer.service
  EOF

  depends_on = [
    data.external.bootstrap_address,
  ]
}

# =====================================================================
# Shared Module 2 APIs, Storage & IAM Permissions
# =====================================================================

resource "google_project_service" "streaming_apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
  ])
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# GCS bucket for Silver BigLake Iceberg storage & Dataflow staging/templates
resource "google_storage_bucket" "streaming_bucket" {
  name                        = "${var.project_id}-streaming-data"
  location                    = var.gcp_region
  project                     = var.project_id
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Additional IAM roles for cymbal-sa-data required across Module 2
resource "google_project_iam_member" "cymbal_sa_streaming_roles" {
  for_each = toset([
    "roles/aiplatform.user",
    "roles/bigtable.user",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/pubsub.viewer",
    "roles/dataflow.worker",
    "roles/dataflow.developer",
    "roles/bigquery.metadataViewer",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${data.google_service_account.cymbal_sa_data.email}"
}

# Allow Managed Kafka Connect service agent to publish to Pub/Sub
resource "google_project_iam_member" "kafka_service_agent_pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-managedkafka.iam.gserviceaccount.com"
}

# Allow Pub/Sub service agent to forward dead-letter messages & write to BigQuery
resource "google_project_iam_member" "pubsub_service_agent_roles" {
  for_each = toset([
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/bigquery.dataEditor",
    "roles/bigquery.metadataViewer",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"

  depends_on = [
    google_project_service.streaming_apis,
  ]
}

# =====================================================================
# Challenge 1.1: Managed Kafka to Pub/Sub Ingestion Bridge
# =====================================================================

resource "google_pubsub_topic" "pos_transactions" {
  name    = "pos-transactions"
  project = var.project_id

  depends_on = [
    google_project_service.streaming_apis,
  ]
}

resource "google_managed_kafka_connector" "pubsub_sink" {
  connector_id    = "pos-transactions-pubsub-sink"
  connect_cluster = var.kafka_connect_cluster_id
  location        = var.gcp_region
  project         = var.project_id

  configs = {
    "connector.class" = "com.google.pubsub.kafka.sink.CloudPubSubSinkConnector"
    "name"            = "pos-transactions-pubsub-sink"
    "tasks.max"       = "3"
    "topics"          = "pos-transactions"
    "value.converter" = "org.apache.kafka.connect.storage.StringConverter"
    "key.converter"   = "org.apache.kafka.connect.storage.StringConverter"
    "cps.topic"       = google_pubsub_topic.pos_transactions.name
    "cps.project"     = var.project_id
  }

  task_restart_policy {
    minimum_backoff = "60s"
    maximum_backoff = "1800s"
  }

  depends_on = [
    google_pubsub_topic.pos_transactions,
    google_project_iam_member.kafka_service_agent_pubsub_publisher,
  ]
}

# =====================================================================
# Challenge 1.2: Medallion Ingestion into BigQuery (Bronze, Silver, Gold & DLQ)
# =====================================================================

locals {
  raw_and_dlq_schema = jsonencode([
    {
      name = "subscription_name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "message_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "publish_time"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "attributes"
      type = "JSON"
      mode = "NULLABLE"
    },
    {
      name = "data"
      type = "STRING"
      mode = "NULLABLE"
    }
  ])

  clean_transactions_schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "NULLABLE" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "business_date", type = "DATE", mode = "NULLABLE" },
    { name = "store_id", type = "STRING", mode = "NULLABLE" },
    { name = "pos_terminal_id", type = "STRING", mode = "NULLABLE" },
    { name = "cashier_id", type = "STRING", mode = "NULLABLE" },
    { name = "customer_id", type = "STRING", mode = "NULLABLE" },
    { name = "customer_loyalty_tier", type = "STRING", mode = "NULLABLE" },
    { name = "payment_method", type = "STRING", mode = "NULLABLE" },
    { name = "payment_network", type = "STRING", mode = "NULLABLE" },
    { name = "card_bin", type = "STRING", mode = "NULLABLE" },
    { name = "is_contactless", type = "BOOLEAN", mode = "NULLABLE" },
    { name = "currency", type = "STRING", mode = "NULLABLE" },
    { name = "item_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "total_quantity", type = "INTEGER", mode = "NULLABLE" },
    { name = "subtotal_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "discount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "tax_amount", type = "NUMERIC", mode = "NULLABLE" },
    { name = "total", type = "NUMERIC", mode = "NULLABLE" },
    { name = "promo_code_applied", type = "STRING", mode = "NULLABLE" },
    { name = "manual_discount_flag", type = "BOOLEAN", mode = "NULLABLE" },
    {
      name = "items"
      type = "RECORD"
      mode = "REPEATED"
      fields = [
        { name = "line_seq", type = "INTEGER", mode = "NULLABLE" },
        { name = "item_id", type = "STRING", mode = "NULLABLE" },
        { name = "item_name", type = "STRING", mode = "NULLABLE" },
        { name = "category", type = "STRING", mode = "NULLABLE" },
        { name = "quantity", type = "INTEGER", mode = "NULLABLE" },
        { name = "unit_price", type = "NUMERIC", mode = "NULLABLE" },
        { name = "total", type = "NUMERIC", mode = "NULLABLE" },
        { name = "discount", type = "NUMERIC", mode = "NULLABLE" },
        { name = "item_net_amount", type = "NUMERIC", mode = "NULLABLE" }
      ]
    }
  ])

  add_business_date_udf = <<-EOT
    function addBusinessDate(message, metadata) {
      const payload = JSON.parse(message.data);
      if (payload.event_timestamp) {
        payload.business_date = String(payload.event_timestamp).substring(0, 10);
      }
      message.data = JSON.stringify(payload);
      return message;
    }
  EOT
}

# Dead-Letter Pub/Sub Topic
resource "google_pubsub_topic" "pos_transactions_dlq" {
  name    = "pos-transactions-dlq"
  project = var.project_id

  depends_on = [
    google_project_service.streaming_apis,
  ]
}

# Dead-Letter BigQuery Table: cymbal_bronze.dlq
resource "google_bigquery_table" "dlq" {
  dataset_id          = data.google_bigquery_dataset.cymbal_bronze.dataset_id
  table_id            = "dlq"
  project             = var.project_id
  deletion_protection = false
  schema              = local.raw_and_dlq_schema
}

# Subscription to land Dead-Letter messages into cymbal_bronze.dlq
resource "google_pubsub_subscription" "pos_transactions_dlq_sub" {
  name    = "pos-transactions-dlq-sub"
  topic   = google_pubsub_topic.pos_transactions_dlq.id
  project = var.project_id

  bigquery_config {
    table                 = "${var.project_id}.${google_bigquery_table.dlq.dataset_id}.${google_bigquery_table.dlq.table_id}"
    write_metadata        = true
    use_table_schema      = false
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  depends_on = [
    google_bigquery_table.dlq,
    google_project_iam_member.cymbal_sa_streaming_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# 1. Bronze Native Table: cymbal_bronze.pos_transactions_raw
resource "google_bigquery_table" "pos_transactions_raw" {
  dataset_id          = data.google_bigquery_dataset.cymbal_bronze.dataset_id
  table_id            = "pos_transactions_raw"
  project             = var.project_id
  deletion_protection = false
  schema              = local.raw_and_dlq_schema

  time_partitioning {
    type  = "DAY"
    field = "publish_time"
  }
}

resource "google_pubsub_subscription" "pos_transactions_bronze_sub" {
  name    = "pos-transactions-bronze-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  bigquery_config {
    table                 = "${var.project_id}.${google_bigquery_table.pos_transactions_raw.dataset_id}.${google_bigquery_table.pos_transactions_raw.table_id}"
    write_metadata        = true
    use_table_schema      = false
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_bigquery_table.pos_transactions_raw,
    google_pubsub_subscription.pos_transactions_dlq_sub,
    google_project_iam_member.cymbal_sa_streaming_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# 2. Silver Iceberg Managed Table: cymbal_silver.pos_transactions_clean
resource "google_bigquery_table" "pos_transactions_clean" {
  dataset_id          = data.google_bigquery_dataset.cymbal_silver.dataset_id
  table_id            = "pos_transactions_clean"
  project             = var.project_id
  deletion_protection = false
  schema              = local.clean_transactions_schema

  time_partitioning {
    type          = "DAY"
    field         = "business_date"
    expiration_ms = 604800000 # 7 days
  }

  biglake_configuration {
    connection_id = "projects/${var.project_id}/locations/${var.gcp_region}/connections/biglake-iceberg-connection"
    storage_uri   = "gs://${google_storage_bucket.streaming_bucket.name}/iceberg/pos_transactions_clean/"
    file_format   = "PARQUET"
    table_format  = "ICEBERG"
  }

  depends_on = [
    google_storage_bucket.streaming_bucket,
  ]
}

resource "google_pubsub_subscription" "pos_transactions_silver_sub" {
  name    = "pos-transactions-silver-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  message_transforms {
    javascript_udf {
      function_name = "addBusinessDate"
      code          = local.add_business_date_udf
    }
  }

  bigquery_config {
    table                 = "${var.project_id}.${google_bigquery_table.pos_transactions_clean.dataset_id}.${google_bigquery_table.pos_transactions_clean.table_id}"
    use_table_schema      = true
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_bigquery_table.pos_transactions_clean,
    google_pubsub_subscription.pos_transactions_dlq_sub,
    google_project_iam_member.cymbal_sa_streaming_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# 3. Gold Native Table: cymbal_gold.pos_transactions_gold
resource "google_bigquery_table" "pos_transactions_gold" {
  dataset_id          = data.google_bigquery_dataset.cymbal_gold.dataset_id
  table_id            = "pos_transactions_gold"
  project             = var.project_id
  deletion_protection = false
  schema              = local.clean_transactions_schema

  time_partitioning {
    type          = "DAY"
    field         = "business_date"
    expiration_ms = 604800000 # 7 days
  }
}

resource "google_pubsub_subscription" "pos_transactions_gold_sub" {
  name    = "pos-transactions-gold-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  message_transforms {
    javascript_udf {
      function_name = "addBusinessDate"
      code          = local.add_business_date_udf
    }
  }

  bigquery_config {
    table                 = "${var.project_id}.${google_bigquery_table.pos_transactions_gold.dataset_id}.${google_bigquery_table.pos_transactions_gold.table_id}"
    use_table_schema      = true
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_bigquery_table.pos_transactions_gold,
    google_pubsub_subscription.pos_transactions_dlq_sub,
    google_project_iam_member.cymbal_sa_streaming_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# =====================================================================
# Lab 2.2 resources to be created (not in starter code)
# =====================================================================

# ---------------------------------------------------------------------
# Challenge 2.1: Real-Time ML Inference With Stateless Input Features
# ---------------------------------------------------------------------

resource "google_bigquery_table" "pos_anomaly_alerts" {
  dataset_id          = data.google_bigquery_dataset.cymbal_gold.dataset_id
  table_id            = "pos_anomaly_alerts"
  project             = var.project_id
  deletion_protection = false
  clustering          = ["store_id", "alert_type"]

  time_partitioning {
    type          = "DAY"
    field         = "alert_ts"
    expiration_ms = 7776000000 # 90 days
  }

  schema = jsonencode([
    { name = "alert_id", type = "STRING", mode = "NULLABLE" },
    { name = "store_id", type = "STRING", mode = "NULLABLE" },
    { name = "alert_type", type = "STRING", mode = "NULLABLE" },
    { name = "alert_source", type = "STRING", mode = "NULLABLE" },
    { name = "transaction_id", type = "STRING", mode = "NULLABLE" },
    { name = "cashier_id", type = "STRING", mode = "NULLABLE" },
    { name = "discount_pct", type = "FLOAT", mode = "NULLABLE" },
    { name = "risk_score", type = "FLOAT", mode = "NULLABLE" },
    { name = "alert_ts", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

resource "google_pubsub_subscription" "order_anomaly_sub" {
  name    = "pos-transactions-order-anomaly-sub"
  topic   = google_pubsub_topic.pos_transactions.id
  project = var.project_id

  # Step 1: Extract model features & stash context in attributes
  message_transforms {
    javascript_udf {
      function_name = "prepareOrderAnomalyFeatures"
      code          = <<-EOT
        function prepareOrderAnomalyFeatures(message, metadata) {
          const d = JSON.parse(message.data);
          const subtotal = Number(d.subtotal_amount || 0);
          const discount = Number(d.discount || 0);
          const discountPct = subtotal > 0 ? (discount / subtotal) : 0.0;

          if (!message.attributes) {
            message.attributes = {};
          }
          message.attributes.transaction_id = String(d.transaction_id || "");
          message.attributes.store_id = String(d.store_id || "");
          message.attributes.cashier_id = String(d.cashier_id || "");
          message.attributes.discount_pct = String(discountPct);
          message.attributes.alert_ts = String(d.event_timestamp || "");

          const instance = {
            customer_loyalty_tier: String(d.customer_loyalty_tier || "NONE"),
            item_count: Number(d.item_count || 0),
            total_item_quantity: Number(d.total_quantity || 0),
            subtotal_amount: subtotal,
            discount_amount: discount,
            tax_amount: Number(d.tax_amount || 0),
            total_amount: Number(d.total || 0)
          };
          message.data = JSON.stringify({ instances: [instance] });
          return message;
        }
      EOT
    }
  }

  # Step 2: Invoke Vertex AI Endpoint via Pub/Sub AI Inference SMT
  message_transforms {
    ai_inference {
      endpoint              = "projects/${var.project_id}/locations/${var.gcp_region}/endpoints/${var.order_anomaly_endpoint_name}"
      service_account_email = data.google_service_account.cymbal_sa_data.email
      unstructured_inference {
        parameters = {}
      }
    }
  }

  # Step 3: Filter high-risk alerts (label == "1" && score >= 0.7) & format for BigQuery
  message_transforms {
    javascript_udf {
      function_name = "filterAndFormatOrderAnomalyAlert"
      code          = <<-EOT
        function filterAndFormatOrderAnomalyAlert(message, metadata) {
          const res = JSON.parse(message.data);
          const pred = res.model_output.predictions[0];
          const values = pred.label_order_anomaly_values;
          const probs = pred.label_order_anomaly_probs;
          let riskScore = 0.0;
          for (let i = 0; i < values.length; i++) {
            if (String(values[i]) === "1") {
              riskScore = Number(probs[i]);
              break;
            }
          }
          const predLabel = String(pred.predicted_label_order_anomaly);
          if (predLabel !== "1" || riskScore < 0.7) {
            return null;
          }
          const attrs = message.attributes || {};
          const msgId = (metadata && metadata.message_id) ? String(metadata.message_id) : String(Date.now());
          const out = {
            alert_id: "ALT-ANOMALY-" + String(attrs.transaction_id) + "-" + msgId,
            store_id: String(attrs.store_id),
            alert_type: "order_anomaly",
            alert_source: "order_anomaly_model",
            transaction_id: String(attrs.transaction_id),
            cashier_id: String(attrs.cashier_id),
            discount_pct: Number(attrs.discount_pct || 0.0),
            risk_score: riskScore,
            alert_ts: String(attrs.alert_ts)
          };
          message.data = JSON.stringify(out);
          return message;
        }
      EOT
    }
  }

  bigquery_config {
    table                 = "${var.project_id}.${google_bigquery_table.pos_anomaly_alerts.dataset_id}.${google_bigquery_table.pos_anomaly_alerts.table_id}"
    use_table_schema      = true
    drop_unknown_fields   = true
    service_account_email = data.google_service_account.cymbal_sa_data.email
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pos_transactions_dlq.id
    max_delivery_attempts = 5
  }

  depends_on = [
    google_bigquery_table.pos_anomaly_alerts,
    google_pubsub_subscription.pos_transactions_dlq_sub,
    google_project_iam_member.cymbal_sa_streaming_roles,
    google_project_iam_member.pubsub_service_agent_roles,
  ]
}

# ---------------------------------------------------------------------
# Challenge 2.2: Real-Time ML Inference With Windowed Input Features
# ---------------------------------------------------------------------

resource "google_bigtable_table" "cashier_realtime_alerts" {
  name                = "cashier_realtime_alerts"
  instance_name       = "operations-db"
  project             = var.project_id
  deletion_protection = "UNPROTECTED"

  column_family {
    family = "stats"
  }

  column_family {
    family = "flags"
  }
}

resource "google_pubsub_subscription" "cashier_abuse_sub" {
  name                 = "pos-transactions-cashier-abuse-sub"
  topic                = google_pubsub_topic.pos_transactions.id
  project              = var.project_id
  ack_deadline_seconds = 60
}

# Stage the Apache Beam Dataflow template to GCS using a Python venv
resource "terraform_data" "stage_cashier_abuse_template" {
  triggers_replace = [
    filesha256("${path.module}/cashier_abuse_pipeline.py"),
    filesha256("${path.module}/requirements.txt"),
    google_storage_bucket.streaming_bucket.name,
    google_pubsub_subscription.cashier_abuse_sub.id,
  ]

  provisioner "local-exec" {
    working_dir = path.module
    command     = <<-EOT
      set -e
      export GOOGLE_API_USE_CLIENT_CERTIFICATE=false
      export CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE=false
      export PIP_INDEX_URL=https://pypi.org/simple
      uv venv --clear --python 3.12 .venv_beam
      . .venv_beam/bin/activate
      uv pip install --default-index https://pypi.org/simple pip "apache-beam[gcp]"
      python cashier_abuse_pipeline.py \
        --runner=DataflowRunner \
        --project=${var.project_id} \
        --region=${var.gcp_region} \
        --staging_location=gs://${google_storage_bucket.streaming_bucket.name}/dataflow/staging \
        --temp_location=gs://${google_storage_bucket.streaming_bucket.name}/dataflow/temp \
        --template_location=gs://${google_storage_bucket.streaming_bucket.name}/dataflow/templates/cashier_abuse_template \
        --requirements_file=requirements.txt \
        --input_subscription=${google_pubsub_subscription.cashier_abuse_sub.id} \
        --dlq_topic=${google_pubsub_topic.pos_transactions_dlq.id} \
        --endpoint_name=${var.cashier_abuse_endpoint_name} \
        --bigtable_instance_id=operations-db \
        --bigtable_table_id=${google_bigtable_table.cashier_realtime_alerts.name} \
        --bigquery_dataset_id=${data.google_bigquery_dataset.cymbal_gold.dataset_id} \
        --bigquery_table_id=${google_bigquery_table.pos_anomaly_alerts.table_id}
      deactivate
    EOT
  }

  depends_on = [
    google_storage_bucket.streaming_bucket,
    google_pubsub_subscription.cashier_abuse_sub,
    google_pubsub_topic.pos_transactions_dlq,
    google_bigtable_table.cashier_realtime_alerts,
    google_bigquery_table.pos_anomaly_alerts,
    google_project_iam_member.cymbal_sa_streaming_roles,
  ]
}

resource "google_dataflow_job" "cashier_abuse_job" {
  name                    = "cashier-abuse-streaming-inference"
  project                 = var.project_id
  region                  = var.gcp_region
  template_gcs_path       = "gs://${google_storage_bucket.streaming_bucket.name}/dataflow/templates/cashier_abuse_template"
  temp_gcs_location       = "gs://${google_storage_bucket.streaming_bucket.name}/dataflow/temp"
  service_account_email   = data.google_service_account.cymbal_sa_data.email
  network                 = data.google_compute_network.cymbal_network.name
  subnetwork              = "regions/${var.gcp_region}/subnetworks/${data.google_compute_subnetwork.cymbal_subnet.name}"
  ip_configuration        = "WORKER_IP_PRIVATE"
  enable_streaming_engine = true
  max_workers             = 2
  on_delete               = "cancel"

  depends_on = [
    terraform_data.stage_cashier_abuse_template,
    google_project_iam_member.cymbal_sa_streaming_roles,
  ]
}

# ---------------------------------------------------------------------
# Challenge 2.3: Operational Activation (Reverse ETL) Using BigQuery Continuous Queries
# ---------------------------------------------------------------------

resource "google_bigquery_reservation" "continuous_reservation" {
  name              = "continuous-query-reservation"
  project           = var.project_id
  location          = var.gcp_region
  edition           = "ENTERPRISE"
  slot_capacity     = 50
  ignore_idle_slots = false

  autoscale {
    max_slots = 150
  }
}

resource "google_bigquery_reservation_assignment" "continuous_assignment" {
  project     = var.project_id
  location    = var.gcp_region
  assignee    = "projects/${var.project_id}"
  job_type    = "CONTINUOUS"
  reservation = google_bigquery_reservation.continuous_reservation.id
}

resource "google_bigtable_table" "pos_transactions_enriched" {
  name                = "pos_transactions_enriched"
  instance_name       = "operations-db"
  project             = var.project_id
  deletion_protection = "UNPROTECTED"

  column_family {
    family = "tx"
  }

  column_family {
    family = "alerts"
  }
}

resource "google_bigtable_app_profile" "bq_continuous_export" {
  instance       = "operations-db"
  app_profile_id = "bq-continuous-export"
  project        = var.project_id
  ignore_warnings = true

  single_cluster_routing {
    cluster_id                 = "operations-cluster"
    allow_transactional_writes = true
  }

  standard_isolation {
    priority = "PRIORITY_LOW"
  }

  depends_on = [
    google_bigtable_table.pos_transactions_enriched,
  ]
}

# Wait 90 seconds for BigQuery CONTINUOUS reservation assignment & IAM propagation
resource "time_sleep" "wait_for_continuous_reservation" {
  create_duration = "90s"

  depends_on = [
    google_bigquery_reservation_assignment.continuous_assignment,
    google_bigtable_app_profile.bq_continuous_export,
    google_bigquery_table.pos_transactions_gold,
    google_bigquery_table.pos_anomaly_alerts,
    google_project_iam_member.cymbal_sa_streaming_roles,
  ]
}

resource "time_static" "cq_job_suffix" {}

resource "google_bigquery_job" "cq_export_transactions" {
  provider = google-beta
  project  = var.project_id
  location = var.gcp_region
  job_id   = "cq_export_transactions_${time_static.cq_job_suffix.unix}"

  query {
    continuous         = true
    use_legacy_sql     = false
    create_disposition = ""
    write_disposition  = ""

    connection_properties {
      key   = "service_account"
      value = data.google_service_account.cymbal_sa_data.email
    }

    query = <<-EOT
      EXPORT DATA
      OPTIONS (
        format = 'CLOUD_BIGTABLE',
        uri = 'https://bigtable.googleapis.com/projects/${var.project_id}/instances/operations-db/appProfiles/${google_bigtable_app_profile.bq_continuous_export.app_profile_id}/tables/${google_bigtable_table.pos_transactions_enriched.name}',
        bigtable_options = '{"columnFamilies": [{"familyId": "tx", "encoding": "BINARY"}]}'
      )
      AS
      SELECT
        CONCAT(store_id, '#', transaction_id) AS rowkey,
        STRUCT(
          transaction_id,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', event_timestamp) AS event_timestamp,
          CAST(business_date AS STRING) AS business_date,
          store_id,
          pos_terminal_id,
          cashier_id,
          customer_id,
          customer_loyalty_tier,
          payment_method,
          payment_network,
          card_bin,
          is_contactless,
          currency,
          item_count,
          total_quantity,
          CAST(subtotal_amount AS FLOAT64) AS subtotal_amount,
          CAST(discount AS FLOAT64) AS discount,
          CAST(tax_amount AS FLOAT64) AS tax_amount,
          CAST(total AS FLOAT64) AS total,
          promo_code_applied,
          manual_discount_flag,
          TO_JSON_STRING(items) AS items
        ) AS tx
      FROM APPENDS(TABLE `${var.project_id}.${data.google_bigquery_dataset.cymbal_gold.dataset_id}.${google_bigquery_table.pos_transactions_gold.table_id}`, NULL)
    EOT
  }

  depends_on = [
    time_sleep.wait_for_continuous_reservation,
  ]
}

resource "google_bigquery_job" "cq_export_alerts" {
  provider = google-beta
  project  = var.project_id
  location = var.gcp_region
  job_id   = "cq_export_alerts_${time_static.cq_job_suffix.unix}"

  query {
    continuous         = true
    use_legacy_sql     = false
    create_disposition = ""
    write_disposition  = ""

    connection_properties {
      key   = "service_account"
      value = data.google_service_account.cymbal_sa_data.email
    }

    query = <<-EOT
      EXPORT DATA
      OPTIONS (
        format = 'CLOUD_BIGTABLE',
        uri = 'https://bigtable.googleapis.com/projects/${var.project_id}/instances/operations-db/appProfiles/${google_bigtable_app_profile.bq_continuous_export.app_profile_id}/tables/${google_bigtable_table.pos_transactions_enriched.name}'
      )
      AS
      SELECT
        CONCAT(store_id, '#', transaction_id) AS rowkey,
        CASE
          WHEN alert_type = 'order_anomaly' THEN JSON_OBJECT(
            'is_order_anomaly', 1,
            'order_anomaly_alert_id', alert_id,
            'order_anomaly_alert_source', alert_source,
            'order_anomaly_risk_score', risk_score,
            'order_anomaly_alert_ts', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', alert_ts)
          )
          WHEN alert_type = 'cashier_promo_abuse' THEN JSON_OBJECT(
            'is_cashier_promo_abuse', 1,
            'cashier_promo_abuse_alert_id', alert_id,
            'cashier_promo_abuse_alert_source', alert_source,
            'cashier_promo_abuse_risk_score', risk_score,
            'cashier_promo_abuse_alert_ts', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', alert_ts)
          )
        END AS alerts
      FROM APPENDS(TABLE `${var.project_id}.${data.google_bigquery_dataset.cymbal_gold.dataset_id}.${google_bigquery_table.pos_anomaly_alerts.table_id}`, NULL)
    EOT
  }

  depends_on = [
    time_sleep.wait_for_continuous_reservation,
    google_bigquery_job.cq_export_transactions,
  ]
}
