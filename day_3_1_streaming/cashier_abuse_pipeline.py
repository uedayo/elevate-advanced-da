#!/usr/bin/env python3
"""Challenge 2.2: Real-Time Cashier Discount Abuse Detection Pipeline.

Apache Beam / Dataflow streaming pipeline that:
1. Reads POS transactions from Pub/Sub subscription `pos-transactions-cashier-abuse-sub`.
2. Maintains a stateful 1-hour rolling window per `cashier_id` using BagStateSpec
   to compute the 6 windowed cashier metrics (`cashier_1h_*`) upon each arriving transaction.
3. Invokes Vertex AI endpoint `cashier-abuse-endpoint` for real-time inference.
4. Dual-Sink Routing:
   - Writes ALL scored records and 1h rolling metrics to Cloud Bigtable
     `operations-db:cashier_realtime_alerts` (with 8-byte big-endian binary INT64/FLOAT64
     encoding compatible with Bigtable GoogleSQL `TO_INT64` / `TO_FLOAT64`).
   - Writes ONLY high-risk alerts (`predicted_label == "1"` and `risk_score >= 0.7`)
     to BigQuery `cymbal_gold.pos_anomaly_alerts`.
5. Routes any malformed or failed messages at any stage in their original form
   to the Dead-Letter Pub/Sub topic `pos-transactions-dlq` (which lands in `cymbal_bronze.dlq`).
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import struct
import time
import uuid

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import StrUtf8Coder
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.userstate import BagStateSpec


def parse_iso_timestamp(ts_str: str) -> float:
  """ISO-8601 タイムスタンプ文字列を UTC エポック秒（float）に変換する。"""
  clean = ts_str.strip()
  if clean.endswith("Z"):
    clean = clean[:-1] + "+00:00"
  dt = datetime.fromisoformat(clean)
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
  return dt.timestamp()


class ParseTransactionDoFn(beam.DoFn):
  """Pub/Sub メッセージをパースし、(cashier_id, (raw_str, parsed_dict)) を出力する。"""

  def process(self, element):
    raw_bytes = element.data if hasattr(element, "data") else element
    try:
      raw_str = (
          raw_bytes.decode("utf-8")
          if isinstance(raw_bytes, (bytes, bytearray))
          else str(raw_bytes)
      )
      txn = json.loads(raw_str)
      cashier_id = str(txn["cashier_id"])
      _ = str(txn["store_id"])
      _ = str(txn["transaction_id"])
      _ = parse_iso_timestamp(str(txn["event_timestamp"]))
      yield (cashier_id, (raw_str, txn))
    except Exception as exc:
      logging.error("Failed to parse POS transaction message: %s", exc)
      raw_out = (
          raw_bytes
          if isinstance(raw_bytes, bytes)
          else str(raw_bytes).encode("utf-8")
      )
      yield pvalue.TaggedOutput(
          "dlq",
          PubsubMessage(
              data=raw_out,
              attributes={"error_stage": "parse", "error_msg": str(exc)[:250]},
          ),
      )


class StatefulCashierWindowDoFn(beam.DoFn):
  """キャッシャー単位の直近1時間移動ウィンドウ特徴量を BagStateSpec で計算する。"""

  TXN_BAG = BagStateSpec("txn_history", StrUtf8Coder())

  def process(self, element, txn_bag=beam.DoFn.StateParam(TXN_BAG)):
    cashier_id, (raw_str, txn) = element
    try:
      current_ts = parse_iso_timestamp(str(txn["event_timestamp"]))
      cutoff_ts = current_ts - 3600.0

      # 既存の状態を読み込み、直近1時間（3600秒）以内のトランザクションのみ保持する
      valid_records = []
      for item_json in txn_bag.read():
        try:
          rec = json.loads(item_json)
          if float(rec.get("ts", 0.0)) >= cutoff_ts:
            valid_records.append(rec)
        except Exception:
          continue

      current_summary = {
          "txn_id": str(txn.get("transaction_id", "")),
          "ts": current_ts,
          "discount": float(txn.get("discount", 0.0) or 0.0),
          "subtotal": float(txn.get("subtotal_amount", 0.0) or 0.0),
          "manual_flag": bool(txn.get("manual_discount_flag", False)),
      }
      valid_records.append(current_summary)

      # 状態を更新（直近1時間分のみ書き戻す）
      txn_bag.clear()
      for rec in valid_records:
        txn_bag.add(json.dumps(rec))

      txn_count = len(valid_records)
      promo_count = sum(1 for r in valid_records if float(r["discount"]) > 0.0)
      promo_rate = (
          float(promo_count) / float(txn_count) if txn_count > 0 else 0.0
      )
      manual_override_count = sum(
          1 for r in valid_records if bool(r["manual_flag"])
      )
      total_discount_usd = float(
          sum(float(r["discount"]) for r in valid_records)
      )
      total_subtotal_usd = float(
          sum(float(r["subtotal"]) for r in valid_records)
      )
      avg_discount_pct = (
          (total_discount_usd / total_subtotal_usd) * 100.0
          if total_subtotal_usd > 0.0
          else 0.0
      )

      features = {
          "cashier_1h_txn_count": int(txn_count),
          "cashier_1h_promo_count": int(promo_count),
          "cashier_1h_promo_rate": float(promo_rate),
          "cashier_1h_manual_override_count": int(manual_override_count),
          "cashier_1h_total_discount_usd": float(total_discount_usd),
          "cashier_1h_avg_discount_pct": float(avg_discount_pct),
      }
      yield (raw_str, txn, features)
    except Exception as exc:
      logging.error(
          "Stateful window calculation failed for cashier %s: %s",
          cashier_id,
          exc,
      )
      yield pvalue.TaggedOutput(
          "dlq",
          PubsubMessage(
              data=raw_str.encode("utf-8"),
              attributes={
                  "error_stage": "stateful_window",
                  "error_msg": str(exc)[:250],
              },
          ),
      )


class PredictAndSinkDoFn(beam.DoFn):
  """Vertex AI エンドポイントで推論を実行し、Bigtable と BigQuery に書き込む。"""

  def __init__(
      self,
      project_id: str,
      region: str,
      endpoint_name: str,
      bigtable_instance_id: str,
      bigtable_table_id: str,
      bigquery_dataset_id: str,
      bigquery_table_id: str,
  ):
    self.project_id = project_id
    self.region = region
    self.endpoint_name = endpoint_name
    self.bigtable_instance_id = bigtable_instance_id
    self.bigtable_table_id = bigtable_table_id
    self.bigquery_dataset_id = bigquery_dataset_id
    self.bigquery_table_id = bigquery_table_id
    self.endpoint = None
    self.bt_table = None
    self.bq_client = None
    self.bq_table_ref = None

  def setup(self):
    from google.cloud import aiplatform
    from google.cloud import bigquery
    from google.cloud import bigtable

    aiplatform.init(project=self.project_id, location=self.region)
    endpoint_path = (
        f"projects/{self.project_id}/locations/{self.region}"
        f"/endpoints/{self.endpoint_name}"
    )
    self.endpoint = aiplatform.Endpoint(endpoint_name=endpoint_path)

    bt_client = bigtable.Client(project=self.project_id, admin=False)
    bt_instance = bt_client.instance(self.bigtable_instance_id)
    self.bt_table = bt_instance.table(self.bigtable_table_id)

    self.bq_client = bigquery.Client(project=self.project_id)
    self.bq_table_ref = (
        f"{self.project_id}.{self.bigquery_dataset_id}.{self.bigquery_table_id}"
    )

  def process(self, element):
    raw_str, txn, features = element
    try:
      instance = {
          "cashier_1h_txn_count": int(features["cashier_1h_txn_count"]),
          "cashier_1h_promo_count": int(features["cashier_1h_promo_count"]),
          "cashier_1h_promo_rate": float(features["cashier_1h_promo_rate"]),
          "cashier_1h_manual_override_count": int(
              features["cashier_1h_manual_override_count"]
          ),
          "cashier_1h_total_discount_usd": float(
              features["cashier_1h_total_discount_usd"]
          ),
          "cashier_1h_avg_discount_pct": float(
              features["cashier_1h_avg_discount_pct"]
          ),
          "promo_code_applied": str(txn.get("promo_code_applied") or "NONE"),
          "manual_discount_flag": (
              "true" if bool(txn.get("manual_discount_flag")) else "false"
          ),
          "discount_amount": float(txn.get("discount", 0.0) or 0.0),
          "total_amount": float(txn.get("total", 0.0) or 0.0),
      }

      prediction_res = self.endpoint.predict(instances=[instance])
      pred = prediction_res.predictions[0]
      pred_label = str(pred["predicted_label_cashier_promo_abuse"])
      label_values = [
          str(v) for v in pred["label_cashier_promo_abuse_values"]
      ]
      label_probs = [float(p) for p in pred["label_cashier_promo_abuse_probs"]]
      idx_pos = label_values.index("1") if "1" in label_values else 0
      risk_score = float(label_probs[idx_pos])

      # 1. Bigtable への書き込み（全トランザクション対象）
      event_ts_str = str(txn["event_timestamp"])
      event_ts_micros = int(round(parse_iso_timestamp(event_ts_str) * 1_000_000))
      reverse_ts = 9223372036854775807 - event_ts_micros
      row_key = f"{txn['store_id']}#{txn['cashier_id']}#{reverse_ts:019d}"

      audit_status = "review" if risk_score >= 0.7 else "clear"
      bt_row = self.bt_table.direct_row(row_key.encode("utf-8"))
      bt_row.set_cell(
          "stats",
          b"cashier_1h_txn_count",
          struct.pack(">q", int(features["cashier_1h_txn_count"])),
      )
      bt_row.set_cell(
          "stats",
          b"cashier_1h_promo_count",
          struct.pack(">q", int(features["cashier_1h_promo_count"])),
      )
      bt_row.set_cell(
          "stats",
          b"cashier_1h_promo_rate",
          struct.pack(">d", float(features["cashier_1h_promo_rate"])),
      )
      bt_row.set_cell(
          "stats",
          b"cashier_1h_manual_override_count",
          struct.pack(
              ">q", int(features["cashier_1h_manual_override_count"])
          ),
      )
      bt_row.set_cell(
          "stats",
          b"cashier_1h_total_discount_usd",
          struct.pack(
              ">d", float(features["cashier_1h_total_discount_usd"])
          ),
      )
      bt_row.set_cell(
          "stats",
          b"cashier_1h_avg_discount_pct",
          struct.pack(">d", float(features["cashier_1h_avg_discount_pct"])),
      )
      bt_row.set_cell(
          "stats",
          b"risk_score",
          struct.pack(">d", float(risk_score)),
      )
      bt_row.set_cell(
          "stats",
          b"last_event_ts",
          event_ts_str.encode("utf-8"),
      )
      bt_row.set_cell(
          "flags",
          b"audit_status",
          audit_status.encode("utf-8"),
      )
      bt_row.commit()

      # 2. BigQuery への書き込み（高リスクアラート: label=="1" かつ risk_score >= 0.7）
      if pred_label == "1" and risk_score >= 0.7:
        subtotal = float(txn.get("subtotal_amount", 0.0) or 0.0)
        discount = float(txn.get("discount", 0.0) or 0.0)
        discount_pct = discount / subtotal if subtotal > 0.0 else 0.0
        alert_row = {
            "alert_id": (
                f"ALT-ABUSE-{txn['transaction_id']}-"
                f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
            ),
            "store_id": str(txn["store_id"]),
            "alert_type": "cashier_promo_abuse",
            "alert_source": "cashier_abuse_model",
            "transaction_id": str(txn["transaction_id"]),
            "cashier_id": str(txn["cashier_id"]),
            "discount_pct": float(discount_pct),
            "risk_score": float(risk_score),
            "alert_ts": event_ts_str,
        }
        errors = self.bq_client.insert_rows_json(self.bq_table_ref, [alert_row])
        if errors:
          raise RuntimeError(f"BigQuery insert_rows_json failed: {errors}")

      yield txn["transaction_id"]
    except Exception as exc:
      logging.error(
          "PredictAndSinkDoFn failed for txn %s: %s",
          txn.get("transaction_id", "UNKNOWN"),
          exc,
      )
      yield pvalue.TaggedOutput(
          "dlq",
          PubsubMessage(
              data=raw_str.encode("utf-8"),
              attributes={
                  "error_stage": "predict_and_sink",
                  "error_msg": str(exc)[:250],
              },
          ),
      )


def run(argv=None):
  parser = argparse.ArgumentParser(allow_abbrev=False)
  parser.add_argument(
      "--input_subscription",
      required=True,
      help="Full Pub/Sub subscription path: projects/<PROJECT>/subscriptions/<SUB>",
  )
  parser.add_argument(
      "--dlq_topic",
      required=True,
      help="Full Pub/Sub DLQ topic path: projects/<PROJECT>/topics/<TOPIC>",
  )
  parser.add_argument(
      "--endpoint_name",
      default="cashier-abuse-endpoint",
  )
  parser.add_argument(
      "--bigtable_instance_id",
      default="operations-db",
  )
  parser.add_argument(
      "--bigtable_table_id",
      default="cashier_realtime_alerts",
  )
  parser.add_argument(
      "--bigquery_dataset_id",
      default="cymbal_gold",
  )
  parser.add_argument(
      "--bigquery_table_id",
      default="pos_anomaly_alerts",
  )
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  gcp_options = pipeline_options.view_as(GoogleCloudOptions)

  with beam.Pipeline(options=pipeline_options) as p:
    raw_msgs = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
        subscription=known_args.input_subscription,
        with_attributes=True,
    )

    parsed_results = raw_msgs | "ParseTransactions" >> beam.ParDo(
        ParseTransactionDoFn()
    ).with_outputs("dlq", main="parsed")

    windowed_results = (
        parsed_results.parsed
        | "Compute1hCashierFeatures"
        >> beam.ParDo(StatefulCashierWindowDoFn()).with_outputs(
            "dlq", main="windowed"
        )
    )

    sink_results = (
        windowed_results.windowed
        | "PredictAndWriteSinks"
        >> beam.ParDo(
            PredictAndSinkDoFn(
                project_id=gcp_options.project,
                region=gcp_options.region,
                endpoint_name=known_args.endpoint_name,
                bigtable_instance_id=known_args.bigtable_instance_id,
                bigtable_table_id=known_args.bigtable_table_id,
                bigquery_dataset_id=known_args.bigquery_dataset_id,
                bigquery_table_id=known_args.bigquery_table_id,
            )
        ).with_outputs("dlq", main="success")
    )

    # 全ステージの DLQ メッセージを統合して Dead-Letter Topic に書き出す
    (
        (
            parsed_results.dlq,
            windowed_results.dlq,
            sink_results.dlq,
        )
        | "FlattenDLQ" >> beam.Flatten()
        | "WriteToDLQTopic"
        >> beam.io.WriteToPubSub(
            topic=known_args.dlq_topic,
            with_attributes=True,
        )
    )


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()
