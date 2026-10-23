#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dataproc Serverless Multi-Day Inventory Reconciliation Pipeline
================================================================
Databricks ノートブック (01a_databricks_spark_inventory_reconciliation.ipynb) から
Google Cloud Dataproc Serverless (Runtime 2.3 / Spark 3.5) 向けに完全移行した
スタンドアロン PySpark パイプラインスクリプト。

主な移行ポイント:
1. dbutils.widgets / display() を排除し、標準の argparse と構造化ロギングへ移行。
2. AWS S3 / Glue カタログへの直接パス参照を排除し、BigQuery Spark コネクタ経由で
   BigLake Federated Catalog (argolisyo.cymbal-lakehouse.elevate_data) から読み込み。
3. 7段階のベクトル化在庫消込ロジック（5分刻み288スロット展開、午後5時ピーク cos^4 需要曲線、
   SHA-256 監査ハッシュ、安全在庫3個閾値の累積消化ウィンドウ、12カラム契約集約）を1対1で保持。
4. BigQuery Managed Iceberg テーブル (cymbal_gold.gold_inventory_reconciliation_ledger) へ
   .option("writeMethod", "direct") を用いて直接コミット。
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# 12カラムのデータ契約 (Conformed 12-Column Contract)
LEDGER_COLUMNS = [
    "business_date",
    "store_id",
    "store_name",
    "city",
    "item_id",
    "unit_price_usd",
    "opening_qty",
    "shelf_qty",
    "backroom_qty",
    "intraday_gross_revenue_usd",
    "est_cover_hours_remaining",
    "reconciliation_status",
]

STATUS_CRITICAL = "CRITICAL BURN SPIKE - STOCKOUT RISK"
STATUS_MONITOR = "MONITOR VELOCITY"
STATUS_NORMAL = "RECONCILED NORMAL HEALTH"

SAFETY_STOCK_UNITS = 3
PEAK_HOUR = 17
CITY_CANDIDATES = ["city", "store_city", "locality", "town", "municipality"]
CITY_FALLBACKS = ["country", "region", "global_region", "market"]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)-7s] [%(name)-24s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("DataprocReconciliation")


class ContractError(RuntimeError):
    """出力スキーマまたは行数がデータ契約（12カラム・ポジション数一致）から逸脱した場合に送出される例外。"""


class DataprocReconciliationPipeline:
    """
    Dataproc Serverless Multi-Day Inventory Reconciliation Engine
    =============================================================
    - BigQuery Lakehouse Federated Catalog から POS ストリーム・店舗在庫ベースライン・店舗マスタを取得。
    - 午後5時ピークを中心とした5分単位（1日288スロット）の在庫消化曲線をベクトル計算。
    - 12カラムの適合済み台帳を BigQuery Managed Iceberg テーブルへ直接書き込み（writeMethod=direct）。
    """

    def __init__(
        self,
        spark_session: SparkSession,
        catalog: str = "argolisyo.cymbal-lakehouse.elevate_data",
        target_table: str = "argolisyo.cymbal_gold.gold_inventory_reconciliation_ledger",
        gcs_staging_bucket: str = "argolisyo-module1-bucket",
        start_date: str = "2025-01-01",
        until_date: str = "2026-09-10",
        horizon_days: int = 1,
        slot_minutes: int = 5,
        critical_cover_hours: float = 6.0,
        monitor_cover_hours: float = 12.0,
        expr_passes: int = 1,
        write_mode: str = "overwrite",
        dry_run: bool = False,
    ):
        self.spark = spark_session
        self.catalog = catalog.strip("`").rstrip(".")
        self.target_table = target_table.strip("`")
        self.gcs_staging_bucket = gcs_staging_bucket.replace("gs://", "").strip("/")
        self.start_date = start_date
        self.until_date = until_date
        self.horizon_days = horizon_days
        self.slot_minutes = slot_minutes
        self.critical_cover_hours = critical_cover_hours
        self.monitor_cover_hours = monitor_cover_hours
        self.expr_passes = expr_passes
        self.write_mode = write_mode
        self.dry_run = dry_run

        # target_table からプロジェクトIDとマテリアライズ用データセットを抽出
        target_parts = self.target_table.replace(":", ".").split(".")
        if len(target_parts) >= 3:
            self.project_id = target_parts[0]
            self.materialization_dataset = target_parts[1]
        elif len(target_parts) == 2:
            self.project_id = self.catalog.split(".")[0]
            self.materialization_dataset = target_parts[0]
        else:
            self.project_id = "argolisyo"
            self.materialization_dataset = "cymbal_gold"

        if 60 % slot_minutes:
            raise ValueError(f"slot_minutes must divide 60, got {slot_minutes}")
        self.slots_per_hour = 60 // slot_minutes
        self.slots_per_day = 24 * self.slots_per_hour
        self.total_slots = horizon_days * self.slots_per_day
        self.hours_per_slot = slot_minutes / 60.0
        self.n_positions: Optional[int] = None

    @staticmethod
    def _pick(df: DataFrame, names: List[str]) -> Optional[str]:
        low = {c.lower(): c for c in df.columns}
        return next((low[n] for n in names if n in low), None)

    # 1. READ FROM BIGQUERY LAKEHOUSE FEDERATED CATALOG
    def read_sources(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        log.info(
            "[1/7 READ] Ingesting date range '%s' to '%s' from Federated Catalog: %s",
            self.start_date,
            self.until_date,
            self.catalog,
        )
        pos_where = ""
        if self.start_date and self.until_date and self.until_date.lower() != "all":
            pos_where = (
                f" WHERE CAST(business_date AS STRING) >= '{self.start_date}'"
                f" AND CAST(business_date AS STRING) <= '{self.until_date}'"
            )
        elif self.until_date and self.until_date.lower() != "all":
            pos_where = f" WHERE CAST(business_date AS STRING) <= '{self.until_date}'"

        def rd(table_name: str, where_clause: str = "") -> DataFrame:
            fq_table = f"{self.catalog}.{table_name}"

            # Primary 1: Spark BigQuery Connector Direct Table Option
            try:
                reader = (
                    self.spark.read.format("bigquery")
                    .option("parentProject", self.project_id)
                    .option("temporaryGcsBucket", self.gcs_staging_bucket)
                    .option("viewsEnabled", "true")
                    .option("materializationProject", self.project_id)
                    .option("materializationDataset", self.materialization_dataset)
                    .option("table", fq_table)
                )
                df_raw = reader.load()
                df_raw.createOrReplaceTempView(f"tmp_{table_name}")
                df = self.spark.sql(f"SELECT * FROM tmp_{table_name}{where_clause}")
                df.take(1)
                log.info(
                    "  ✅ Loaded table '%s' via Spark BigQuery Connector table option: `%s`",
                    table_name,
                    fq_table,
                )
                return df
            except Exception as e1:
                log.info(
                    "  ℹ️ Direct table option fallback for '%s' (%s). Trying BigQuery SQL query option...",
                    fq_table,
                    e1,
                )

            # Primary 2: Spark BigQuery Connector Query Option (supports 4-part federated catalog names)
            try:
                query_sql = f"SELECT * FROM `{fq_table}`{where_clause}"
                df = (
                    self.spark.read.format("bigquery")
                    .option("parentProject", self.project_id)
                    .option("temporaryGcsBucket", self.gcs_staging_bucket)
                    .option("viewsEnabled", "true")
                    .option("materializationProject", self.project_id)
                    .option("materializationDataset", self.materialization_dataset)
                    .option("query", query_sql)
                    .load()
                )
                df.take(1)
                log.info(
                    "  ✅ Loaded table '%s' via Spark BigQuery Connector query option: `%s`",
                    table_name,
                    fq_table,
                )
                return df
            except Exception as e2:
                log.warning(
                    "  ⚠️ Query option fallback for '%s': %s", fq_table, e2
                )
                raise RuntimeError(
                    f"Could not load table '{table_name}' from Federated Catalog: {fq_table}"
                ) from e2

        bronze = rd("bronze_pos_stream_events", pos_where)
        inv = rd("silver_store_inventory")
        nodes = rd("store_nodes")
        return bronze, inv, nodes

    # 2. SILVER POS TRANSFORMATION
    def silver_pos(
        self, bronze: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        log.info(
            "[2/7 SILVER POS] Processing POS transactions for range: %s to %s",
            self.start_date,
            self.until_date,
        )
        have = {c.lower() for c in bronze.columns}
        order = [
            F.col(c).desc_nulls_last()
            for c in ("_ingested_at", "_msk_message_id")
            if c in have
        ] or [F.lit(1)]

        # items カラムが文字列 (JSON) の場合は ArrayType(StructType) へパースし、既に配列の場合はそのまま利用
        if "items" in bronze.columns and str(
            bronze.schema["items"].dataType
        ).lower().startswith("string"):
            item_schema = ArrayType(
                StructType(
                    [
                        StructField("line_seq", IntegerType(), True),
                        StructField("item_id", StringType(), True),
                        StructField("item_name", StringType(), True),
                        StructField("category", StringType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("unit_price", DoubleType(), True),
                        StructField("unit_price_usd", DoubleType(), True),
                        StructField("total_item_price", DoubleType(), True),
                        StructField("item_discount", DoubleType(), True),
                        StructField("item_net_amount", DoubleType(), True),
                    ]
                )
            )
            bronze = bronze.withColumn(
                "items", F.from_json(F.col("items"), item_schema)
            )

        txn = (
            bronze.withColumn(
                "_rk",
                F.row_number().over(
                    Window.partitionBy("transaction_id").orderBy(*order)
                ),
            )
            .filter(F.col("_rk") == 1)
            .withColumn(
                "event_ts",
                F.coalesce(
                    F.to_timestamp(
                        F.col("event_timestamp").cast("string"),
                        "yyyy-MM-dd HH:mm:ss",
                    ),
                    F.to_timestamp(F.col("event_timestamp")),
                ),
            )
            .withColumn(
                "business_dt",
                F.coalesce(
                    F.to_date(
                        F.col("business_date").cast("string"), "yyyy-MM-dd"
                    ),
                    F.to_date(F.col("business_date")),
                    F.to_date(F.col("event_ts")),
                ),
            )
            .filter(
                F.col("transaction_id").isNotNull()
                & F.col("store_id").isNotNull()
            )
        )

        if self.start_date:
            txn = txn.filter(
                F.col("business_dt") >= F.to_date(F.lit(self.start_date))
            )
        if self.until_date and self.until_date.lower() != "all":
            txn = txn.filter(
                F.col("business_dt") <= F.to_date(F.lit(self.until_date))
            )

        dates_df = txn.select(
            F.col("business_dt").alias("business_date")
        ).distinct()

        daily_store_revenue = (
            txn.groupBy("business_dt", "store_id")
            .agg(
                F.round(
                    F.sum(
                        F.coalesce(
                            F.col("total_amount_usd").cast("double"),
                            F.lit(0.0),
                        )
                    ),
                    2,
                ).alias("intraday_gross_revenue_usd")
            )
            .withColumnRenamed("business_dt", "business_date")
        )

        lines = (
            txn.select(
                "business_dt", "store_id", F.explode_outer("items").alias("ln")
            )
            .select(
                F.col("business_dt").alias("business_date"),
                F.col("store_id"),
                F.col("ln.item_id").alias("item_id"),
                F.col("ln.quantity").cast("double").alias("qty"),
            )
            .filter(F.col("item_id").isNotNull())
        )

        daily_sku_units = lines.groupBy(
            "business_date", "store_id", "item_id"
        ).agg(F.greatest(F.sum("qty"), F.lit(0.0)).alias("daily_units"))

        return dates_df, daily_store_revenue, daily_sku_units

    # 3. POSITION GRID
    def positions(
        self,
        inv: DataFrame,
        nodes: DataFrame,
        dates_df: DataFrame,
        daily_store_revenue: DataFrame,
        daily_sku_units: DataFrame,
    ) -> DataFrame:
        log.info("[3/7 POSITIONS] Building Grid: (dates x store_id x item_id)")
        city_col = self._pick(nodes, CITY_CANDIDATES) or self._pick(
            nodes, CITY_FALLBACKS
        )
        city_expr = F.col(city_col) if city_col else F.lit(None)
        dim = nodes.select(
            F.col("store_id"),
            F.col("store_name").cast("string").alias("store_name"),
            city_expr.cast("string").alias("city"),
        ).dropDuplicates(["store_id"])

        inv_baseline = inv.dropDuplicates(["store_id", "item_id"])

        base_inv = (
            inv_baseline.drop("store_name", "city")
            .join(F.broadcast(dim), "store_id", "inner")
            .withColumn(
                "on_hand",
                (
                    F.coalesce(F.col("shelf_qty"), F.lit(0))
                    + F.coalesce(F.col("backroom_qty"), F.lit(0))
                ).cast("double"),
            )
        )

        grid = dates_df.crossJoin(F.broadcast(base_inv))

        pos = (
            grid.join(daily_store_revenue, ["business_date", "store_id"], "left")
            .join(
                daily_sku_units,
                ["business_date", "store_id", "item_id"],
                "left",
            )
            .withColumn(
                "intraday_gross_revenue_usd",
                F.coalesce(
                    F.col("intraday_gross_revenue_usd"), F.lit(0.0)
                ).cast("double"),
            )
            .withColumn(
                "daily_units",
                F.coalesce(F.col("daily_units"), F.lit(0.0)).cast("double"),
            )
            .withColumn(
                "pos_key",
                F.concat_ws(
                    "~",
                    F.col("business_date").cast("string"),
                    F.col("store_id"),
                    F.col("item_id"),
                ),
            )
        )

        self.n_positions = pos.count()
        log.info(
            "  Grid: %s positions across evaluated horizon.",
            f"{self.n_positions:,}",
        )
        return pos

    # 4. TIME FABRIC EXPANSION
    def fabric(self, pos: DataFrame) -> DataFrame:
        n = self.n_positions * self.total_slots
        log.info(
            "[4/7 FABRIC] %s positions x %s slots = %s calculation rows",
            f"{self.n_positions:,}",
            f"{self.total_slots:,}",
            f"{n:,}",
        )
        slots = self.spark.range(0, self.total_slots).withColumnRenamed(
            "id", "slot"
        )
        return (
            pos.crossJoin(F.broadcast(slots))
            .withColumn("slot", F.col("slot").cast("int"))
            .withColumn(
                "hour",
                F.pmod(
                    (F.col("slot") / F.lit(self.slots_per_hour)).cast("int"),
                    F.lit(24),
                ),
            )
        )

    # 5. DEMAND SHAPING & AUDIT HASH
    def project(self, fab: DataFrame) -> DataFrame:
        log.info(
            "[5/7 PROJECT] Vectorized 5:00 PM Peak Demand Shaping & Cryptographic Auditing"
        )
        phase = (F.col("hour") - F.lit(float(PEAK_HOUR))) * F.lit(
            3.141592653589793 / 12.0
        )
        shape = (
            F.pow(F.cos(phase), F.lit(4.0)) * F.lit(1.85) + F.lit(0.15)
        ) / F.lit(0.84375)

        df = fab.withColumn(
            "slot_demand",
            F.col("daily_units") * shape / F.lit(float(self.slots_per_day)),
        )
        chain = F.concat_ws("|", F.col("pos_key"), F.col("slot").cast("string"))
        for i in range(self.expr_passes):
            chain = F.sha2(F.concat_ws("#", chain, F.lit(f"s{i}")), 256)

        return df.withColumn("audit_hash", chain)

    # 6. CUMULATIVE DEPLETION WINDOW
    def window(self, df: DataFrame) -> DataFrame:
        log.info(
            "[6/7 WINDOW] Cumulative Depletion Window (Safety Stock <= %d units)",
            SAFETY_STOCK_UNITS,
        )
        w_pos = (
            Window.partitionBy("pos_key")
            .orderBy("slot")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        return (
            df.withColumn("burned", F.sum("slot_demand").over(w_pos))
            .withColumn(
                "projected_on_hand",
                F.greatest(F.lit(0.0), F.col("on_hand") - F.col("burned")),
            )
            .withColumn(
                "is_out",
                (
                    F.col("projected_on_hand")
                    <= F.lit(float(SAFETY_STOCK_UNITS))
                ).cast("int"),
            )
        )

    # 7. DAILY GRAIN SYNTHESIS
    def collapse(self, df: DataFrame) -> DataFrame:
        log.info("[7/7 COLLAPSE] Synthesizing Daily Reconciliation Ledger")
        carry = [
            "business_date",
            "store_id",
            "store_name",
            "city",
            "item_id",
            "unit_price_usd",
            "opening_qty",
            "shelf_qty",
            "backroom_qty",
            "intraday_gross_revenue_usd",
            "on_hand",
            "daily_units",
        ]

        return (
            df.groupBy("pos_key")
            .agg(
                *[F.first(c, ignorenulls=True).alias(c) for c in carry],
                F.min(F.when(F.col("is_out") == 1, F.col("slot"))).alias(
                    "out_slot"
                ),
            )
            .withColumn("business_date", F.to_date(F.col("business_date")))
            .withColumn("store_id", F.col("store_id").cast("string"))
            .withColumn("store_name", F.col("store_name").cast("string"))
            .withColumn("city", F.col("city").cast("string"))
            .withColumn("item_id", F.col("item_id").cast("string"))
            .withColumn("unit_price_usd", F.col("unit_price_usd").cast("double"))
            .withColumn("opening_qty", F.col("opening_qty").cast("long"))
            .withColumn("shelf_qty", F.col("shelf_qty").cast("long"))
            .withColumn("backroom_qty", F.col("backroom_qty").cast("long"))
            .withColumn(
                "intraday_gross_revenue_usd",
                F.col("intraday_gross_revenue_usd").cast("double"),
            )
            .withColumn("hourly_burn", F.col("daily_units") / F.lit(24.0))
            .withColumn(
                "est_cover_hours_remaining",
                F.round(
                    F.coalesce(
                        F.col("out_slot") * F.lit(self.hours_per_slot),
                        F.when(
                            F.col("hourly_burn") > 0,
                            F.col("on_hand") / F.col("hourly_burn"),
                        ),
                    ),
                    1,
                ).cast("double"),
            )
            .withColumn(
                "reconciliation_status",
                F.when(
                    F.col("est_cover_hours_remaining")
                    <= F.lit(self.critical_cover_hours),
                    F.lit(STATUS_CRITICAL),
                )
                .when(
                    F.col("est_cover_hours_remaining")
                    <= F.lit(self.monitor_cover_hours),
                    F.lit(STATUS_MONITOR),
                )
                .otherwise(F.lit(STATUS_NORMAL))
                .cast("string"),
            )
            .select(*LEDGER_COLUMNS)
            .orderBy(
                F.col("business_date").desc(),
                F.col("est_cover_hours_remaining").asc_nulls_last(),
            )
        )

    # REPORTING & CONTRACT VALIDATION
    def report(self, led: DataFrame) -> int:
        n = led.count()
        if list(led.columns) != LEDGER_COLUMNS:
            raise ContractError(
                f"Schema drift detected: expected {LEDGER_COLUMNS}, got {list(led.columns)}"
            )
        if self.n_positions is not None and n != self.n_positions:
            raise ContractError(
                f"Row drift detected: {n:,} rows out vs {self.n_positions:,} positions in"
            )
        log.info(
            "✅ Contract Verified: 12 conformed columns, %s positions in -> %s rows out.",
            f"{self.n_positions:,}",
            f"{n:,}",
        )

        print("\n" + "=" * 128)
        print(
            f"FULL HISTORICAL INVENTORY RECONCILIATION LEDGER ($ USD) [{self.start_date} to {self.until_date}]"
        )
        print(
            f"Execution Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        print("=" * 128)

        cols = [
            "business_date",
            "store_id",
            "city",
            "item_id",
            "unit_price_usd",
            "shelf_qty",
            "est_cover_hours_remaining",
            "reconciliation_status",
        ]
        led.select(*cols).show(15, truncate=False)

        print("-" * 128)
        print(
            f"Reconciliation Status Distribution for Full Horizon ({self.start_date} to {self.until_date}):"
        )
        led.groupBy("reconciliation_status").agg(
            F.count("*").alias("row_count"),
            F.round(F.sum("intraday_gross_revenue_usd"), 2).alias(
                "total_revenue_usd"
            ),
            F.round(F.avg("est_cover_hours_remaining"), 1).alias(
                "avg_cover_hours"
            ),
        ).orderBy(F.col("row_count").desc()).show(truncate=False)
        print("=" * 128 + "\n")
        return n

    # WRITE TO BIGQUERY MANAGED ICEBERG TABLE (writeMethod=direct)
    def write(self, led: DataFrame) -> None:
        if self.dry_run:
            log.info(
                "[DRY-RUN] Dry run enabled. Skipping push to BigQuery Managed Iceberg table."
            )
            return

        log.info(
            "Committing reconciled ledger directly to BigQuery Managed Iceberg table: %s (mode=%s, writeMethod=direct)...",
            self.target_table,
            self.write_mode,
        )
        try:
            (
                led.write.format("bigquery")
                .mode(self.write_mode)
                .option("table", self.target_table)
                .option("writeMethod", "direct")
                .option("temporaryGcsBucket", self.gcs_staging_bucket)
                .option("parentProject", self.project_id)
                .save()
            )
            log.info(
                "  ✅ Successfully committed %s rows to BigQuery Managed Iceberg table: %s",
                f"{self.n_positions or 0:,}",
                self.target_table,
            )
        except Exception as e:
            log.error(
                "❌ Failed write to BigQuery Managed Iceberg table %s: %s",
                self.target_table,
                e,
            )
            raise e

    # RUN PIPELINE
    def run(self) -> DataFrame:
        t0 = time.time()
        print("=" * 96)
        print(
            f"🚀 Starting Dataproc Serverless Multi-Day Inventory Reconciliation Pipeline | {self.start_date} -> {self.until_date}"
        )
        print(f"🏛️ Federated Catalog Dataset    : {self.catalog}")
        print(f"📋 Target BigQuery Iceberg Table: {self.target_table}")
        print(f"🪣 GCS Temporary Staging Bucket : gs://{self.gcs_staging_bucket}")
        print(
            f"⚡ Cover Thresholds             : Critical <= {self.critical_cover_hours:.1f}h | Monitor <= {self.monitor_cover_hours:.1f}h"
        )
        print("=" * 96)

        bronze, inv, nodes = self.read_sources()
        dates_df, store_revenue, sku_units = self.silver_pos(bronze)
        pos = self.positions(inv, nodes, dates_df, store_revenue, sku_units)
        ledger = self.collapse(self.window(self.project(self.fabric(pos))))

        self.report(ledger)
        self.write(ledger)
        duration = time.time() - t0
        print(
            f"\n✅ Pipeline core stages completed successfully in {duration:.2f} seconds."
        )
        return ledger


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cymbal Retail Multi-Day Inventory Reconciliation Pipeline (Dataproc Serverless 2.3)"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="argolisyo.cymbal-lakehouse.elevate_data",
        help="BigQuery Lakehouse Federated Catalog dataset (e.g. <PROJECT_ID>.cymbal-lakehouse.elevate_data)",
    )
    parser.add_argument(
        "--target-table",
        type=str,
        default="argolisyo.cymbal_gold.gold_inventory_reconciliation_ledger",
        help="BigQuery Managed Iceberg target table (e.g. <PROJECT_ID>.cymbal_gold.gold_inventory_reconciliation_ledger)",
    )
    parser.add_argument(
        "--gcs-staging-bucket",
        type=str,
        default="argolisyo-module1-bucket",
        help="GCS temporary staging bucket for BigQuery/Iceberg commits",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-01-01",
        help="Evaluation start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--until-date",
        type=str,
        default="2026-09-10",
        help="Evaluation end date (YYYY-MM-DD or ALL)",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=1,
        help="Intraday simulation horizon in days (default: 1)",
    )
    parser.add_argument(
        "--slot-minutes",
        type=int,
        default=5,
        help="Intraday slot granularity in minutes (default: 5 -> 288 slots/day)",
    )
    parser.add_argument(
        "--critical-cover-hours",
        type=float,
        default=6.0,
        help="Critical stockout threshold in hours (default: 6.0)",
    )
    parser.add_argument(
        "--monitor-cover-hours",
        type=float,
        default=12.0,
        help="Monitor velocity threshold in hours (default: 12.0)",
    )
    parser.add_argument(
        "--expr-passes",
        type=int,
        default=1,
        help="SHA-256 audit hash passes (default: 1)",
    )
    parser.add_argument(
        "--write-mode",
        type=str,
        default="overwrite",
        choices=["overwrite", "append"],
        help="Spark write mode (default: overwrite)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate contract without writing to BigQuery",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    bucket_clean = args.gcs_staging_bucket.replace("gs://", "").strip("/")

    spark = (
        SparkSession.builder.appName(
            "Cymbal-Inventory-Reconciliation-Pipeline"
        )
        .config("temporaryGcsBucket", bucket_clean)
        .config("viewsEnabled", "true")
        .getOrCreate()
    )

    pipeline = DataprocReconciliationPipeline(
        spark_session=spark,
        catalog=args.catalog,
        target_table=args.target_table,
        gcs_staging_bucket=bucket_clean,
        start_date=args.start_date,
        until_date=args.until_date,
        horizon_days=args.horizon_days,
        slot_minutes=args.slot_minutes,
        critical_cover_hours=args.critical_cover_hours,
        monitor_cover_hours=args.monitor_cover_hours,
        expr_passes=args.expr_passes,
        write_mode=args.write_mode,
        dry_run=args.dry_run,
    )
    pipeline.run()


if __name__ == "__main__":
    main()
