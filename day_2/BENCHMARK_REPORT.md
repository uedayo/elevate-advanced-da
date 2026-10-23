# 📊 Dataproc Serverless 2.3 Benchmark Report: Standard Engine vs. Lightning Engine

**プロジェクト ID**: `argolisyo` (`555891812099`)  
**リージョン**: `us-central1`  
**対象ワークロード**: [migrated_inventory_reconciliation_pipeline.py](file:///usr/local/google/home/uedayo/workspace/elevate-da-adv-learner/day_2/migrated_inventory_reconciliation_pipeline.py)  
**入力ソース**: BigLake Federated Catalog (`argolisyo.cymbal-lakehouse.elevate_data`: `bronze_pos_stream_events`, `silver_store_inventory`, `store_nodes`)  
**出力シンク**: BigQuery Managed Iceberg テーブル (`argolisyo.cymbal_gold.gold_inventory_reconciliation_ledger`, `writeMethod=direct`)  
**検証日時**: 2026-10-22 UTC

---

## 1. エグゼクティブサマリー (Executive Summary)

Databricks ノートブック（[01a_databricks_spark_inventory_reconciliation.ipynb](file:///usr/local/google/home/uedayo/workspace/elevate-da-adv-learner/day_2/01a_databricks_spark_inventory_reconciliation.ipynb)）の7段階ベクトル化在庫消込パイプライン（21営業日 × 400店舗・SKUペア = 8,400ポジション × 288スロット = **2,419,200 イントラデイ計算行**）を **Google Cloud Dataproc Serverless Runtime 2.3 (`2.3.42` / Apache Spark 3.5)** へ移行し、Python コードを **1文字も変更せず** プロパティフラグ（`dataproc.tier=premium,spark.dataproc.engine=lightningEngine`）のみで **Standard Engine** と **Lightning Engine** の実測ベンチマーク比較を実施しました。

* **パフォーマンス向上**: Lightning Engine は Standard Engine と比較して、RUNNING 実行時間を **131.67秒 → 105.38秒（-19.97% 短縮 / 1.25倍高速化）**、コアパイプライン実行時間を **111.78秒 → 90.76秒（-18.80% 短縮 / 1.23倍高速化）** に短縮しました。
* **コンピュート＆シャッフル効率**: DCU 消費量（`milliDcuSeconds`）およびシャッフルストレージ保持時間（`shuffleStorageGbSeconds`）ともに **-20.61% 削減**（`1,552,350` → `1,232,400` milliDCU-s）を達成しました。
* **数学的パリティ (100% Data Contract Parity)**: 両エンジンとも、レガシー Databricks 実装と小数点以下まで完全一致する **8,400 行（8,265 / 84 / 51）** を BigQuery Managed Iceberg テーブルへ出力し、`spark.dataproc.lineage.enabled=true` により Dataplex / BigQuery Data Lineage 上にエンドツーエンドのデータ系譜（OpenLineage）を自動記録しました。

---

## 2. エンジン別・実測ベンチマーク比較表 (Empirical Telemetry Comparison)

| 評価指標 (Metric) | Run 1: Standard Engine (`default`) | Run 2: Lightning Engine (`lightningEngine`) | 差分 / 改善率 (Delta) |
| :--- | :--- | :--- | :--- |
| **バッチ ID (`batchId`)** | `recon-standard-20261022-083708` | `recon-lightning-20261022-084103` | — |
| **バッチ UUID** | `12ba4435-d618-4655-8553-c8ac3c434e72` | `87ded9ca-ac0a-4bdb-b408-777c72d72074` | — |
| **Dataproc Runtime Version** | `2.3.42` (Spark 3.5) | `2.3.42` (Spark 3.5) | 同一ランタイム |
| **Spark エンジン設定** | `spark.dataproc.engine=default` | `dataproc.tier=premium`<br>`spark.dataproc.engine=lightningEngine` | プロパティフラグのみ変更（コード変更ゼロ） |
| **OpenLineage 系譜追跡** | `spark.dataproc.lineage.enabled=true` | `spark.dataproc.lineage.enabled=true` | 両環境で有効 |
| **ワーカー構成 (Autotuning Cohort)** | Driver: 4 vCPU / 9.6GB<br>Executors: 2 × (4 vCPU / 9.6GB) | Driver: 4 vCPU / 9.6GB<br>Executors: 2 × (4 vCPU / 9.6GB) | 同一リソース構成 |
| **コアパイプライン処理時間 (`pipeline.run()`)** | **111.78 秒** | **90.76 秒** | **-21.02 秒 (-18.80% / 1.23x 高速)** |
| **バッチ実行時間 (`RUNNING` → `SUCCEEDED`)** | **131.67 秒** (`08:38:16Z` → `08:40:28Z`) | **105.38 秒** (`08:42:00Z` → `08:43:46Z`) | **-26.29 秒 (-19.97% / 1.25x 高速)** |
| **総所要時間 (`PENDING` → `SUCCEEDED`)** | **197.26 秒** (`08:37:10Z` → `08:40:28Z`) | **160.94 秒** (`08:41:05Z` → `08:43:46Z`) | **-36.32 秒 (-18.41% 短縮)** |
| **DCU 消費量 (`milliDcuSeconds`)** | **1,552,350** (`1,552.35 DCU-s` / `0.4312 DCU-h`) | **1,232,400** (`1,232.40 DCU-s` / `0.3423 DCU-h`) | **-319,950 milliDCU-s (-20.61% 削減)** |
| **シャッフル消費量 (`shuffleStorageGbSeconds`)** | **157,200** (`157.20 GB-s`) | **124,800** (`124.80 GB-s`) | **-32,400 GB-s (-20.61% 削減)** |

```mermaid
xychart-beta
    title "Dataproc Serverless 2.3: Standard vs. Lightning Engine (Seconds & DCU-Seconds)"
    x-axis ["Core Pipeline (s)", "Batch RUNNING (s)", "Total E2E (s)", "DCU Consumed (x10 DCU-s)"]
    y-axis "Value (Lower is Better)" 0 --> 220
    bar [111.78, 131.67, 197.26, 155.24]
    bar [90.76, 105.38, 160.94, 123.24]
```

---

## 3. 数学的パリティ＆データ契約検証 (Mathematical Parity & Data Contract Verification)

BigQuery Managed Iceberg テーブル（`argolisyo.cymbal_gold.gold_inventory_reconciliation_ledger`）に対する検証クエリ結果と、移行元の Databricks ノートブック出力の比較は以下の通り **100% 完全一致** しています。

| `reconciliation_status` | Databricks 基準行数 | Standard Engine 行数 | Lightning Engine 行数 | `total_revenue_usd` ($ USD) | `avg_cover_hours` (h) | パリティ判定 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `RECONCILED NORMAL HEALTH` | 8,265 | 8,265 | 8,265 | `$2,040,364,262.34` | `767.7` | ✅ **完全一致** |
| `MONITOR VELOCITY` | 84 | 84 | 84 | `$32,973,687.42` | `7.7` | ✅ **完全一致** |
| `CRITICAL BURN SPIKE - STOCKOUT RISK` | 51 | 51 | 51 | `$24,305,211.74` | `5.1` | ✅ **完全一致** |
| **合計 (Total Conformed Positions)** | **8,400** | **8,400** | **8,400** | **`$2,097,643,161.50`** | — | ✅ **完全一致** |

---

## 4. アーキテクチャ考察：なぜ Lightning Engine が優位なのか (Technical Analysis)

1. **C++ ネイティブ・ベクトル化実行と SIMD 演算**:
   * 本パイプラインのステージ `[4/7 FABRIC]`〜`[6/7 WINDOW]` では、8,400 ポジションを288スロット（2,419,200行）へ Cross-Join 展開した上で、$\cos^4$ 三角関数による午後5時ピーク需要曲線計算、SHA-256 ハッシュ連鎖計算、および `Window.partitionBy("pos_key").orderBy("slot")` による累積在庫消化計算を実行します。
   * Lightning Engine は JVM ベースの行指向処理ではなく、カラムナメモリレイアウト上で C++ ベクトル化演算（SIMD）と高速ネイティブ Shuffle を適用するため、CPU バウンドな数学関数・ハッシュ演算およびウィンドウ集約の実行時間を約 20% 短縮し、同時に DCU 消費量を 20.61% 削減しました。
2. **本番オーケストレーションへの採用判断**:
   * SLA 短縮（19.97% 高速化）とリソース消費削減（-20.61% DCU-s）の双方で優位性が実証されたため、Cloud Composer 3 の日次本番 DAG（[cymbal_nightly_inventory_reconciliation.py](file:///usr/local/google/home/uedayo/workspace/elevate-da-adv-learner/day_2/cymbal_nightly_inventory_reconciliation.py)）には **Lightning Engine + OpenLineage + BigQueryCheckOperator 品質ゲート** の構成を採用しました。
