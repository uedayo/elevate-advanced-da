"""POS Hardware Troubleshooting RAG tool over BigQuery vector embeddings."""

import os
import re
from google.cloud import bigquery

REFUSAL_MESSAGE = (
    "[WARNING: Uncertified / Out-of-Scope Hardware Inquiry. Vector similarity score "
    "fell below the safety threshold and no certified POS runbook matched your query. "
    "Please consult authorized hardware vendor support.]"
)


def pos_troubleshooting_rag_tool(query: str) -> str:
    """Performs hybrid search (vector similarity + exact error code boost) over POS hardware manuals in BigQuery.

    Applies fine-grained 500-character sliding window chunks from pos_manual_chunk_embeddings,
    stitches adjacent chunks (N-1 to N+1), boosts exact error code matches (+0.25), enforces a
    0.65 similarity threshold, and returns the Top-5 certified procedures with clickable HTTPS links.

    Args:
        query: Hardware error code or troubleshooting question (e.g. 'ERR-PAY-4001 EMV freeze').

    Returns:
        Top-5 certified recovery procedures with clickable manual links, or a warning disclaimer
        if the hardware query falls below the relevance threshold.
    """
    project_id = (
        os.environ.get("PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or "argolisyo"
    )
    similarity_threshold = float(os.environ.get("RAG_SIMILARITY_THRESHOLD", "0.65"))

    # Extract explicit error code token (e.g., ERR-PAY-4001, ERR-DN-PRNT-24V) if present
    error_match = re.search(r"(ERR-[A-Z0-9-]+)", query.upper())
    error_code = error_match.group(1) if error_match else ""

    sql = f"""
    WITH query_emb AS (
      SELECT ml_generate_embedding_result AS emb
      FROM ML.GENERATE_EMBEDDING(
        MODEL `{project_id}.module1_unstructureddata.pos_text_embedding_model`,
        (SELECT @query_text AS content),
        STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type)
      )
    ),
    scored_chunks AS (
      SELECT
        base.source_pdf_uri,
        base.document_filename,
        base.document_title,
        base.equipment_covered,
        base.chunk_index,
        base.chunk_content,
        ROUND(1.0 - distance, 4) AS vector_score,
        ROUND(
          (1.0 - distance) + IF(
            @error_code != '' AND STRPOS(UPPER(base.chunk_content), @error_code) > 0,
            0.25,
            0.0
          ),
          4
        ) AS hybrid_score
      FROM VECTOR_SEARCH(
        TABLE `{project_id}.cymbal_gold.pos_manual_chunk_embeddings`,
        'embedding',
        TABLE query_emb,
        'emb',
        top_k => 15,
        distance_type => 'COSINE'
      )
    ),
    filtered_matches AS (
      SELECT *
      FROM scored_chunks
      WHERE hybrid_score >= @threshold
      ORDER BY hybrid_score DESC
      LIMIT 5
    )
    SELECT
      m.document_title,
      m.equipment_covered,
      REPLACE(m.source_pdf_uri, 'gs://', 'https://storage.cloud.google.com/') AS https_manual_link,
      m.vector_score,
      m.hybrid_score,
      STRING_AGG(c.chunk_content, '\\n' ORDER BY c.chunk_index) AS stitched_context
    FROM filtered_matches AS m
    JOIN `{project_id}.cymbal_gold.pos_manual_chunk_embeddings` AS c
      ON c.source_pdf_uri = m.source_pdf_uri
     AND c.chunk_index BETWEEN m.chunk_index - 1 AND m.chunk_index + 1
    GROUP BY
      m.source_pdf_uri,
      m.chunk_index,
      m.document_title,
      m.equipment_covered,
      m.vector_score,
      m.hybrid_score
    ORDER BY m.hybrid_score DESC, m.chunk_index ASC
    """

    try:
        client = bigquery.Client(project=project_id)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("query_text", "STRING", query),
                bigquery.ScalarQueryParameter("error_code", "STRING", error_code),
                bigquery.ScalarQueryParameter(
                    "threshold", "FLOAT64", similarity_threshold
                ),
            ]
        )
        rows = list(client.query(sql, job_config=job_config).result())
        if not rows:
            return REFUSAL_MESSAGE

        formatted_matches: list[str] = []
        for idx, row in enumerate(rows, start=1):
            formatted_matches.append(
                f"--- [MATCH #{idx} | Hybrid Score: {row.hybrid_score} (Vector Score: {row.vector_score})] ---\n"
                f"Manual: {row.document_title} ({row.equipment_covered})\n"
                f"Documentation Link: {row.https_manual_link}\n"
                f"Stitched Procedure Context:\n{row.stitched_context}"
            )
        return "\n\n".join(formatted_matches)

    except Exception as exc:
        return f"[FALLBACK: Error executing BigQuery Vector Search over POS manuals: {exc}]"
