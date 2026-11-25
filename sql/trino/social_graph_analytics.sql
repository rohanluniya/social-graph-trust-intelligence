-- Trino 484 Analytical SQL Suite for Apache Iceberg V2 Social Graph & Trust Intelligence

-- 1. Point-in-time active directed graph snapshot query (event-time temporal edges)
SELECT
    src_user_id,
    dst_user_id,
    valid_from,
    valid_to
FROM iceberg.silver.temporal_edges
WHERE is_deleted = FALSE
  AND is_blocked = FALSE
  AND valid_from <= 1760006000000
  AND (valid_to IS NULL OR valid_to > 1760006000000);

-- 2. Partition skew diagnostics on vertex-cut adjacency table
SELECT
    partition_id,
    COUNT(*) AS edge_count,
    SUM(CASE WHEN is_supernode_edge THEN 1 ELSE 0 END) AS supernode_edges,
    COUNT(DISTINCT salt_bucket) AS active_salt_buckets
FROM iceberg.gold.adjacency_partitioned
GROUP BY partition_id
ORDER BY edge_count DESC;

-- 3. Celebrity supernode influence vs false-positive safety audit
SELECT
    nf.user_id,
    nf.in_degree,
    nf.out_degree,
    nf.pagerank_score,
    nf.two_hop_exact,
    nf.two_hop_hll,
    ROUND(ABS(nf.two_hop_hll - nf.two_hop_exact) * 100.0 / NULLIF(nf.two_hop_exact, 0), 2) AS hll_rel_err_pct,
    ts.risk_score,
    ts.risk_tier
FROM iceberg.gold.node_graph_features nf
JOIN iceberg.gold.trust_risk_scores ts ON nf.user_id = ts.user_id
WHERE nf.is_supernode = TRUE
ORDER BY nf.in_degree DESC;

-- 4. Community-level abuse concentration summary
SELECT
    cm.community_id,
    COUNT(*) AS members,
    ROUND(AVG(ts.risk_score), 2) AS avg_risk_score,
    SUM(CASE WHEN ts.risk_tier IN ('high', 'critical') THEN 1 ELSE 0 END) AS flagged_members,
    ROUND(AVG(nf.local_clustering), 4) AS avg_local_clustering
FROM iceberg.gold.communities_and_components cm
JOIN iceberg.gold.node_graph_features nf ON cm.user_id = nf.user_id
JOIN iceberg.gold.trust_risk_scores ts ON cm.user_id = ts.user_id
GROUP BY cm.community_id
ORDER BY flagged_members DESC, members DESC
LIMIT 20;

-- 5. GDPR Right-to-Erasure zero-leak verification query
WITH erased AS (
    SELECT DISTINCT user_id
    FROM iceberg.silver.user_blocks_and_deletions
    WHERE action_type = 'user_deleted'
)
SELECT
    COUNT(*) AS residual_active_edge_leaks
FROM iceberg.silver.temporal_edges te
JOIN erased e
  ON te.src_user_id = e.user_id OR te.dst_user_id = e.user_id
WHERE te.is_active = TRUE;

-- 6. Apache Iceberg V2 snapshot history & time-travel inspection
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM iceberg.silver."temporal_edges$snapshots"
ORDER BY committed_at DESC
LIMIT 10;
