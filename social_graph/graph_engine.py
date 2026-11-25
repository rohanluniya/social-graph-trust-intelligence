"""Sparse-matrix (scipy.sparse CSR/CSC) graph engine with exact incremental feature updates."""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy import sparse
from scipy.sparse.csgraph import connected_components


@dataclass
class FullGraphMetrics:
    user_ids: list[str]
    in_degree: dict[str, int]
    out_degree: dict[str, int]
    reciprocal_degree: dict[str, int]
    reciprocal_ratio: dict[str, float]
    triangles: dict[str, int]
    local_clustering: dict[str, float]
    pagerank: dict[str, float]
    component_id: dict[str, int]
    component_size: dict[str, int]
    component_count: int
    largest_component_size: int
    community_id: dict[str, int]
    community_size: dict[str, int]
    community_count: int
    modularity_q: float
    out_neighbors: dict[str, set[str]]
    in_neighbors: dict[str, set[str]]
    compute_ms: float


def _build_sparse_matrices(
    user_ids: list[str],
    edges: list[tuple[str, str]],
) -> tuple[sparse.csr_matrix, sparse.csr_matrix, dict[str, int]]:
    """Build directed CSR adjacency matrix A and undirected binary CSR matrix U."""
    n = len(user_ids)
    idx_map = {uid: i for i, uid in enumerate(user_ids)}
    if n == 0 or not edges:
        empty = sparse.csr_matrix((n, n), dtype=np.float64)
        return empty, empty, idx_map

    rows: list[int] = []
    cols: list[int] = []
    seen: set[tuple[int, int]] = set()
    for src, dst in edges:
        i = idx_map.get(src)
        j = idx_map.get(dst)
        if i is not None and j is not None and i != j and (i, j) not in seen:
            seen.add((i, j))
            rows.append(i)
            cols.append(j)

    data = np.ones(len(rows), dtype=np.float64)
    a_csr = sparse.csr_matrix((data, (rows, cols)), shape=(n, n), dtype=np.float64)
    u_raw = a_csr + a_csr.T
    u_raw.data = np.ones_like(u_raw.data, dtype=np.float64)
    u_csr = u_raw.tocsr()
    return a_csr, u_csr, idx_map


def compute_pagerank_sparse(
    a_csr: sparse.csr_matrix,
    alpha: float = 0.85,
    tol: float = 1e-9,
    max_iter: int = 100,
    init_scores: np.ndarray | None = None,
    personalization: np.ndarray | None = None,
) -> np.ndarray:
    """Power-iteration PageRank on directed CSR matrix with proper dangling-node redistribution."""
    n = a_csr.shape[0]
    if n == 0:
        return np.zeros(0, dtype=np.float64)

    out_deg = np.asarray(a_csr.sum(axis=1)).ravel()
    dangling = out_deg == 0
    inv_out = np.zeros(n, dtype=np.float64)
    nonzero = out_deg > 0
    inv_out[nonzero] = 1.0 / out_deg[nonzero]

    # Row-stochastic transition matrix P = D^-1 A; we multiply P^T @ r = A^T @ (inv_out * r)
    a_csc_t = a_csr.T.tocsr()

    if personalization is not None:
        teleport = personalization.astype(np.float64)
        teleport = teleport / teleport.sum()
    else:
        teleport = np.full(n, 1.0 / n, dtype=np.float64)

    if init_scores is not None and init_scores.shape == (n,):
        r = init_scores.astype(np.float64).copy()
        s = float(r.sum())
        r = (r / s) if s > 0 else teleport.copy()
    else:
        r = teleport.copy()

    for _ in range(max_iter):
        dangling_mass = float(r[dangling].sum()) if np.any(dangling) else 0.0
        r_next = alpha * (a_csc_t.dot(r * inv_out) + dangling_mass * teleport) + (
            1.0 - alpha
        ) * teleport
        diff = float(np.abs(r_next - r).sum())
        r = r_next
        if diff < tol:
            break

    total = float(r.sum())
    if total > 0:
        r /= total
    return r


def detect_communities_lpa(
    u_csr: sparse.csr_matrix,
    max_iter: int = 30,
) -> tuple[np.ndarray, float]:
    """Deterministic degree-dampened Label Propagation + exact Newman-Girvan Modularity Q.

    Weighting neighbor votes by `1 / sqrt(degree(v))` prevents high-degree celebrity supernodes
    from collapsing distinct organic communities into a single giant cluster.
    """
    n = u_csr.shape[0]
    if n == 0:
        return np.zeros(0, dtype=np.int32), 0.0

    labels = np.arange(n, dtype=np.int32)
    indptr = u_csr.indptr
    indices = u_csr.indices

    degrees = np.diff(indptr).astype(np.float64)
    # Inverse-degree damping suppresses celebrity supernode broadcast dominance
    safe_deg = np.maximum(degrees, 1.0)
    inv_weights = np.where(degrees > 0, 1.0 / np.power(safe_deg, 0.75), 0.0)
    # Visit lower-degree nodes first so dense local cores stabilize before hubs
    visit_order = np.argsort(degrees, kind="mergesort")

    for _ in range(max_iter):
        changed = 0
        for u in visit_order:
            start, end = int(indptr[u]), int(indptr[u + 1])
            if start == end:
                continue
            nbrs = indices[start:end]
            score_by_label: dict[int, float] = {}
            for v in nbrs:
                lbl = int(labels[v])
                score_by_label[lbl] = score_by_label.get(lbl, 0.0) + float(inv_weights[v])
            max_score = max(score_by_label.values())
            cur = int(labels[u])
            if score_by_label.get(cur, -1.0) >= max_score - 1e-12:
                best = cur
            else:
                best = min(
                    lbl for lbl, sc in score_by_label.items() if sc >= max_score - 1e-12
                )
            if best != cur:
                labels[u] = best
                changed += 1
        if changed == 0:
            break

    # Relabel deterministically by descending community size (0 = largest)
    comm_counts = Counter(int(lbl) for lbl in labels)
    ordered_comms = sorted(comm_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    remap = {old: new_idx for new_idx, (old, _) in enumerate(ordered_comms)}
    compact = np.array([remap[int(lbl)] for lbl in labels], dtype=np.int32)

    # Compute exact Modularity Q = sum_c [ (e_cc / 2m) - (d_c / 2m)^2 ]
    two_m = float(u_csr.nnz)
    if two_m == 0.0:
        return compact, 0.0

    num_comms = len(ordered_comms)
    comm_deg_sum = np.zeros(num_comms, dtype=np.float64)
    for u in range(n):
        comm_deg_sum[compact[u]] += float(degrees[u])

    coo = u_csr.tocoo()
    same_mask = compact[coo.row] == compact[coo.col]
    internal_edges_twice = np.bincount(compact[coo.row[same_mask]], minlength=num_comms).astype(
        np.float64
    )
    modularity = float(
        np.sum((internal_edges_twice / two_m) - np.square(comm_deg_sum / two_m))
    )
    return compact, round(modularity, 4)


def compute_full_graph_metrics(
    user_ids: list[str],
    active_edges: list[tuple[str, str]],
) -> FullGraphMetrics:
    """Compute exact graph features, components, communities, and PageRank via scipy.sparse CSR."""
    t0 = time.perf_counter()
    sorted_users = sorted(set(user_ids))
    n = len(sorted_users)
    a_csr, u_csr, idx_map = _build_sparse_matrices(sorted_users, active_edges)

    out_deg_arr = np.asarray(a_csr.sum(axis=1)).ravel().astype(np.int64)
    in_deg_arr = np.asarray(a_csr.sum(axis=0)).ravel().astype(np.int64)
    recip_mat = a_csr.multiply(a_csr.T)
    recip_arr = np.asarray(recip_mat.sum(axis=1)).ravel().astype(np.int64)

    # Exact triangle count per node via sparse matrix product: diag(U^3) / 2 == ((U @ U) .* U).sum(axis=1) / 2
    if u_csr.nnz > 0:
        u2 = u_csr.dot(u_csr)
        tri_twice = np.asarray(u2.multiply(u_csr).sum(axis=1)).ravel()
        tri_arr = np.rint(tri_twice / 2.0).astype(np.int64)
    else:
        tri_arr = np.zeros(n, dtype=np.int64)

    undir_deg_arr = np.diff(u_csr.indptr).astype(np.int64)
    clust_arr = np.zeros(n, dtype=np.float64)
    eligible = undir_deg_arr >= 2
    clust_arr[eligible] = (2.0 * tri_arr[eligible]) / (
        undir_deg_arr[eligible] * (undir_deg_arr[eligible] - 1)
    )

    # Connected components (weakly connected on undirected U)
    n_comp, comp_labels_raw = connected_components(u_csr, directed=False) if n > 0 else (0, np.zeros(0, dtype=np.int32))
    comp_counts = Counter(int(c) for c in comp_labels_raw)
    ordered_comps = sorted(comp_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    comp_remap = {old: idx for idx, (old, _) in enumerate(ordered_comps)}
    comp_labels = np.array([comp_remap[int(c)] for c in comp_labels_raw], dtype=np.int32)
    comp_sizes_map = {idx: sz for idx, (_, sz) in enumerate(ordered_comps)}
    largest_comp = ordered_comps[0][1] if ordered_comps else 0

    # Community detection (LPA) + Modularity Q
    comm_labels, modularity_q = detect_communities_lpa(u_csr)
    comm_counts = Counter(int(c) for c in comm_labels)

    # Global PageRank
    pr_arr = compute_pagerank_sparse(a_csr)

    # Adjacency sets for fast neighborhood inspection & sketches
    out_neighbors: dict[str, set[str]] = {u: set() for u in sorted_users}
    in_neighbors: dict[str, set[str]] = {u: set() for u in sorted_users}
    for src, dst in active_edges:
        if src in idx_map and dst in idx_map and src != dst:
            out_neighbors[src].add(dst)
            in_neighbors[dst].add(src)

    in_degree: dict[str, int] = {}
    out_degree: dict[str, int] = {}
    reciprocal_degree: dict[str, int] = {}
    reciprocal_ratio: dict[str, float] = {}
    triangles: dict[str, int] = {}
    local_clustering: dict[str, float] = {}
    pagerank: dict[str, float] = {}
    component_id: dict[str, int] = {}
    component_size: dict[str, int] = {}
    community_id: dict[str, int] = {}
    community_size: dict[str, int] = {}

    for i, uid in enumerate(sorted_users):
        ind = int(in_deg_arr[i])
        outd = int(out_deg_arr[i])
        rec = int(recip_arr[i])
        in_degree[uid] = ind
        out_degree[uid] = outd
        reciprocal_degree[uid] = rec
        reciprocal_ratio[uid] = round(rec / outd, 6) if outd > 0 else 0.0
        triangles[uid] = int(tri_arr[i])
        local_clustering[uid] = round(float(clust_arr[i]), 6)
        pagerank[uid] = float(pr_arr[i])
        cid = int(comp_labels[i])
        component_id[uid] = cid
        component_size[uid] = int(comp_sizes_map[cid])
        mid = int(comm_labels[i])
        community_id[uid] = mid
        community_size[uid] = int(comm_counts[mid])

    compute_ms = round((time.perf_counter() - t0) * 1000.0, 3)

    return FullGraphMetrics(
        user_ids=sorted_users,
        in_degree=in_degree,
        out_degree=out_degree,
        reciprocal_degree=reciprocal_degree,
        reciprocal_ratio=reciprocal_ratio,
        triangles=triangles,
        local_clustering=local_clustering,
        pagerank=pagerank,
        component_id=component_id,
        component_size=component_size,
        component_count=int(n_comp),
        largest_component_size=int(largest_comp),
        community_id=community_id,
        community_size=community_size,
        community_count=len(comm_counts),
        modularity_q=modularity_q,
        out_neighbors=out_neighbors,
        in_neighbors=in_neighbors,
        compute_ms=compute_ms,
    )


class IncrementalGraphState:
    """Maintains exact incremental degrees, reciprocity, triangles, clustering, and warm-start PageRank.

    Guarantees exact mathematical equivalence against full CSR recomputation after any sequence
    of directed edge additions, deletions, blocks, and user erasures.
    """

    def __init__(self, user_ids: list[str], initial_edges: list[tuple[str, str]]) -> None:
        self.users: set[str] = set(user_ids)
        self.out_adj: dict[str, set[str]] = defaultdict(set)
        self.in_adj: dict[str, set[str]] = defaultdict(set)
        self.undir_adj: dict[str, set[str]] = defaultdict(set)
        self.triangles: dict[str, int] = {u: 0 for u in self.users}
        self.reciprocal_degree: dict[str, int] = {u: 0 for u in self.users}
        self._last_pr: dict[str, float] = {}

        for src, dst in initial_edges:
            self.add_edge(src, dst)

    def _ensure_user(self, uid: str) -> None:
        if uid not in self.users:
            self.users.add(uid)
            self.triangles.setdefault(uid, 0)
            self.reciprocal_degree.setdefault(uid, 0)

    def add_edge(self, src: str, dst: str) -> bool:
        """Apply a single directed edge addition incrementally."""
        if src == dst:
            return False
        self._ensure_user(src)
        self._ensure_user(dst)
        if dst in self.out_adj[src]:
            return False

        self.out_adj[src].add(dst)
        self.in_adj[dst].add(src)

        # Check if reverse directed edge already exists
        if src in self.out_adj[dst]:
            self.reciprocal_degree[src] += 1
            self.reciprocal_degree[dst] += 1
            # Undirected edge already existed -> triangles unchanged
            return True

        # New undirected edge {src, dst}: every common undirected neighbor forms a new triangle
        u_src = self.undir_adj[src]
        u_dst = self.undir_adj[dst]
        common = u_src & u_dst if len(u_src) <= len(u_dst) else u_dst & u_src
        if common:
            cnt = len(common)
            self.triangles[src] += cnt
            self.triangles[dst] += cnt
            for w in common:
                self.triangles[w] += 1

        u_src.add(dst)
        u_dst.add(src)
        return True

    def remove_edge(self, src: str, dst: str) -> bool:
        """Apply a single directed edge deletion incrementally."""
        if dst not in self.out_adj.get(src, set()):
            return False

        self.out_adj[src].discard(dst)
        self.in_adj[dst].discard(src)

        if src in self.out_adj.get(dst, set()):
            self.reciprocal_degree[src] = max(0, self.reciprocal_degree[src] - 1)
            self.reciprocal_degree[dst] = max(0, self.reciprocal_degree[dst] - 1)
            # Undirected edge persists via reverse edge -> triangles unchanged
            return True

        # Undirected edge {src, dst} removed: decrement triangles for common undirected neighbors
        u_src = self.undir_adj[src]
        u_dst = self.undir_adj[dst]
        u_src.discard(dst)
        u_dst.discard(src)
        common = u_src & u_dst if len(u_src) <= len(u_dst) else u_dst & u_src
        if common:
            cnt = len(common)
            self.triangles[src] = max(0, self.triangles[src] - cnt)
            self.triangles[dst] = max(0, self.triangles[dst] - cnt)
            for w in common:
                self.triangles[w] = max(0, self.triangles[w] - 1)
        return True

    def erase_user(self, user_id: str) -> int:
        """Remove a user and all incident directed edges incrementally."""
        if user_id not in self.users:
            return 0
        removed = 0
        for dst in list(self.out_adj.get(user_id, set())):
            if self.remove_edge(user_id, dst):
                removed += 1
        for src in list(self.in_adj.get(user_id, set())):
            if self.remove_edge(src, user_id):
                removed += 1
        self.users.discard(user_id)
        self.out_adj.pop(user_id, None)
        self.in_adj.pop(user_id, None)
        self.undir_adj.pop(user_id, None)
        self.triangles.pop(user_id, None)
        self.reciprocal_degree.pop(user_id, None)
        self._last_pr.pop(user_id, None)
        return removed

    def apply_delta_batch(
        self,
        additions: list[tuple[str, str]],
        deletions: list[tuple[str, str]],
        erased_users: list[str] | None = None,
    ) -> dict[str, Any]:
        """Apply a delta batch and measure incremental execution latency."""
        t0 = time.perf_counter()
        added_cnt = sum(1 for s, d in additions if self.add_edge(s, d))
        deleted_cnt = sum(1 for s, d in deletions if self.remove_edge(s, d))
        erased_edges = sum(self.erase_user(u) for u in (erased_users or []))
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "added": added_cnt,
            "deleted": deleted_cnt,
            "erased_edges": erased_edges,
            "incremental_ms": round(elapsed_ms, 3),
        }

    def snapshot_features(self, compute_pagerank: bool = False) -> dict[str, dict[str, Any]]:
        """Return exact per-node features matching FullGraphMetrics schema."""
        sorted_users = sorted(self.users)
        pr_map: dict[str, float] = {}
        if compute_pagerank and sorted_users:
            edges = [(u, v) for u in sorted_users for v in self.out_adj.get(u, set())]
            a_csr, _, _ = _build_sparse_matrices(sorted_users, edges)
            init_vec = (
                np.array([self._last_pr.get(u, 1.0 / len(sorted_users)) for u in sorted_users])
                if self._last_pr
                else None
            )
            pr_arr = compute_pagerank_sparse(a_csr, init_scores=init_vec)
            pr_map = {u: float(pr_arr[i]) for i, u in enumerate(sorted_users)}
            self._last_pr = pr_map

        features: dict[str, dict[str, Any]] = {}
        for uid in sorted_users:
            in_d = len(self.in_adj.get(uid, set()))
            out_d = len(self.out_adj.get(uid, set()))
            rec_d = int(self.reciprocal_degree.get(uid, 0))
            rec_ratio = round(rec_d / out_d, 6) if out_d > 0 else 0.0
            tri = int(self.triangles.get(uid, 0))
            u_deg = len(self.undir_adj.get(uid, set()))
            clust = round((2.0 * tri) / (u_deg * (u_deg - 1)), 6) if u_deg >= 2 else 0.0
            features[uid] = {
                "in_degree": in_d,
                "out_degree": out_d,
                "reciprocal_degree": rec_d,
                "reciprocal_ratio": rec_ratio,
                "triangles": tri,
                "local_clustering": clust,
                "pagerank": pr_map.get(uid, self._last_pr.get(uid, 0.0)),
            }
        return features


def verify_incremental_parity(
    full_metrics: FullGraphMetrics,
    incremental_features: dict[str, dict[str, Any]],
    check_pagerank: bool = True,
) -> dict[str, Any]:
    """Assert exact numerical parity between incremental updates and full CSR recomputation."""
    full_users = set(full_metrics.user_ids)
    inc_users = set(incremental_features.keys())
    missing = sorted(full_users - inc_users)
    extra = sorted(inc_users - full_users)

    max_exact_diff = 0.0
    max_pr_diff = 0.0
    mismatches: list[str] = []

    for uid in sorted(full_users & inc_users):
        inc = incremental_features[uid]
        for field_name, full_val in (
            ("in_degree", full_metrics.in_degree[uid]),
            ("out_degree", full_metrics.out_degree[uid]),
            ("reciprocal_degree", full_metrics.reciprocal_degree[uid]),
            ("triangles", full_metrics.triangles[uid]),
            ("reciprocal_ratio", full_metrics.reciprocal_ratio[uid]),
            ("local_clustering", full_metrics.local_clustering[uid]),
        ):
            diff = abs(float(full_val) - float(inc[field_name]))
            if diff > max_exact_diff:
                max_exact_diff = diff
            if diff > 1e-6:
                mismatches.append(f"{uid}.{field_name}: full={full_val} vs inc={inc[field_name]}")

        if check_pagerank:
            pr_diff = abs(float(full_metrics.pagerank[uid]) - float(inc.get("pagerank", 0.0)))
            if pr_diff > max_pr_diff:
                max_pr_diff = pr_diff

    passed = (
        not missing
        and not extra
        and max_exact_diff <= 1e-6
        and (not check_pagerank or max_pr_diff <= 1e-6)
    )
    return {
        "parity_verified": passed,
        "users_checked": len(full_users & inc_users),
        "missing_users": len(missing),
        "extra_users": len(extra),
        "max_exact_feature_abs_diff": round(max_exact_diff, 9),
        "max_pagerank_abs_diff": round(max_pr_diff, 9),
        "sample_mismatches": mismatches[:5],
    }
