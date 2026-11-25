"""Unit and Hypothesis property-based tests for sparse CSR graph engine, incremental parity, and sketches."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from social_graph.graph_engine import (
    IncrementalGraphState,
    compute_full_graph_metrics,
    verify_incremental_parity,
)
from social_graph.sketches import CountMinSketch, HyperLogLog


@pytest.mark.unit
@settings(max_examples=35, deadline=None)
@given(
    n_nodes=st.integers(min_value=5, max_value=24),
    init_edges=st.lists(
        st.tuples(st.integers(min_value=0, max_value=23), st.integers(min_value=0, max_value=23)),
        max_size=45,
    ),
    add_edges=st.lists(
        st.tuples(st.integers(min_value=0, max_value=23), st.integers(min_value=0, max_value=23)),
        max_size=25,
    ),
    del_indices=st.lists(st.integers(min_value=0, max_value=44), max_size=12),
    erase_idx=st.integers(min_value=0, max_value=23),
)
def test_property_incremental_equals_full_recomputation(
    n_nodes: int,
    init_edges: list[tuple[int, int]],
    add_edges: list[tuple[int, int]],
    del_indices: list[int],
    erase_idx: int,
) -> None:
    """Property: incremental edge adds, deletes, and user erasure match full CSR recomputation exactly."""
    users = [f"usr_{i:06d}" for i in range(n_nodes)]
    e0 = [
        (users[u % n_nodes], users[v % n_nodes])
        for u, v in init_edges
        if (u % n_nodes) != (v % n_nodes)
    ]
    e_add = [
        (users[u % n_nodes], users[v % n_nodes])
        for u, v in add_edges
        if (u % n_nodes) != (v % n_nodes)
    ]

    # Build incremental state on initial graph
    inc = IncrementalGraphState(users, e0)
    _ = inc.snapshot_features(compute_pagerank=True)

    # Apply additions, deletions, and an erasure
    active_set = set(e0)
    for pair in e_add:
        active_set.add(pair)

    edge_list = sorted(active_set)
    deletions = [edge_list[idx % len(edge_list)] for idx in del_indices] if edge_list else []
    for pair in deletions:
        active_set.discard(pair)

    erased_uid = users[erase_idx % n_nodes]
    inc.apply_delta_batch(additions=e_add, deletions=deletions, erased_users=[erased_uid])
    inc_features = inc.snapshot_features(compute_pagerank=True)

    # Ground-truth full CSR recomputation on resulting graph
    remaining_users = [u for u in users if u != erased_uid]
    remaining_edges = sorted(
        (s, d) for s, d in active_set if s != erased_uid and d != erased_uid
    )
    full = compute_full_graph_metrics(remaining_users, remaining_edges)

    parity = verify_incremental_parity(full, inc_features, check_pagerank=True)
    assert parity["parity_verified"], f"Parity mismatch: {parity}"
    assert parity["max_exact_feature_abs_diff"] == 0.0
    assert parity["max_pagerank_abs_diff"] <= 1e-6


@pytest.mark.unit
@settings(max_examples=25, deadline=None)
@given(
    n_nodes=st.integers(min_value=4, max_value=20),
    raw_edges=st.lists(
        st.tuples(st.integers(min_value=0, max_value=19), st.integers(min_value=0, max_value=19)),
        max_size=40,
    ),
)
def test_property_undirected_components_and_pagerank_invariants(
    n_nodes: int,
    raw_edges: list[tuple[int, int]],
) -> None:
    """Property: undirected components are symmetric across every edge and PageRank sums to 1.0."""
    users = [f"usr_{i:06d}" for i in range(n_nodes)]
    edges = [
        (users[u % n_nodes], users[v % n_nodes])
        for u, v in raw_edges
        if (u % n_nodes) != (v % n_nodes)
    ]
    metrics = compute_full_graph_metrics(users, edges)

    # Every edge's endpoints must share the exact same weakly connected component
    for src, dst in edges:
        assert metrics.component_id[src] == metrics.component_id[dst]

    # Sum of component sizes across unique component IDs must equal total active nodes
    unique_comp_sizes = {metrics.component_id[u]: metrics.component_size[u] for u in users}
    assert sum(unique_comp_sizes.values()) == len(users)

    # PageRank forms a valid probability distribution
    pr_sum = sum(metrics.pagerank.values())
    assert abs(pr_sum - 1.0) < 1e-6
    assert all(v >= 0.0 for v in metrics.pagerank.values())


@pytest.mark.unit
def test_hll_accuracy_and_merge_monotonicity() -> None:
    """HyperLogLog estimates stay within theoretical error bounds and merge monotonically."""
    h1 = HyperLogLog(precision=10)
    h2 = HyperLogLog(precision=10)
    set1 = [f"usr_{i:06d}" for i in range(500)]
    set2 = [f"usr_{i:06d}" for i in range(250, 900)]
    h1.add_many(set1)
    h2.add_many(set2)

    est1 = h1.estimate()
    assert abs(est1 - 500) / 500 < 0.12

    h1.merge_inplace(h2)
    est_union = h1.estimate()
    assert est_union >= est1
    assert abs(est_union - 900) / 900 < 0.12


@pytest.mark.unit
@settings(max_examples=25, deadline=None)
@given(
    events=st.lists(
        st.tuples(
            st.integers(min_value=1, max_value=60),
            st.integers(min_value=1, max_value=10),
        ),
        min_size=1,
        max_size=120,
    )
)
def test_property_cms_never_underestimates(events: list[tuple[int, int]]) -> None:
    """Property: Count-Min Sketch estimate is always >= exact frequency for every key."""
    cms = CountMinSketch(depth=5, width=512)
    exact: dict[str, int] = {}
    for key_int, inc in events:
        key = f"usr_{key_int:06d}"
        cms.increment(key, inc)
        exact[key] = exact.get(key, 0) + inc

    for key, true_cnt in exact.items():
        assert cms.query(key) >= true_cnt
