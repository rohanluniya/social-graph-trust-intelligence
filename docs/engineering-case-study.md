# Engineering Case Study: Designing a Skew-Resilient Social Graph & Trust Intelligence Platform

## 1. Problem Statement

Operating a large social graph presents four simultaneous distributed-systems bottlenecks:
1. **Power-law celebrity supernode skew:** Less than 1% of vertices absorb a disproportionate share of inbound edges, overloading single partitions under naive hashing and causing tail-latency spikes on 2-hop expansions.
2. **Temporal edge correctness under out-of-order streams:** Late-arriving `follow` retries must never resurrect edges closed by newer `unfollow`, `block`, or GDPR `user_deleted` mutations.
3. **Expensive full-graph recomputations & memory blowup:** Recomputing global features from scratch per micro-batch wastes CPU, and exact 2-hop neighbor sets explode in RAM when traversing celebrity hubs.
4. **Trust & privacy dual mandate:** Abuse signals must be explainable (zero celebrity false positives, clear reason codes) while GDPR Article 17 deletions must leave zero residual active edges across lakehouse tables and serving caches.

## 2. Key Engineering Findings From Real Execution

### Finding A: Unweighted Label Propagation Collapses Under Celebrity Hubs
In our initial vertical slice (`local` profile, `1,588` active nodes, `32,380` edges), standard unweighted Label Propagation collapsed nearly all organic users into a single giant community because mega-celebrity hubs bridged every organic cluster (`community_count = 5`, Modularity `Q = 0.0282`). Furthermore, mapping communities blindly onto fixed partition pairs amplified vertex-cut skew to `7.542×`, which our automated data-quality gate `DQ-07` immediately flagged (`8/9 PASS`).

**Resolution:**
1. Switched to **inverse-degree weighted deterministic LPA** (`weight(v) = 1 / degree(v)^0.75`), dampening supernode broadcast dominance while preserving local dense cores -> Modularity jumped **17.5×** from `0.0282` to **`0.4935`** across `41` coherent communities.
2. Switched regular-node placement to **LPT capacity-aware balanced assignment** while salting supernode edges across mirror buckets -> vertex-cut skew flattened to **`1.071×`** vs `1.397×` naive hash (`-23.34%` skew reduction, `-65.6%` CV reduction, `4.2×` faster 2-hop p95 latency), bringing `DQ` to **`9/9 PASS`**.

### Finding B: Exact Incremental Triangle & Reciprocity Maintenance Matches Full CSR to `0.0`
Maintaining directed/undirected incidence sets with incremental triangle delta accounting (`common = undir_adj[u] & undir_adj[v]`) achieved **`46.8×` structural speedup** (`2.89 ms` vs `135.31 ms` full CSR on `local`, `14.94 ms` vs `684.62 ms` on `stress`) and **`6.9×` speedup including warm-start PageRank** while asserting **`0.0` max absolute numerical difference** across both `local` benchmarks and randomized Hypothesis property tests.

### Finding C: HyperLogLog Cuts 2-Hop Memory by `320×` at Stress Scale
Materializing exact 2-hop distinct neighbor sets at `stress` scale (`5,970` nodes, `138,745` edges) required `1.95 GB` of string-set memory because followers of mega-celebrities inherited thousands of indirect 2-hop peers. Vectorized NumPy `HyperLogLog(p=10)` (`1,024` bytes/node) reduced memory by **`320.1×`** (`6.11 MB` total) with only **`3.60%` mean relative error**.

### Finding D: Explainable Trust Scoring & Borderline Organic False-Positive Analysis
On the `local` synthetic ground-truth benchmark (`152` adversarial bot/ring accounts, `1,436` organic/celebrity accounts), the explainable 5-signal additive model achieved **Precision `0.9742`**, **Recall `0.9934`**, **F1 `0.9837`**, **FPR `0.28%`**, and **`0/8` Celebrity False Positives**. Inspecting the `4` false positives (`usr_000166`, `usr_000173`, `usr_000180`, `usr_000187`) revealed benign "power-fan" accounts engaging rapidly on identical celebrity posts while receiving heated-debate reports/blocks (`low_reciprocity_fanout == 0.0`). In production, gating `COORDINATED_RING_OVERLAP` with account-age/ASN diversity and downweighting celebrity-post co-engagement resolves this FP class cleanly.
