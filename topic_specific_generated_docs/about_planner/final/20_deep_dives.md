# 20. Deep Dives

Prerequisites: All previous numbered modules. This chapter assumes familiarity with `RelOptInfo`, `Path`, `PathTarget`, `RestrictInfo`, `EquivalenceClass`, `pathkey`, `varnullingrels`, `SpecialJoinInfo`, and the create_plan / setrefs pipeline.

This chapter collects fifteen topics that deserved more space than the per-component modules could give them. Each section is a self-contained explanation rooted in the source code, with line citations for verification. Read in order or jump to the topic of interest.

Sections:
- [20.1 add_path() Pareto-dominance test and the cost_diff_fuzz_factor](#201-add_path-pareto-dominance-test-and-the-cost_diff_fuzz_factor)
- [20.2 Outer-join identity 3 and the Pbc/Pb*c clone-clause mechanism](#202-outer-join-identity-3-and-the-pbcpbc-clone-clause-mechanism)
- [20.3 varnullingrels and PlaceHolderVar above outer joins](#203-varnullingrels-and-placeholdervar-above-outer-joins)
- [20.4 EquivalenceClass transitivity in the presence of outer joins](#204-equivalenceclass-transitivity-in-the-presence-of-outer-joins)
- [20.5 DP join search complexity](#205-dp-join-search-complexity)
- [20.6 GEQO chromosome encoding and crossover operator selection](#206-geqo-chromosome-encoding-and-crossover-operator-selection)
- [20.7 Cost-model GUC sensitivity and tuning workflow](#207-cost-model-guc-sensitivity-and-tuning-workflow)
- [20.8 Selectivity estimation: MCV, histograms, ndistinct, multivariate stats](#208-selectivity-estimation-mcv-histograms-ndistinct-multivariate-stats)
- [20.9 Parameterized paths and ParamPathInfo](#209-parameterized-paths-and-parampathinfo)
- [20.10 Subquery pull-up rules and the simple-subquery predicate](#2010-subquery-pull-up-rules-and-the-simple-subquery-predicate)
- [20.11 Hash-aggregate spill-to-disk and the planner's row-count bound](#2011-hash-aggregate-spill-to-disk-and-the-planners-row-count-bound)
- [20.12 Partition pruning at plan vs execution time](#2012-partition-pruning-at-plan-vs-execution-time)
- [20.13 Parallel-safe vs parallel-restricted classification](#2013-parallel-safe-vs-parallel-restricted-classification)
- [20.14 Memoize node decision and cost](#2014-memoize-node-decision-and-cost)
- [20.15 Planner hooks in the wild: pg_hint_plan, citus, timescaledb](#2015-planner-hooks-in-the-wild-pg_hint_plan-citus-timescaledb)

---

## 20.1 add_path() Pareto-dominance test and the cost_diff_fuzz_factor

`add_path()` is the heart of the cost-based optimizer. Every path generated for a `RelOptInfo` is offered to it; the function decides whether to keep the new path, whether to discard it, and whether to discard previously-kept paths the new one now dominates.

The dominance check is a multi-dimensional Pareto test, not a simple `total_cost` comparison. Two paths can both survive if they are incomparable along the relevant axes:

- **Total cost**.
- **Startup cost** (only when the rel's `consider_startup` or `consider_param_startup` flag is true).
- **Pathkeys** (sort orderings).
- **Parameterization** (`param_info`).
- **Parallel-safety** flag.
- **Row count** (rarely — only for parameterized paths that may be cheaper at the cost of producing fewer rows).

A new path P_new is kept if there is some axis along which P_new wins (strictly better). It dominates an existing path P_old if it is fuzzily at least as good on every axis and strictly better on at least one.

### 20.1.1 The fuzz factor

`STD_FUZZ_FACTOR` is defined in `src/backend/optimizer/util/pathnode.c:47`:

```c
#define STD_FUZZ_FACTOR 1.01
```

`compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor)` (`pathnode.c:164`) returns one of `COSTS_BETTER1`, `COSTS_BETTER2`, `COSTS_EQUAL`, or `COSTS_DIFFERENT`. The 1% fuzz prevents the planner from keeping nearly-identical paths because of trivial cost differences (e.g. floating-point noise). The factor is multiplicative: `path1` is fuzzily worse on total cost iff `path1->total_cost > path2->total_cost * fuzz_factor`.

The decision logic from the source:

```c
/* fuzzily worse on total cost */
if (path1->total_cost > path2->total_cost * fuzz_factor) {
    /* but possibly fuzzily better on startup */
    if (CONSIDER_PATH_STARTUP_COST(path1) &&
        path2->startup_cost > path1->startup_cost * fuzz_factor)
        return COSTS_DIFFERENT;     /* keep both */
    return COSTS_BETTER2;            /* path2 dominates */
}
```

The symmetric branch handles the case where `path1` dominates `path2`. If neither path is fuzzily worse on either dimension, return `COSTS_EQUAL`.

### 20.1.2 Why fuzziness matters

Without the 1% fuzz, `add_path` would frequently keep two paths whose costs differ in the seventh decimal place — purely artifactual differences that nonetheless pollute the path list and slow down later iterations. The 1% threshold is empirically tuned: lower values lose useful pruning; higher values risk discarding genuinely better plans.

### 20.1.3 The Pareto test in full

`add_path()` (`pathnode.c`, around line 425) iterates `parent_rel->pathlist`. For each existing path, it calls `compare_path_costs_fuzzily` and also checks pathkeys (`compare_pathkeys`), parameterization (`bms_subset_compare(p1->param_info->ppi_required_outer, p2->param_info->ppi_required_outer)` if present), and parallel-safety. Possible outcomes per pair:

| Cost compare    | Pathkey compare         | Param compare         | Decision (versus existing) |
|-----------------|-------------------------|-----------------------|----------------------------|
| `COSTS_BETTER1` | `PATHKEYS_EQUAL/BETTER1`| equal or better       | new path dominates existing |
| `COSTS_BETTER2` | `PATHKEYS_EQUAL/BETTER2`| equal or better (other dir) | existing dominates new |
| `COSTS_EQUAL`   | one strictly better     | equal                 | strictly-better-pathkey side wins |
| `COSTS_DIFFERENT` | any                   | any                   | both kept              |
| any             | `PATHKEYS_DIFFERENT`    | any                   | both kept              |

When the new path dominates an existing one, the existing path is `pfree`'d (with one important exception: IndexPaths are not freed because they may be referenced from the parent's `pathlist` for OR-clause planning). The pruning is aggressive: a plan space of millions of paths gets reduced to a few dozen by the time `set_cheapest()` runs.

---

## 20.2 Outer-join identity 3 and the Pbc/Pb*c clone-clause mechanism

The PostgreSQL planner is allowed to reorder outer joins under three identities documented in `src/backend/optimizer/README` (lines 193-235). Identity 3 is the trickiest:

```
3.   (A leftjoin B on (Pab)) leftjoin C on (Pbc)
   = A leftjoin (B leftjoin C on (Pbc)) on (Pab)
```

The identity holds only if `Pbc` is **strict** for at least one column of B (i.e. `Pbc` returns NULL or false when given a NULL B-column value). If `Pbc` is not strict, the first form might produce some rows with non-null C columns where the second form would make those entries null.

### 20.2.1 Why this matters for clauses

Suppose the original query is in the second form: `A leftjoin (B leftjoin C on (Pbc)) on (Pab)`. The parser marks `Pbc`'s B Vars with empty `varnullingrels` (B has not been nulled yet at the inner join's level). If the planner reorders to:

```
(A leftjoin B on (Pab)) leftjoin C on (Pb*c)
```

the B Vars in `Pbc` should now have B/the A/B-join in their `varnullingrels` because B has been nulled by the outer join above the inner join. This new form is `Pb*c` (Pbc with B-Vars marked nulled).

The two forms have **different evaluation semantics**: `Pbc` evaluates against the row before the outer null-extension; `Pb*c` evaluates after.

### 20.2.2 The clone solution

Quoting `src/backend/optimizer/README` line 386:

> ... is legal, we generate both the Pbc and Pb*c forms of that ON clause...

In other words, when the planner detects that identity 3 may apply, it generates **two RestrictInfo clones** of the same logical clause: one with the unnullified B Vars (`Pbc`) and one with the nullified B Vars (`Pb*c`). Both clones share the same `rinfo_serial`, so `add_path` recognizes them as one logical condition and does not double-count them.

The clones are tagged via the `is_clone` and `has_clone` flags on RestrictInfo (see [Module 11.3](11_restrictinfo_and_clause_utils.md#1131-the-rinfo_is_pushed_down-macro)):

- `has_clone = true`: this RI has a sibling clone elsewhere.
- `is_clone = true`: this RI is itself a clone (the sibling is the canonical form).

### 20.2.3 Selecting the right clone at join time

When forming an outer join, `join_is_legal` and the qual-distribution code (`distribute_qual_to_rels`) examine each RI's `required_relids`. The `Pb*c` form requires the OJ relid to be present; the `Pbc` form does not. This lets the planner pick whichever clone is legal at the current join level:

- Below the A/B join: only `Pbc` is legal.
- Above the A/B join: only `Pb*c` is legal (B is now nullable).

The README at line 533 elaborates: when forming an outer join, "it is easy to reject the 'Pb*c' form since its required relid set includes the OJ relid, which has not yet been formed."

### 20.2.4 Implications for selectivity

Because both clones share the same `rinfo_serial` and represent the same logical condition, the planner uses only one of them for selectivity (the canonical `Pbc` form). This avoids double-counting in cost estimates.

---

## 20.3 varnullingrels and PlaceHolderVar above outer joins

The traditional way to represent "this column may be NULL because of an outer join" was implicit: any reference to a nullable-side Var was just a plain Var, and the planner had to track context separately. PG 16 introduced explicit `varnullingrels` markers on every Var, dramatically simplifying outer-join semantics.

### 20.3.1 What varnullingrels means

Quoting the README around line 376:

```
SELECT * FROM t1 LEFT JOIN t2 ON (t1.x = t2.y) WHERE foo(t2.z)
```

`t2.z` referenced anywhere above the LEFT JOIN carries `varnullingrels = {oj_relid}`, indicating that the value may be NULL because of that specific outer join. Below the LEFT JOIN, the same Var carries an empty `varnullingrels`.

Two same-name Vars with different `varnullingrels` are *not* equal, and they evaluate differently: the bare Var produces the original column value; the marked Var produces NULL when the join did not match.

### 20.3.2 Why PlaceHolderVar exists

A subexpression that involves *only* nullable-side columns might disappear without a trace if pulled above an outer join. The textbook example:

```sql
SELECT *
FROM t1 LEFT JOIN (SELECT x, x+1 AS y FROM t2) t2v ON (t1.k = t2v.x)
WHERE t2v.y IS NULL;
```

If `t2v` is pulled up (subquery flattening), the expression `x+1` would normally be evaluated wherever the planner placed it. But evaluating `x+1` *below* the outer join produces a non-null value even when the outer join produces a NULL row. The plan must defer evaluation to *above* the outer join, where the nullable side is correctly null-extended.

PlaceHolderVar (PHV) wraps such expressions:

```c
typedef struct PlaceHolderVar
{
    Expr        xpr;
    Expr       *phexpr;          /* expression to be computed */
    Relids      phrels;          /* base relids the expression references */
    Relids      phnullingrels;   /* set of OJs that have nulled this PHV */
    Index       phid;            /* unique ID */
    Index       phlevelsup;
} PlaceHolderVar;
```

`phrels` says "this expression conceptually belongs at a level that contains these rels"; `phnullingrels` is the analogue of varnullingrels for PHVs.

### 20.3.3 Where PHVs are constructed

`make_placeholder_expr` (`src/backend/optimizer/util/placeholder.c`) wraps an expression. The wrapping happens during preprocessing:

- During subquery pull-up (`pull_up_subqueries`) when the pulled-up tlist entry references nullable-side Vars.
- During UNION ALL pull-up when arms have differing expressions.
- When `reduce_outer_joins` cannot eliminate an OJ but needs to ensure expressions are evaluated above it.

### 20.3.4 PHV lifecycle in path generation

Each PHV gets a `PlaceHolderInfo` (in `root->placeholder_list`) recording where it must be evaluated. `mark_placeholders_in_expr` in `placeholder.c` walks the qual tree to discover PHV references and update their `ph_eval_at` (the lowest level at which the expression can be evaluated) and `ph_needed` (the highest level where it is referenced). Path-target builders honor these to decide whether a given path can produce the PHV.

`pull_varnos` (used by `make_restrictinfo`) treats `phrels + phnullingrels` exactly the way it treats `varno + varnullingrels`, so a RestrictInfo containing a PHV correctly propagates its requirement. See [Module 11.11](11_restrictinfo_and_clause_utils.md#1111-placeholdervar-interactions).

---

## 20.4 EquivalenceClass transitivity in the presence of outer joins

EquivalenceClasses (ECs) are the planner's machinery for transitive equality: if `a = b` and `b = c`, then a single EC contains `{a, b, c}` and the planner can derive `a = c` for free.

This works straightforwardly for inner joins. Outer joins break it.

### 20.4.1 The broken-EC problem

Consider:

```sql
SELECT * FROM a LEFT JOIN b ON a.x = b.y
WHERE b.y = b.z;
```

Naively merging `{a.x, b.y}` and `{b.y, b.z}` would produce one EC `{a.x, b.y, b.z}` and the planner would derive `a.x = b.z`. But if a row of `a` has no matching `b`, the LEFT JOIN extends with NULLs, and the WHERE `b.y = b.z` filters that row out — meaning the LEFT JOIN behaves like an INNER JOIN for those rows. Whether that is OK depends on subtle interactions between strictness and qual placement.

The planner handles this via **broken ECs**: ECs whose transitivity is no longer fully valid because they contain members that have crossed an outer join boundary in incompatible ways.

### 20.4.2 The mechanism

`process_equivalence` (`src/backend/optimizer/path/equivclass.c`) refuses to merge ECs across outer-join boundaries when doing so would be unsafe. The EC merging logic considers the `JoinDomain` of each member: members from inside an outer-join nullable side cannot be unconditionally merged with members from outside.

Each EC carries `ec_below_outer_join`, `ec_broken`, and per-member `em_jdomain` flags so downstream code (mergeclause selection, pathkey derivation) can be conservative when these flags are set.

### 20.4.3 reconsider_outer_join_clauses

After the initial qual distribution pass, `reconsider_outer_join_clauses` (`src/backend/optimizer/plan/initsplan.c`) revisits OJ-affected clauses:

- A WHERE clause that strictly references a nullable column proves that LEFT JOIN's null-extended rows are filtered out, so the LEFT JOIN can be demoted to an INNER JOIN.
- After the demotion, ECs that were originally broken can be re-merged.

This is the cleanup pass that recovers transitivity when possible. It uses `find_nonnullable_rels` and `find_forced_null_vars` from [Module 11](11_restrictinfo_and_clause_utils.md#117-strictness-analysis).

### 20.4.4 Practical impact

In practice, for queries with only inner joins, the EC machinery is fully transitive and very powerful. For queries with outer joins, some equalities that "should" be derivable are not — and the planner conservatively avoids the transitive derivation. This is a known soundness-vs-completeness tradeoff.

---

## 20.5 DP join search complexity

`standard_join_search` (`src/backend/optimizer/path/allpaths.c:3411`) performs dynamic programming over join levels. For an n-way inner join with no outer-join restrictions:

- Level 1: n base rels.
- Level 2: C(n, 2) pairs = n(n-1)/2 joinrels.
- Level k: ways to partition n rels into a level-k joinrel.
- Level n: a single joinrel containing all n rels.

The total number of joinrels considered (the size of `root->join_rel_list`) is the number of non-empty subsets of the n rels minus 1 (the empty set). That is `2^n - 1`, but each subset is itself the result of joining many possible pairs of smaller subsets, and DP enumerates all such joins.

### 20.5.1 The closed-form formula

The total enumeration cost is

```
S(n) = 3^n − 2^(n+1) + 1
```

This is the number of *bushy join trees* with n leaves, which is what `join_search_one_level` enumerates. Derivation sketch: at each level, for each subset S of size k > 1, we partition S into all non-trivial pairs (L, R) where L ∪ R = S and L ∩ R = ∅. The number of such partitions across all subsets is what the formula counts.

| n  | 3^n − 2^(n+1) + 1 | Approximate |
|----|-------------------|-------------|
|  6 | 605               | 0.6 k       |
|  8 | 6,049             | 6 k         |
| 10 | 57,005            | 57 k        |
| 12 | 530,401           | 530 k       |
| 14 | 4,749,861         | 5 M         |
| 16 | 41,949,121        | 42 M        |

The growth is much faster than `2^n` because each subset can be re-derived in many ways.

### 20.5.2 The geqo_threshold cutoff

`geqo_threshold` defaults to 12 in `src/backend/optimizer/path/allpaths.c:3374`:

```c
else if (enable_geqo && levels_needed >= geqo_threshold)
    return geqo(root, levels_needed, initial_rels);
else
    return standard_join_search(root, levels_needed, initial_rels);
```

For n = 12, DP considers ≈ 530k joinrels. With `add_paths_to_joinrel` doing meaningful work per joinrel (cost evaluation, parameterization analysis, partial-path generation), 530k joinrels means tens to hundreds of milliseconds of planning. Above that, GEQO's polynomial-effort genetic search becomes more attractive than DP's exponential exhaustion.

The threshold is not magic: lowering it (e.g. to 9) speeds up planning for medium-complex queries at the cost of plan quality. Raising it (e.g. to 16) trades plan quality for planning time. See [Module 15.9](15_geqo.md#159-tuning-levers).

### 20.5.3 Outer-join restrictions reduce the search

When the query has outer joins, `join_is_legal` rejects many candidate orderings (see SpecialJoinInfo machinery in [Module 05](05_initial_setup_and_jointree.md)). The reduction is dramatic: a 12-rel query with a chain of LEFT JOINs may have only a few thousand legal orderings, well within DP's reach.

That is why the threshold is set on `levels_needed` rather than total joinrels: the planner does not know in advance how many will be legal, and it would rather be safe (use GEQO) when the legal set is potentially huge.

---

## 20.6 GEQO chromosome encoding and crossover operator selection

A GEQO chromosome is a permutation of integers `1..nr_rel`. Each integer is a "gene" representing a base relation index into `initial_rels`. The chromosome is a single fixed-length string (`Gene *string`) of length `nr_rel`.

### 20.6.1 The decoder: gimme_tree

The decoder is not a simple left-to-right join. `gimme_tree` (`geqo_eval.c:163`) uses the **clump-merging heuristic** described in [Module 15.4.2](15_geqo.md#1542-gimme_tree-and-merge_clump). Two passes:

1. **Desirable-join pass** (`force=false`): iterate the tour, maintain a list of "clumps" (each clump is a partial joinrel), try to merge each new gene into an existing clump only if the join is "desirable" (has a relevant join clause or join-order restriction).
2. **Force pass** (`force=true`): if multiple clumps remain, force-join them in any legal order.

The first pass builds bushy trees aligned with join-clause connectivity. The second pass is a fallback for chromosomes whose tour does not naturally segment into clauses.

### 20.6.2 Why this is bushy-friendly

The original (1990s) GEQO emitted only left-deep trees: gene order = join order, no clumps. This is suboptimal because real workloads benefit from balanced bushy trees (lower intermediate cardinalities). The clump heuristic is bushy-friendly because clumps grow asymmetrically as relevant joins are found.

### 20.6.3 Crossover operators compared

Six crossover operators are available, all selected by compile-time macro in `src/include/optimizer/geqo.h`:

| Operator | Strategy | Strengths | Weaknesses |
|----------|----------|-----------|------------|
| **ERX** (default) | Edge Recombination — preserve adjacency | Best at preserving join-pair information | Can have edge failures (random fill-in) |
| **PMX** | Partially Matched — copy a segment, resolve duplicates by mapping | Preserves position of segment | Relatively destructive of adjacency |
| **CX** | Cycle Crossover — alternate cycles between parents | Strict inheritance from parents | Often produces clones (no diversity) |
| **PX** | Position — random subset of positions from one parent | Good positional inheritance | Random fill from other parent breaks order |
| **OX1** | Order — preserve a contiguous block, fill rest in order | Good order inheritance | Relatively similar to PMX |
| **OX2** | Order with multiple positions | More diverse than OX1 | Same fundamental concept |

ERX is the default because it best preserves the *adjacency information* in the tour, which corresponds to which relations were joined directly. The other operators are available for research.

### 20.6.4 Why operator choice is a build-time constant

Switching operators is rare and typically only done by GEQO researchers. Making it a runtime GUC would cost startup time per query (function-pointer dispatch) for negligible benefit. The compile-time macro approach trades flexibility for simplicity.

---

## 20.7 Cost-model GUC sensitivity and tuning workflow

The PostgreSQL cost model is a linear combination of GUCs. Knowing which GUCs influence which decisions, and in what magnitude, is essential for tuning.

### 20.7.1 The major cost GUCs

| GUC                       | Default | Affects |
|---------------------------|---------|---------|
| `seq_page_cost`           | 1.0     | Sequential page reads (SeqScan, BitmapHeapScan when sequential) |
| `random_page_cost`        | 4.0     | Random page reads (IndexScan, BitmapHeapScan when random) |
| `cpu_tuple_cost`          | 0.01    | Per-tuple processing in scan/join/agg |
| `cpu_index_tuple_cost`    | 0.005   | Per-index-tuple cost (B-tree traversal) |
| `cpu_operator_cost`       | 0.0025  | Per-operator evaluation (sort comparisons, hash compares) |
| `parallel_setup_cost`     | 1000.0  | Fixed per-Gather overhead |
| `parallel_tuple_cost`     | 0.1     | Per-tuple shipping cost worker→leader |
| `effective_cache_size`    | 4 GB    | Used by Mackert-Lohman for cached random reads |
| `work_mem`                | 4 MB    | Memory budget for sort/hash/material |
| `min_parallel_table_scan_size` | 8 MB | Threshold for parallel SeqScan |
| `min_parallel_index_scan_size` | 512 KB | Threshold for parallel IndexScan |
| `geqo_threshold`          | 12      | DP-vs-GEQO switch |

### 20.7.2 The two ratios that matter most

Most plan choices reduce to two ratios:

- **`random_page_cost / seq_page_cost`**: controls IndexScan-vs-SeqScan choice. Default 4.0 reflects spinning-disk reality from the early 2000s. On SSD/NVMe, lower it to 1.5-2.0; on cloud storage with high tail latencies, leave at 4.0 or raise to 6.0. Lowering this favors IndexScan and BitmapHeapScan; raising it favors SeqScan.
- **`cpu_tuple_cost / cpu_operator_cost`**: controls how aggressively the planner tries to filter early. Default 4.0 (0.01 / 0.0025) means a tuple costs 4 operators. Raising `cpu_tuple_cost` favors plans that filter many rows early; lowering it favors plans that defer filtering.

### 20.7.3 effective_cache_size and Mackert-Lohman

`effective_cache_size` does not allocate memory; it is a hint to the cost model about how many pages of the index/heap are likely to be cached. The Mackert-Lohman formula in `cost_index` uses it to estimate how many random page reads a parameterized inner-side scan will actually incur (vs. reading from cache).

Set `effective_cache_size` to about 50-75% of system RAM for a dedicated PG server. Wrong values produce dramatic plan instabilities — a too-low value makes parameterized nestloops look much more expensive than they actually are.

### 20.7.4 Tuning workflow

1. **Baseline**: run problem queries with `EXPLAIN (ANALYZE, BUFFERS)`. Note actual rows vs. estimates and actual times vs. cost.
2. **Verify statistics**: `ANALYZE` first. Most cost-model "bugs" are stale stats.
3. **Adjust I/O ratios** (`random_page_cost`, `seq_page_cost`, `effective_cache_size`) to match the storage and the buffer cache you actually have.
4. **Use `enable_*` flags as diagnostic toggles**: `SET enable_hashjoin = off; EXPLAIN ...` to see what the planner would do without that algorithm. Compare costs to find where the model is wrong.
5. **Adjust `work_mem`** if hash/sort spills are visible in EXPLAIN (ANALYZE).
6. **Raise `default_statistics_target`** for columns with skewed distributions; consider extended statistics for correlated columns.
7. **Tune parallelism** last (`max_parallel_workers_per_gather`, `parallel_setup_cost`, `min_parallel_table_scan_size`).

The order matters: I/O ratios before memory before parallelism. Each layer's defaults assume the previous layer is correctly tuned.

---

## 20.8 Selectivity estimation: MCV, histograms, ndistinct, multivariate stats

Selectivity is a number in [0, 1] estimating the fraction of rows a clause will return. The planner uses it to compute row counts (`rows = total_rows × selectivity`) and ultimately costs. Bad selectivity → bad costs → bad plans.

### 20.8.1 The four pieces of per-column statistics

`pg_statistic` rows store, per `(relid, attnum)`:

- **`stadistinct`**: number of distinct values. Negative means "fraction of distinct values relative to row count". `100` = exactly 100 distinct values; `-0.5` = half the rows have unique values.
- **MCV list** (`stakind = STATISTIC_KIND_MCV`): the K most common values and their frequencies. K is `default_statistics_target` (default 100). Used for equality selectivity on values present in the MCV.
- **Histogram** (`stakind = STATISTIC_KIND_HISTOGRAM`): equi-depth bucket boundaries. Each consecutive pair represents a bucket containing `1/(K+1)` fraction of the rows. Used for inequality selectivity (`<`, `>`, `BETWEEN`).
- **Correlation** (`stakind = STATISTIC_KIND_CORRELATION`): how closely the column's physical order matches its logical order. Used by `cost_index` to decide how random/sequential index scans will be.

### 20.8.2 How selectivity is computed

For `WHERE col = 'x'`:
1. Look up `'x'` in the MCV list. If found, selectivity = the MCV's frequency.
2. Else estimate from `(1 - sum_of_MCV_freqs) / (stadistinct - num_MCVs)`.

For `WHERE col < 'x'`:
1. Find which histogram bucket `'x'` falls in.
2. Linearly interpolate within the bucket.
3. Add the cumulative fraction of preceding buckets.

For `WHERE col1 = 'x' AND col2 = 'y'`:
1. Compute selectivity for each clause independently.
2. Multiply (assuming independence). This is the source of many estimation errors.

### 20.8.3 Multivariate (extended) statistics

When clauses are correlated (city and zip code, for example), independence assumption fails badly. `CREATE STATISTICS` lets the DBA declare extended statistics:

```sql
CREATE STATISTICS s_city_zip (dependencies, ndistinct, mcv) ON city, zip FROM addresses;
ANALYZE addresses;
```

Three kinds:
- **dependencies**: functional dependencies between columns (e.g. zip → city implies that knowing zip eliminates city's selectivity contribution).
- **ndistinct**: joint distinct-count estimates (used for GROUP BY estimation).
- **mcv**: joint MCV list capturing actual (city, zip) pair frequencies.

The cost estimator's `clauselist_selectivity` checks for applicable extended statistics first and uses them when available, falling back to per-column independence otherwise.

### 20.8.4 Common failure modes

- **Stale statistics**: ANALYZE has not run since data changed dramatically. Symptom: rowcount estimate way off in EXPLAIN ANALYZE. Fix: `VACUUM ANALYZE` or `ALTER TABLE ... SET (autovacuum_analyze_scale_factor = 0.05)`.
- **Insufficient statistics target**: skewed distributions need more MCV slots. Fix: `ALTER TABLE ... ALTER COLUMN ... SET STATISTICS 1000;` then ANALYZE.
- **Independence assumption violated**: correlated columns. Fix: extended statistics.
- **Correlated subqueries in WHERE**: the planner cannot estimate selectivities for arbitrary expressions. Fix: rewrite as JOIN, or accept the default.
- **Type mismatch**: `WHERE int_col = '42'::text` may bypass statistics if the cast is to a mismatched type.

---

## 20.9 Parameterized paths and ParamPathInfo

A parameterized path is one whose execution depends on values supplied from outside — typically Vars from an enclosing nestloop's outer side. The classic case: a nestloop with an indexed inner side.

```
Nested Loop
  ->  Seq Scan on outer
  ->  Index Scan on inner using inner_pkey
        Index Cond: (inner.k = outer.k)
```

The Index Scan is parameterized by `outer.k`. Its rows estimate is "rows per outer tuple" not "total rows".

### 20.9.1 ParamPathInfo struct

`src/include/nodes/pathnodes.h`:

```c
typedef struct ParamPathInfo
{
    Relids      ppi_req_outer;     /* rels supplying parameters */
    Cardinality ppi_rows;          /* estimated rows per execution */
    List       *ppi_clauses;        /* extra clauses enabled by parameterization */
    Bitmapset  *ppi_serials;        /* rinfo_serials of those clauses */
} ParamPathInfo;
```

A path's `param_info` field points to a ParamPathInfo. NULL means "not parameterized; rows is the total rowcount of the rel".

The `ppi_clauses` list contains clauses that the path can apply because the parameterization makes some Vars bound. For example, `outer.k = inner.k` in the example above lives in `ppi_clauses` of the Index Scan path; without parameterization, this clause could not be applied at the inner level.

### 20.9.2 ParamPathInfo deduplication

Parameterized paths for the same rel with the same `ppi_req_outer` share their ParamPathInfo. `get_baserel_parampathinfo` and `get_appendrel_parampathinfo` (`src/backend/optimizer/util/relnode.c`) cache ParamPathInfos in `rel->ppilist`, returning an existing one when the parameterization matches.

This deduplication matters because thousands of paths might have the same parameterization, and we want to reuse the rowcount estimate (which involves selectivity computation) rather than recompute it.

### 20.9.3 reparameterize_path_by_child

Partition-wise nestloop joins introduce a wrinkle: an inner path parameterized by the parent of a partitioned outer rel must be translated to be parameterized by the *child* of that outer rel for each partition. `reparameterize_path_by_child` (`src/backend/optimizer/util/pathnode.c`) walks the path tree and produces a child-translated copy.

This is also where `ReparameterizeForeignPathByChild` comes into play: FDWs must reparameterize their paths the same way, or partition-wise join with FDW children breaks.

### 20.9.4 Why GEQO ignores parameterization

GEQO's `geqo_eval` (`src/backend/optimizer/geqo/geqo_eval.c` comment near line 107) explicitly ignores parameterized paths during fitness evaluation:

> The fact that parameterized paths are not considered means that some plans that would be considered by the full join search are not considered here.

Consequence: GEQO sometimes produces worse plans for queries with many indexed nestloops. Workarounds: lower `geqo_threshold` if planning time is acceptable, or explicitly hint the join order via `pg_hint_plan`.

---

## 20.10 Subquery pull-up rules and the simple-subquery predicate

`pull_up_subqueries` (`src/backend/optimizer/prep/prepjointree.c:934`) flattens a FROM-clause subquery into the parent if the subquery is *simple*. The predicate is a conjunction of properties:

### 20.10.1 The is_simple_subquery checklist

From `is_simple_subquery` (prepjointree.c, ~line 1500):

1. **No LIMIT/OFFSET**: these clauses change the row set in ways that flattening would invalidate.
2. **No DISTINCT/HAVING**: these require aggregation or set-operation semantics.
3. **No aggregates / window functions / SRFs / CTEs at top level**: aggregates impose grouping; window functions are positional; SRFs change row counts; CTEs are independent planning units.
4. **No SELECT INTO**: side-effecting.
5. **No security barrier conflict**: the parent and subquery must agree on `security_barrier` flags.
6. **Non-empty jointree**: a FROM-less subquery is a Result, handled differently.
7. **No locking clauses (FOR UPDATE etc.)**: locking semantics are tied to the original scan.
8. **No volatile functions in tlist where they would change semantics**: a volatile function evaluated multiple times after pull-up gives different answers.
9. **No reference to outer query Vars** that would create lateral dependencies the planner cannot satisfy after pull-up.

If all are true, the subquery is pulled up: its range table merges with the parent's, its quals merge into the parent's WHERE/ON clauses, and its tlist becomes referenceable from the parent's expressions.

### 20.10.2 UNION ALL pull-up

`pull_up_simple_union_all` is the cousin: a UNION ALL of simple subqueries can be pulled up as an AppendRelInfo (parent rel = the whole UNION; children = each arm). This unlocks partition-wise plans on the union.

Eligibility: each arm must be a `pull_up_subqueries`-eligible plain SELECT, and the arms must agree on output types and column count.

### 20.10.3 Lateral references

A LATERAL subquery references the parent's Vars. Pulling up requires the LATERAL machinery to track which rels supply the parent's Vars; this is encoded in `lateral_relids` on the child rel after pull-up. Some LATERAL patterns (e.g. with non-trivial join clauses) are not pull-uppable and remain as SubqueryScans.

### 20.10.4 When pull-up is *not* a win

For very large subqueries, pull-up can dramatically increase the planner's join-search complexity. The planner does not pre-decide; it pulls up when eligible and pays the planning cost. For OLTP queries with deep view nesting, this is fine because the subqueries are small. For analytic queries, consider materialized views or explicit `WITH MATERIALIZED` to keep the planner's work manageable.

---

## 20.11 Hash-aggregate spill-to-disk and the planner's row-count bound

PG 13 introduced disk-spilling for hash aggregates, lifting a longstanding limitation that hashagg required the entire hash table to fit in `work_mem`. The planner's role: estimate when spill will happen and cost it correctly.

### 20.11.1 The pre-PG13 limitation

Before PG13, `cost_agg` for `AGG_HASHED` would refuse to consider hashagg if the estimated hash table size exceeded `work_mem`. Queries whose estimates were wrong (underestimated `numGroups`) would either fall back to AGG_SORTED or, worse, OOM at runtime.

### 20.11.2 The PG13+ model

`cost_agg` (`src/backend/optimizer/path/costsize.c:2650`) now accepts that hashagg may spill. The cost model:

```
hash_size = numGroups * transitionSpace
if hash_size > work_mem:
    spill_pages = (hash_size - work_mem) / BLCKSZ
    spill_cost = spill_pages * (seq_page_cost + cpu_tuple_cost)
else:
    spill_cost = 0
total = hash_construction_cost + spill_cost + per_tuple_cost
```

The `numGroups` estimate comes from `estimate_num_groups` (`selfuncs.c`) which uses `stadistinct` for grouping columns and the multivariate ndistinct for combinations.

### 20.11.3 Planner's row-count bound

Even with spill support, very-many-groups can still kill performance because every spill batch must be re-read and re-aggregated. The planner sets a soft bound: hashagg is preferred only when `numGroups × transitionSpace < ~10 × work_mem`. Beyond that, AGG_SORTED becomes competitive again because sort spills are sequential while hash spills are random-ish.

The actual decision is made by `add_path` comparing the AGG_HASHED and AGG_SORTED paths' costs. The bound is implicit in the cost formulas.

### 20.11.4 Tuning

- For aggregations with many groups, raise `work_mem` to fit the hash table in memory. Consider session-level: `SET work_mem = '256MB';` before the query.
- If `numGroups` is hard to estimate (correlated grouping columns), use extended statistics (`CREATE STATISTICS ... ndistinct`).
- For analytic workloads, `enable_partitionwise_aggregate` plus matching partition keys can break a large group-by into per-partition group-bys, each fitting in work_mem.

---

## 20.12 Partition pruning at plan vs execution time

Partition pruning eliminates partitions that cannot match the query's filters. There are two kinds: plan-time (based on constants known at planning) and run-time (based on Params known only at execution).

### 20.12.1 Plan-time pruning

`prune_append_rel_partitions` (`src/backend/partitioning/partprune.c`) is called from `expand_partitioned_rtentry`. Steps:

1. For each clause in `rel->baserestrictinfo`, `match_clause_to_partition_key` checks if the clause is testable against a partition key (must be stable expressions and constants — not Params, not volatile).
2. `gen_partprune_steps` builds a `PartitionPruneStep` tree: per-partition-key operator steps combined by AND/OR.
3. `get_matching_partitions` evaluates the steps using the constants; partitions whose bounds are excluded are dropped from `live_parts`.
4. Excluded partitions never get RelOptInfos; they are absent from the plan entirely.

Plan-time pruning happens before path generation, so the pruned partitions cost nothing in planning or execution.

### 20.12.2 Run-time pruning

When clauses involve Params (PreparedStatement parameters or nestloop parameters), pruning cannot happen at plan time. Instead, `make_partition_pruneinfo` builds a `PartitionPruneInfo` attached to the Append/MergeAppend Plan node. The executor evaluates pruning steps:

- At init time using initial Param values (`exec_init_partition_prune` from `nodeAppend.c`).
- Per-rescan when nestloop parameter values change (`exec_partition_prune_subnodes`).

Excluded partitions are skipped at the Append's iteration level. The plan tree still contains them (as sub-Plans of the Append), but they never execute.

### 20.12.3 PartitionPruneStep shapes

```c
typedef struct PartitionPruneStepOp { ... }  /* apply one operator-clause to bounds */
typedef struct PartitionPruneStepCombine { ... }  /* AND/OR of step results */
```

The steps form a tree rooted at the Append. For `WHERE p_key = $1 AND p_key2 IN (1, 2, 3)`, the tree might be a Combine(AND, [StepOp(=, $1), StepOp(IN, [1,2,3])]).

### 20.12.4 Which kind to expect

| Clause kind                                  | Plan-time? | Run-time? |
|----------------------------------------------|------------|-----------|
| `WHERE p_key = 42`                           | yes        | (n/a)     |
| `WHERE p_key = $1` (PreparedStatement)        | no         | yes       |
| `WHERE p_key = outer.k` (nestloop)            | no         | yes (per outer tuple) |
| `WHERE p_key = (SELECT max(x) FROM t)`        | no         | yes (init-pruning, runs once via InitPlan) |
| `WHERE p_key = now()` (volatile)              | no         | no (volatile blocks both) |
| `WHERE p_key = current_setting('foo')` (stable)| only if value known at plan | no |

### 20.12.5 EXPLAIN reading

In EXPLAIN, look for:
- `Subplans Removed: N` — plan-time pruning eliminated N partitions.
- `Workers Planned: N` — irrelevant to pruning, but pruning combines with parallel append.

In EXPLAIN ANALYZE:
- `Subplans Removed: N (init)` — runtime init-pruning.
- `Subplans Removed: N (exec)` — per-rescan pruning.

---

## 20.13 Parallel-safe vs parallel-restricted classification

Three levels of parallelism eligibility per `pg_proc.proparallel`:

- `s` (PROPARALLEL_SAFE): may be evaluated in a worker.
- `r` (PROPARALLEL_RESTRICTED): may be evaluated only in the leader.
- `u` (PROPARALLEL_UNSAFE): must not run while any parallelism is active.

### 20.13.1 The hazard hierarchy

`max_parallel_hazard` (`src/backend/optimizer/util/clauses.c`) walks an expression tree returning the maximum hazard found:

```c
char max_parallel_hazard(Query *parse);
```

Hazards include:
- Volatile or stable functions marked unsafe/restricted.
- Temp tables (workers cannot see them).
- RowMarks (`SELECT FOR UPDATE`) on tables.
- Modifying CTEs (`WITH ... INSERT ...`).
- User-defined aggregates without `combinefunc` (cannot combine partials).
- Foreign tables not opting into parallel via `IsForeignScanParallelSafe`.
- `Vars` referencing the leader's `upper_params` (these can only be evaluated in the leader's context).

The worst hazard found becomes `glob->maxParallelHazard`. `parallelModeOK` is set true iff the worst hazard is not UNSAFE.

### 20.13.2 The path-level flag

`parallel_safe` on each Path is the *upward propagation* of safety. Rules:

- For scans: `parallel_safe = rel->consider_parallel`.
- For joins: `parallel_safe = joinrel->consider_parallel && outer->parallel_safe && inner->parallel_safe`.
- For upper-relation paths (Sort, Agg, etc.): `parallel_safe = rel->consider_parallel && subpath->parallel_safe`, AND-ed with `is_parallel_safe()` checks on tlist and qual expressions.
- ModifyTable paths and LockRows paths force `parallel_safe = false` because writes cannot be parallelized.

A Gather can wrap a path only if that path is `parallel_safe`. Inside a parallel section, every node must be at least RESTRICTED-safe (UNSAFE expressions cannot appear).

### 20.13.3 Practical guidance for extension authors

When you write a function:

- If it has no side effects and does not access session state: mark `PARALLEL SAFE` (`s`).
- If it has side effects only in the leader (e.g. reads a session GUC): `PARALLEL RESTRICTED` (`r`). Workers cannot evaluate it, but the leader can in a parallel-restricted context.
- If it modifies global state that must not be touched by workers: `PARALLEL UNSAFE` (`u`).

Default for SQL functions: `u` (unsafe). You must explicitly opt in. C functions default to `s` only if their `provolatile` is `i` (immutable); otherwise default is `u`.

### 20.13.4 Diagnosing parallelism not chosen

```sql
EXPLAIN (VERBOSE) SELECT ...;
```

If you do not see `Workers Planned`, then parallelism was not chosen. Common reasons:
- `force_parallel_mode = 'off'` and the planner judged parallel not worth it.
- Some expression in the query is not parallel-safe. Check `pg_proc.proparallel` on every function in the query.
- `max_parallel_workers_per_gather = 0`.
- `max_parallel_hazard != PROPARALLEL_SAFE` due to a hidden function (e.g. an implicit cast).

`SET force_parallel_mode = 'regress';` forces a Gather wrap for testing — useful for catching hidden hazards.

---

## 20.14 Memoize node decision and cost

Memoize is a per-call cache for the inner side of a parameterized nestloop. When the outer side has many duplicate parameter values, Memoize avoids re-running the inner subpath.

### 20.14.1 When Memoize is considered

`add_paths_to_joinrel` considers a Memoize wrapper for the inner path of a nestloop when:

- The inner path is parameterized by the outer rel's Vars.
- The cache key (the parameter expressions) is hashable.
- The estimated number of distinct parameter values is much less than the outer's row count (so caching is worthwhile).
- `enable_memoize = on` (the GUC).

### 20.14.2 The cost model

`cost_memoize_rescan` (`src/backend/optimizer/path/costsize.c:2509`) estimates the hit ratio:

```
distinct_params = ndistinct(param_exprs)
cache_capacity = work_mem / avg_entry_size
hit_ratio = min(1, cache_capacity / distinct_params)

per_call_cost = hit_ratio * cache_lookup_cost + 
                (1 - hit_ratio) * subpath_total_cost
total_inner_cost = outer_rows * per_call_cost
```

If `hit_ratio` is close to 1 (cache fits all distinct keys), Memoize wins big over plain rescan. If `hit_ratio` is small (cache constantly evicts), Memoize loses to Material (which buffers everything once) or to plain rescan.

### 20.14.3 The planner's row-count bound

`est_entries` is the planner's estimate of how many entries the cache will hold. It is bounded by `work_mem / entry_size`. If the actual workload exceeds this, the executor evicts older entries (LRU). Excess evictions degrade performance.

### 20.14.4 When Memoize loses

- High-cardinality parameter values: cache thrashes, no hit benefit. `est_entries` is small relative to outer rows; the cost model rejects Memoize.
- Cheap inner subpath: the per-call savings are smaller than the cache management overhead. Cost model picks plain nestloop.
- Inner is unique per parameter: every cache lookup is a miss; pure overhead. The planner should detect this via `inner_unique`, but watch EXPLAIN ANALYZE.

### 20.14.5 Operational tips

- If EXPLAIN ANALYZE shows `Cache Misses: many, Cache Hits: few`, the cache is not earning its keep. Either `SET enable_memoize = off` for the query or raise `work_mem`.
- For NUMA systems, parallel workers each have their own Memoize cache; the cumulative memory is `parallel_workers × work_mem × est_entries`. Plan capacity accordingly.

---

## 20.15 Planner hooks in the wild: pg_hint_plan, citus, timescaledb

Three production extensions illustrate three different uses of the planner's hook API.

### 20.15.1 pg_hint_plan

Uses: `planner_hook`, `set_rel_pathlist_hook`, `set_join_pathlist_hook`, `join_search_hook`.

Pattern:

1. `_PG_init` saves prev hooks, installs its own.
2. `pg_hint_plan_planner` (the `planner_hook`) parses `/*+ ... */` comments at the start of `query_string`. Stores the parsed hint table in TLS.
3. `pg_hint_plan_set_rel_pathlist` (the `set_rel_pathlist_hook`) — when a Scan hint exists for the rel, prunes the rel's pathlist to keep only paths whose `pathtype` matches the hint.
4. `pg_hint_plan_set_join_pathlist` (the `set_join_pathlist_hook`) — when a Join method hint exists for this pair, prunes accordingly.
5. `pg_hint_plan_join_search` (the `join_search_hook`) — when a Leading hint exists, manually builds joinrels in the requested order using `make_join_rel` directly. Other orderings are simply not generated.
6. After `standard_planner` returns, the TLS hint table is cleared.

The hints are advisory: pg_hint_plan does not refuse to plan if a hint is impossible. It downgrades to a warning and falls back to the standard search.

### 20.15.2 citus

Citus shards tables across a cluster. Its planner integration is much deeper than pg_hint_plan:

- `planner_hook`: routes single-shard queries to a shard, or distributes multi-shard queries via "distributed planner".
- `set_rel_pathlist_hook`: for distributed tables, replaces the standard scan paths with `CustomPath`s representing remote scans.
- `set_join_pathlist_hook`: for joins between distributed tables, evaluates whether the join can be pushed entirely to workers (co-located join).
- `create_upper_paths_hook`: for aggregates over distributed tables, builds custom paths that compute partial aggregates on each shard plus a coordinator-side combine.

Citus's CustomScan provider (`citus_custom_scan_methods`) has its own `PlanCustomPath` that emits the executor-side custom node.

### 20.15.3 timescaledb

Timescaledb uses `set_rel_pathlist_hook` and `set_join_pathlist_hook` to:

- Insert chunk-skipping paths over hypertable chunks (using time-range constraints).
- Replace the default planning of large hypertable queries with pruning-aware paths.
- Add custom aggregation paths for time-bucketed aggregations (`time_bucket` is a stable function; pruning works at plan time).

Both `set_rel_pathlist_hook` and `set_join_pathlist_hook` are called *after* standard logic, so timescaledb sees what the standard planner would have done and adds or replaces paths accordingly.

### 20.15.4 Coexistence considerations

When multiple extensions install hooks, ordering matters. The convention "save prev, then install" means hooks are called in *reverse* of installation order (newest hook first, oldest last). For `planner_hook` this is fine because it is a wrapper. For `set_rel_pathlist_hook`, multiple hooks may legitimately add or remove paths; the result depends on call order.

Best practices for extension authors:

- Never assume your hook is the only one.
- Always chain to `prev_*_hook` before doing your own work, unless you explicitly want to override.
- Document your installation order requirements in your README.
- Test with at least one other planner-touching extension installed.

### 20.15.5 Debugging hook chains

```sql
LOAD 'pg_hint_plan';
LOAD 'timescaledb';
EXPLAIN (VERBOSE) SELECT ...;
```

If the resulting plan is unexpected, isolate by selectively `LOAD`ing extensions. Use `auto_explain.log_min_duration = 0` plus `log_statement = 'all'` to capture every plan with full detail.

---

## Cross-references

- The body of evidence behind these deep dives is distributed across modules 04 through 19. Each section above references the relevant production module.
- The Stage 2 documentation under `topic_specific_generated_docs/about_planner/stage2/` contains the per-component sources.
- For the canonical algorithmic descriptions, consult `src/backend/optimizer/README` directly — many of the "Why" sections in these deep dives summarize what that file states authoritatively.

End of main book. Continue to the appendices for the symbol index, glossary, and navigation aids.
