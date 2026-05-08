# Path Catalog: Join Paths

This file documents the three Path subtypes that represent join methods. All three derive from the abstract `JoinPath` struct (`pathnodes.h:2065`), which carries the join type, inner-unique flag, outer/inner subpaths, and the join restriction clauses:

```c
typedef struct JoinPath
{
    pg_node_attr(abstract)
    Path        path;
    JoinType    jointype;
    bool        inner_unique;       /* outer matches at most one inner */
    Path       *outerjoinpath;
    Path       *innerjoinpath;
    List       *joinrestrictinfo;   /* RestrictInfos to apply at join */
} JoinPath;
```

Join paths are produced from `add_paths_to_joinrel()` in `joinpath.c` for every viable join method on each pair of relations. The three concrete variants below cover the join algorithms that core PostgreSQL implements.

The `inner_unique` flag is critical to runtime semantics: when set, the executor stops searching the inner side after the first match, which converts a semi-join into an inner-join-with-stop and accelerates inner-unique nestloops, mergejoins, and hashjoins.

---

## NestPath (T_NestPath)

**Identity**: struct `NestPath` defined at `src/include/nodes/pathnodes.h:2092`.

```c
typedef struct NestPath
{
    JoinPath    jpath;
} NestPath;
```

(No fields beyond the abstract `JoinPath` — nested-loop joins need no extra metadata.)

**Purpose**: Represents a nested-loop join. For each outer tuple, the inner subpath is rescanned (or, when parameterized, re-executed with the outer tuple's values bound to nestloop Params).

**Constructor**: `create_nestloop_path(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype, JoinCostWorkspace *workspace, JoinPathExtraData *extra, Path *outer_path, Path *inner_path, List *restrict_clauses, List *pathkeys, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:2457`.
   - Allocation: `makeNode(NestPath)`.
   - Cost computation: `final_cost_nestloop(root, pathnode, workspace, extra)` at the end. The two-stage cost model uses `initial_cost_nestloop` to short-circuit clearly-uncompetitive paths before populating the full path.
   - Side effect: drops `restrict_clauses` that are already enforced inside the inner parameterized path (matched by `rinfo_serial`) so they aren't re-evaluated at the join.

**Cost function**: `initial_cost_nestloop()` (`costsize.c:3233`) + `final_cost_nestloop()` (`costsize.c:3308`).
   - Formula summary: `outer_path->total_cost + outer_rows * inner_rescan_cost + cpu_per_tuple * outer*inner`. Inner rescan cost is computed by `cost_rescan()` and accounts for materialization and parameter changes. For semi/anti joins with `inner_unique`, the formula uses `outer_rows * inner_path->total_cost` only on a fraction of outer rows.
   - GUC dependencies: `cpu_tuple_cost`, `cpu_operator_cost`.

**Pathkey behavior**: Caller-supplied; usually equals the outer path's pathkeys (a nestloop preserves outer ordering as long as it's not a left/anti join with non-empty inner).

**Parameterization**: The classic case — inner path is typically parameterized by the outer rel, enabling indexed inner lookups. `required_outer` set excludes outer-rel itself.

**Parallel-aware**: No (the NestPath itself is not parallel-aware), but `parallel_safe` propagates from outer/inner subpaths.

**Plan counterpart**: `create_nestloop_plan()` at `src/backend/optimizer/plan/createplan.c:4348` produces `NestLoop` (`plannodes.h:807`).

**When chosen**: When (a) the inner side is small or a parameterized index lookup makes per-outer-tuple inner scan cheap, (b) when no equijoin clauses make hash/merge applicable, or (c) for FOR EACH ROW semantics like LATERAL.

**Example SQL**: `SELECT * FROM small s JOIN big b ON b.id = s.fk;` (with index on b.id) → `Nested Loop -> Seq Scan on s -> Index Scan on big using big_pkey`

---

## MergePath (T_MergePath)

**Identity**: struct `MergePath` defined at `src/include/nodes/pathnodes.h:2132`.

```c
typedef struct MergePath
{
    JoinPath    jpath;
    List       *path_mergeclauses;  /* join clauses used for merge */
    List       *outersortkeys;      /* keys for explicit sort, if any */
    List       *innersortkeys;      /* keys for explicit sort, if any */
    bool        skip_mark_restore;  /* can executor skip mark/restore? */
    bool        materialize_inner;  /* add Materialize to inner? */
} MergePath;
```

**Purpose**: Represents a merge-join over two presorted (or to-be-sorted) input streams. As the comment in pathnodes.h observes, a single MergePath may compile down to up to four executor nodes: MergeJoin, an outer Sort, an inner Sort, and an inner Material — combined in `create_mergejoin_plan` rather than represented as separate paths.

**Constructor**: `create_mergejoin_path(...)` at `src/backend/optimizer/util/pathnode.c:2553`.
   - Allocation: `makeNode(MergePath)`.
   - Cost computation: `final_cost_mergejoin(root, pathnode, workspace, extra)`. `initial_cost_mergejoin` is called earlier for early pruning.
   - Side effect: `final_cost_mergejoin` decides whether `skip_mark_restore` and `materialize_inner` apply.

**Cost function**: `initial_cost_mergejoin()` (`costsize.c:3514`) + `final_cost_mergejoin()` (`costsize.c:3745`).
   - Formula summary: outer_path cost + (cost of outer Sort if `outersortkeys != NIL`) + inner_path cost + (cost of inner Sort if needed) + (cost of inner Material if needed) + per-comparison CPU cost across both rescanned streams. Also accounts for early termination when one side runs out.

**Pathkey behavior**: Output pathkeys equal the outer path's mergeclause pathkeys (with caveats from `truncate_useless_pathkeys`). Useful upstream when the next operator wants the same ordering.

**Parameterization**: Yes — but unusual; typically MergePath is for non-parameterized joins.

**Parallel-aware**: No directly; `parallel_safe` is the AND of its inputs.

**Plan counterpart**: `create_mergejoin_plan()` at `src/backend/optimizer/plan/createplan.c:4440` produces `MergeJoin` (`plannodes.h:833`), possibly with synthetic `Sort` nodes (via `make_sort_from_pathkeys`) and a `Material` node injected when `materialize_inner` is set.

**When chosen**: When both inputs are already sorted (or cheaply sortable) on the join keys, especially for large equijoin-driven joins. Also chosen for full outer joins where the only viable algorithms are merge or hash.

**Example SQL**: `SELECT * FROM a JOIN b ON a.k = b.k` with both sides indexed on k → `Merge Join -> Index Scan a -> Index Scan b`

---

## HashPath (T_HashPath)

**Identity**: struct `HashPath` defined at `src/include/nodes/pathnodes.h:2151`.

```c
typedef struct HashPath
{
    JoinPath    jpath;
    List       *path_hashclauses;   /* join clauses used for hashing */
    int         num_batches;        /* number of batches expected */
    Cardinality inner_rows_total;   /* total inner rows expected */
} HashPath;
```

**Purpose**: Represents a hash-join. Inner side is built into a hash table; outer side is probed.

**Constructor**: `create_hashjoin_path(...)` at `src/backend/optimizer/util/pathnode.c:2619`. Accepts a `parallel_hash` flag that selects Parallel Hash (shared hash table built collaboratively by workers).
   - Allocation: `makeNode(HashPath)`.
   - Cost computation: `final_cost_hashjoin(root, pathnode, workspace, extra)`. Fills in `num_batches`.

**Cost function**: `initial_cost_hashjoin()` (`costsize.c:4073`) + `final_cost_hashjoin()` (`costsize.c:4181`).
   - Formula summary: cost of building the inner hash table (`inner_path->total_cost + cost of hashing each row`) plus cost of probing (`outer_rows * (hash_cost + match_check_cost)`). Multi-batch overhead added when the hash table won't fit in `work_mem`.

**Pathkey behavior**: Always `NIL` — a hashjoin's output ordering is unpredictable, especially with batching. The pathnodes.h comment explicitly notes this is conservative; in single-batch cases the outer ordering could in principle be preserved, but the planner doesn't bet on that.

**Parameterization**: Yes via `required_outer`.

**Parallel-aware**: Yes when `parallel_hash = true`. The hash table becomes a shared DSM region built collaboratively by all workers participating in the join.

**Plan counterpart**: `create_hashjoin_plan()` at `src/backend/optimizer/plan/createplan.c:4747` produces `HashJoin` (`plannodes.h:862`). Critically, this creator also synthesizes a `Hash` plan node (`plannodes.h:1197`) wrapping the inner subplan — Hash is the only Plan node never directly produced from a Path; it appears solely as a child of HashJoin.

**When chosen**: For large equijoins where neither side is presorted, when one side fits comfortably in `work_mem`, or when the outer side is so much larger than the inner that hashing the inner is clearly the cheapest path.

**Example SQL**: `SELECT * FROM big_a JOIN big_b ON a.k = b.k;` (both unsorted, inner small enough) → `Hash Join -> Seq Scan big_a -> Hash -> Seq Scan big_b`
