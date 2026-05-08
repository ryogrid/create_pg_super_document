# Path Catalog: Append and Partition-wise Paths

This file covers the two Path subtypes that combine the outputs of multiple subpaths into a single relation: `AppendPath` and `MergeAppendPath`. Both are also the substrate for partition-wise plans (each partition's scan becomes a child path), and for inheritance hierarchies (regular table inheritance reuses the same machinery).

---

## AppendPath (T_AppendPath)

**Identity**: struct `AppendPath` defined at `src/include/nodes/pathnodes.h:1931`.

```c
typedef struct AppendPath
{
    Path        path;
    List       *subpaths;           /* list of component Paths */
    int         first_partial_path; /* index of first partial subpath */
    Cardinality limit_tuples;       /* hard limit on output tuples, or -1 */
} AppendPath;
```

**Purpose**: Represents an Append plan — concatenating tuples from several subpaths, in arbitrary order. Used for:
1. Partitioned-table or inheritance-hierarchy scans (one subpath per partition/child).
2. UNION ALL of subqueries.
3. Empty-result placeholders (an AppendPath with `subpaths == NIL` represents a provably-empty relation; macro `IS_DUMMY_APPEND` and `IS_DUMMY_REL` test for this).
4. Parallel Append: combines partial and non-partial subpaths under a single shared-state coordinator.

**Constructor**: `create_append_path(PlannerInfo *root, RelOptInfo *rel, List *subpaths, List *partial_subpaths, List *pathkeys, Relids required_outer, int parallel_workers, bool parallel_aware, double rows)` at `src/backend/optimizer/util/pathnode.c:1244`.
   - Allocation: `makeNode(AppendPath)`.
   - Notable behaviors:
     - For parallel-aware Append, sorts non-partial paths by descending total cost and partial paths by descending startup cost (so workers pick expensive jobs first while leader picks cheapest startup).
     - Sets `first_partial_path = list_length(subpaths)` then concatenates partial paths after non-partial.
     - Special-cases single-child Append: cost equals child's cost (planner will collapse the Append in setrefs.c).
     - Applies query-wide LIMIT to `limit_tuples` if the rel covers the whole query.
   - Cost computation: inline `cost_append(pathnode)`.

**Cost function**: `cost_append()` at `src/backend/optimizer/path/costsize.c:2231`.
   - Formula summary: For non-parallel: `startup_cost = first_subpath->startup_cost`, `total_cost = sum(subpath->total_cost)`. For parallel: estimates per-worker share of work, accounts for non-partial paths each running on a single worker.
   - GUC dependencies: indirectly through subpath costs.

**Pathkey behavior**: Caller-supplied. Append generally doesn't preserve ordering, but for partition-wise plans where each child is sorted compatibly and pathkeys are inherited, an ordered Append is meaningful.

**Parameterization**: Yes. The constructor checks that all subpaths have the same `PATH_REQ_OUTER` and uses `get_baserel_parampathinfo` (for baserels) or `get_appendrel_parampathinfo` (for joinrels and partition trees) to set `param_info`.

**Parallel-aware**: Yes when `parallel_aware = true`. Both leader and workers participate; non-partial children run start-to-finish on a single worker each, partial children are split across multiple workers.

**Plan counterpart**: `create_append_plan()` at `src/backend/optimizer/plan/createplan.c:1217` produces `Append` (`plannodes.h:265`). Includes `apprelids` (RTIs of the appendrels), `nasyncplans` count for foreign-table async execution, `first_partial_plan`, and `part_prune_info` for runtime partition pruning.

**When chosen**: Always used for partitioned tables and inheritance hierarchies that aren't proven to need ordering preserved. Also for UNION ALL.

**Example SQL**:
```sql
SELECT * FROM partitioned_table;
-- Append
--   -> Seq Scan on partition_1
--   -> Seq Scan on partition_2
--   -> Seq Scan on partition_3
```

---

## MergeAppendPath (T_MergeAppendPath)

**Identity**: struct `MergeAppendPath` defined at `src/include/nodes/pathnodes.h:1955`.

```c
typedef struct MergeAppendPath
{
    Path        path;
    List       *subpaths;           /* list of component Paths */
    Cardinality limit_tuples;       /* hard limit on output tuples, or -1 */
} MergeAppendPath;
```

**Purpose**: Represents a MergeAppend plan — k-way merge of presorted subpaths, preserving overall sort order. Used when:
1. Querying a partitioned table with `ORDER BY` matching a partition-key prefix where each partition can be scanned in order.
2. Querying an inheritance hierarchy with ordered output requirements.

**Constructor**: `create_merge_append_path(PlannerInfo *root, RelOptInfo *rel, List *subpaths, List *pathkeys, Relids required_outer)` at `src/backend/optimizer/util/pathnode.c:1415`.
   - Allocation: `makeNode(MergeAppendPath)`.
   - Notable behavior: For each subpath, if its pathkeys don't match the desired pathkeys, computes the cost of inserting a Sort node above it (using a dummy `cost_sort` call) and accumulates that into total cost.
   - Special-cases single-subpath: degenerates into the child's own cost (will be collapsed in setrefs.c).
   - Cost computation: `cost_merge_append()` (for the multi-subpath case).

**Cost function**: `cost_merge_append()` at `src/backend/optimizer/path/costsize.c:2404`.
   - Formula summary: input costs (with implicit Sort costs included from constructor) + heap-based merge cost (`2.0 * cpu_operator_cost * N * log2(num_subpaths)`) over all output tuples.
   - GUC dependencies: `cpu_operator_cost`, `cpu_tuple_cost`.

**Pathkey behavior**: Output pathkeys = the desired sort order (caller-supplied). Each subpath must produce output ordered by these pathkeys (Sort nodes are inserted at plan time to enforce this).

**Parameterization**: Limited — the constructor uses `get_appendrel_parampathinfo` (no `get_baserel_parampathinfo` fallback), and there's an assert in `create_merge_append_plan` that `param_info == NULL`. Currently the planner doesn't generate parameterized MergeAppend.

**Parallel-aware**: No (`parallel_aware = false` always; `parallel_workers = 0`). MergeAppend's heap-based merge is not parallelizable.

**Plan counterpart**: `create_merge_append_plan()` at `src/backend/optimizer/plan/createplan.c:1438` produces `MergeAppend` (`plannodes.h:287`). Plan generation also runs `prepare_sort_from_pathkeys` to set `sortColIdx`/`sortOperators`/`collations`/`nullsFirst` arrays for the executor's heap, and inserts explicit Sort nodes for unordered children. Like Append, supports `part_prune_info` for runtime partition pruning.

**When chosen**: When (a) querying a partitioned table with ORDER BY where each partition can be cheaply produced in order (common case: indexed scans on each partition), and (b) the alternative (Append + Sort over the whole result) is more expensive.

**Example SQL**:
```sql
SELECT * FROM partitioned_t ORDER BY partition_key;
-- Merge Append
--   Sort Key: partition_key
--   -> Index Scan partition_1 (ordered)
--   -> Index Scan partition_2 (ordered)
--   -> Index Scan partition_3 (ordered)
```

---

## Partition-wise Variants

There are no separate "PartitionwiseAppendPath" or similar Path types — partition-wise plans are constructed using ordinary AppendPath or MergeAppendPath whose subpaths are themselves base relation paths (for partition-wise scans), join paths (for partition-wise join), or aggregate paths (for partition-wise aggregation). The relevant facilities are:

- **Partition-wise scan**: each leaf partition becomes a child relation of the partitioned table's RelOptInfo, and `set_append_rel_pathlist()` calls `add_paths_to_append_rel()` to build an AppendPath over each partition's cheapest path.
- **Partition-wise join**: when `enable_partitionwise_join = on` and the partitioning of two joined tables matches, `try_partitionwise_join()` (in `joinrels.c`) builds joinrels for each pair of matching partitions, then an AppendPath aggregates them.
- **Partition-wise aggregation**: when `enable_partitionwise_aggregate = on`, partial aggregates are computed per partition and then combined via an AppendPath plus a Finalize Agg.
- **Runtime partition pruning** is encoded as `PartitionPruneInfo` attached to the resulting `Append` or `MergeAppend` plan, set from the path's `subpaths` list against `rel->baserestrictinfo` and parameterized clauses in `create_append_plan` / `create_merge_append_plan`. The executor consults this at scan-start (init pruning) and per-Param-change (exec pruning) to skip subplans entirely.
