# Component: Inheritance and Partitioning

> Stage 2 documentation for **INHERIT_AND_PARTITION**.
> Sources:
> - `src/backend/optimizer/util/inherit.c`: `expand_inherited_rtentry`,
>   `expand_partitioned_rtentry`, child RTE creation.
> - `src/backend/optimizer/util/appendinfo.c`: `AppendRelInfo` helpers
>   (`make_append_rel_info`, `adjust_appendrel_attrs`,
>   `find_appinfos_by_relids`).
> - `src/backend/optimizer/path/allpaths.c`:
>   `set_append_rel_size`, `set_append_rel_pathlist`,
>   `add_paths_to_append_rel`, `generate_orderedappend_paths`,
>   `accumulate_append_subpath`, `get_cheapest_parameterized_child_path`.
> - `src/backend/partitioning/partprune.c`: plan-time and run-time
>   partition pruning.
> - `src/backend/optimizer/path/joinrels.c`: `try_partitionwise_join`,
>   `build_child_join_sjinfo`, `compute_partition_bounds`,
>   `get_matching_part_pairs`.
>
> Diagram: `diagrams/11_partition_pruning_plan_time.mermaid`.

## 1. Why this exists

Inheritance and partitioning add the concept of an **AppendRel**: a
parent RelOptInfo that represents the union of its children's rows.
The planner has to:

1. **Expand**: at the right point in the lifecycle, produce one child
   RelOptInfo per included child.
2. **Translate**: a Var referring to the parent must be rewritten to
   reference the child's column for plans built on children.
3. **Prune**: avoid scanning child relations whose range is excluded
   by query quals (compile-time pruning) or by parameter values
   (run-time pruning).
4. **Push down**: parent-level quals must be propagated to children
   (`apply_child_basequals`).
5. **Combine**: children's paths are unioned via `AppendPath` or
   `MergeAppendPath`.
6. **Partitionwise join**: when both sides are partitioned compatibly,
   per-partition joins followed by an Append are usually faster than
   a single big join.
7. **Partitionwise aggregate**: aggregates can sometimes be pushed
   below an Append.

---

## 2. Symbol table

| Symbol                                       | File:line                                     | Importance | Tier |
|----------------------------------------------|-----------------------------------------------|------------|------|
| `expand_inherited_rtentry`                   | `src/backend/optimizer/util/inherit.c`        | 0.65 | 2 |
| `expand_partitioned_rtentry`                 | `src/backend/optimizer/util/inherit.c`        | 0.55 | 2 |
| `expand_single_inheritance_child`            | `src/backend/optimizer/util/inherit.c`        | 0.45 | 3 |
| `apply_child_basequals`                      | `src/backend/optimizer/util/inherit.c`        | 0.50 | 2 |
| `make_append_rel_info`                       | `src/backend/optimizer/util/appendinfo.c`     | 0.50 | 2 |
| `adjust_appendrel_attrs`                     | `src/backend/optimizer/util/appendinfo.c`     | 0.55 | 2 |
| `find_appinfos_by_relids`                    | `src/backend/optimizer/util/appendinfo.c`     | 0.40 | 3 |
| `set_append_rel_size`                        | `src/backend/optimizer/path/allpaths.c`       | 0.55 | 2 |
| `set_append_rel_pathlist`                    | `src/backend/optimizer/path/allpaths.c:1232`  | 0.78 | 1 |
| `add_paths_to_append_rel`                    | `src/backend/optimizer/path/allpaths.c:1302`  | 0.78 | 1 |
| `generate_orderedappend_paths`               | `src/backend/optimizer/path/allpaths.c`       | 0.55 | 2 |
| `accumulate_append_subpath`                  | `src/backend/optimizer/path/allpaths.c`       | 0.45 | 3 |
| `get_cheapest_parameterized_child_path`      | `src/backend/optimizer/path/allpaths.c`       | 0.45 | 3 |
| `make_partition_pruneinfo`                   | `src/backend/partitioning/partprune.c`        | 0.55 | 2 |
| `gen_partprune_steps`                        | `src/backend/partitioning/partprune.c`        | 0.50 | 2 |
| `prune_append_rel_partitions`                | `src/backend/partitioning/partprune.c`        | 0.50 | 2 |
| `match_clause_to_partition_key`              | `src/backend/partitioning/partprune.c`        | 0.45 | 3 |
| `get_matching_partitions`                    | `src/backend/partitioning/partprune.c`        | 0.45 | 3 |
| `try_partitionwise_join`                     | `src/backend/optimizer/path/joinrels.c`       | 0.50 | 2 |
| `build_child_join_sjinfo`                    | `src/backend/optimizer/path/joinrels.c`       | 0.45 | 3 |
| `compute_partition_bounds`                   | `src/backend/optimizer/path/joinrels.c`       | 0.45 | 3 |
| `create_partitionwise_grouping_paths`        | `src/backend/optimizer/plan/planner.c`        | 0.45 | 3 |
| `AppendRelInfo`                              | `src/include/nodes/pathnodes.h:2959`          | 0.55 | 2 |

---

## 3. Lifecycle: where inheritance expansion happens

Inheritance/partition expansion is intentionally **delayed** until after
preprocessing / EC merging / qual distribution, so that:

- Pruning quals are visible per-child with full RestrictInfo information.
- The parent's `notnullattnums`, `pages`, `tuples` etc. are settled.
- ECs and pathkeys are known so child-rel pathkey adjustment works.

Concretely, in `query_planner`:

```c
/* Most setup is done first... */
/* then: */
add_other_rels_to_query(root);   /* this is the expansion step */
distribute_row_identity_vars(root);
/* then: */
final_rel = make_one_rel(root, joinlist);
```

`add_other_rels_to_query` (initsplan.c) calls `expand_inherited_rtentry`
for every parent RTE with `inh = true`.

---

## 4. `expand_inherited_rtentry` and `expand_partitioned_rtentry`

### 4.1 `expand_inherited_rtentry`
For a non-partitioned parent (regular inheritance):
1. Compute the children via `find_inheritance_children`.
2. For each child:
   - Build a child RTE_RELATION RTE.
   - Build a child RelOptInfo via `build_simple_rel(root, child_rti,
     parent_rel)`. `reloptkind = RELOPT_OTHER_MEMBER_REL`.
   - Build an `AppendRelInfo` linking parent ↔ child.
3. Call `apply_child_basequals` to propagate parent quals to children.

### 4.2 `expand_partitioned_rtentry`
For a partitioned parent:
1. Walk the partition descriptor (`PartitionDesc`) recursively (sub-partitioned
   tables produce intermediate "other partitioned rels").
2. Apply **partition pruning** at expansion time using
   `prune_append_rel_partitions` to skip whole subtrees that are
   definitively excluded.
3. Build `AppendRelInfo`s like the non-partitioned case, but the parent
   keeps its partitioning metadata (`part_scheme`, `boundinfo`,
   `partexprs`, `nullable_partexprs`) for later partitionwise-join
   matching.

### 4.3 `apply_child_basequals`
Translates each parent-level RestrictInfo through
`adjust_appendrel_attrs` so it refers to the child's columns, then
adds the translated RestrictInfo to the child's `baserestrictinfo`.

---

## 5. `AppendRelInfo`

```c
typedef struct AppendRelInfo {
    NodeTag    type;
    Index      parent_relid;
    Index      child_relid;
    Oid        parent_reltype;
    Oid        child_reltype;
    List      *translated_vars;     /* parent-attno -> child Var/Expr */
    int        num_child_cols;
    AttrNumber *parent_colnos;       /* child-attno -> parent attno */
    Oid        parent_reloid;
} AppendRelInfo;
```
Source: `src/include/nodes/pathnodes.h:2959`.

`translated_vars` enables forward translation: given a parent
`Var(varattno=k)`, look up `list_nth(translated_vars, k-1)` to get
the child expression. For inheritance the entries are simple Vars;
for UNION ALL pull-up they may be arbitrary expressions.

`adjust_appendrel_attrs(root, expr, naps, appinfos)` walks `expr`
and substitutes parent Vars accordingly. It's used for:
- Building child `baserestrictinfo` (parent qual → child qual).
- Translating parent `joininfo` references to children for
  partitionwise-join.
- Building child PathTargets.
- Parameterized path child-translation in `reparameterize_path_by_child`.

---

## 6. AppendRel path generation

### 6.1 `set_append_rel_size`
For a parent appendrel:
1. For each live child RelOptInfo, run `set_rel_size` (recurses).
2. Sum child `rows` to get parent rows.
3. Sum child widths weighted by row count for parent width.
4. `consider_parallel` becomes the AND of children's flags.

### 6.2 `set_append_rel_pathlist` (allpaths.c:1232)
For a parent appendrel:
1. For each live child, recurse `set_rel_pathlist`.
2. Call `add_paths_to_append_rel` with the surviving children.
3. Optionally generate ordered-append paths (`generate_orderedappend_paths`)
   if the children share a pathkey order — a `MergeAppendPath` can preserve
   it.

### 6.3 `add_paths_to_append_rel` (allpaths.c:1302)
Builds Append / MergeAppend / parallel-Append paths:

- **AppendPath (cheapest_total)**: pick each child's
  `cheapest_total_path` (or `cheapest_unique_path` etc.).
- **AppendPath (parameterized)**: for each distinct parameterization
  needed, pick `get_cheapest_parameterized_child_path` per child.
- **AppendPath (partial)**: parallel-aware Append. Children's
  `partial_pathlist` mixed with non-partial cheapest paths;
  partial children come first.
- **MergeAppendPath**: only if all children's pathkeys cover the
  required ordering. Built by `generate_orderedappend_paths`.

`accumulate_append_subpath` is a helper that flattens nested
AppendPaths whose subpaths can be merged into the outer Append's
subpath list, avoiding redundant Append nodes.

---

## 7. Partition pruning

### 7.1 Plan-time pruning
Done from inside `expand_partitioned_rtentry` via
`prune_append_rel_partitions`:
1. Walk the parent's `baserestrictinfo`.
2. For each clause, `match_clause_to_partition_key` decides whether
   the clause can be evaluated against the partition key (must be a
   stable expression of partition columns vs. constants).
3. `gen_partprune_steps` builds `PartitionPruneStep` nodes: AND/OR
   combinators and per-step operator information.
4. `get_matching_partitions` evaluates the steps using the constant
   sides; partitions whose bounds are excluded are dropped from
   `live_parts` and skipped during expansion.

### 7.2 Run-time pruning
When clauses are present that depend on `Param`s (parameter values
unknown at plan time, e.g. PreparedStatement parameters or nestloop
parameter sources), `make_partition_pruneinfo` builds a
`PartitionPruneInfo` that's attached to the Append/MergeAppend Plan
node. The executor evaluates the steps:
- At `InitPlan` time using initial Param values
  (`exec_init_partition_prune`).
- Per-rescan when nestloop parameter values change
  (`exec_partition_prune_subnodes`).

### 7.3 `PartitionPruneStep` shapes

| Step kind                | Meaning |
|--------------------------|---------|
| `T_PartitionPruneStepOp` | Apply one operator-clause to bounds. |
| `T_PartitionPruneStepCombine` | AND/OR of step results. |

The hierarchical step tree is generated once; the executor evaluates
it per partition pruning request.

---

## 8. Partitionwise join

Source: `src/backend/optimizer/path/joinrels.c`.

`try_partitionwise_join(root, joinrel, ...)` runs at each join level
when `enable_partitionwise_join = true`. It checks:
- Both inputs are partitioned, with `part_scheme` matching (same
  partition strategy, partition keys, opclasses).
- The join clauses include equality on the partition keys (so each
  child partition has at most one matching child on the other side).
- The partition bounds are mergeable
  (`partition_bounds_can_match` and `compute_partition_bounds`).

If matched, `get_matching_part_pairs` lists the (left-child,
right-child) pairs to join. For each pair:
1. `build_child_join_sjinfo` produces a child-level SpecialJoinInfo.
2. Recursively call `add_paths_to_joinrel` for the per-partition join.
3. Append the children up via an AppendPath built on the joinrel.

This adds a path that is sometimes much cheaper than a single
big-table join (especially with hash-partitioning that aligns
join keys).

---

## 9. Partitionwise aggregate

`enable_partitionwise_aggregate` allows partial aggregates to be
pushed below an Append:
- `create_partitionwise_grouping_paths` (planner.c) is called from
  `create_grouping_paths` when applicable.
- Each partition's local aggregate path produces partial groups; the
  Append is on top, then a final aggregate combines partial results.
- For full pushdown (when the GROUP BY is the partition key), no
  combine step is needed — each partition's groups are already final.

---

## 10. PlannerGlobal interactions

`glob->appendRelations` accumulates all AppendRelInfo records in
plan order. `set_plan_references` uses this to fix up Vars that
reference parent rels (replacing them with child Vars at
plan-finalization time when the chosen path is on a child rel).

---

## 11. Performance characteristics

- `expand_inherited_rtentry`: O(children) per parent.
- `apply_child_basequals`: O(children × parent_quals).
- `gen_partprune_steps`: O(quals × partition_keys).
- `prune_append_rel_partitions`: O(partitions × steps).
- `try_partitionwise_join`: O(partitions × per-pair join planning) — can
  be expensive if both sides have many partitions.

---

## 12. GUC summary

| GUC | Effect |
|-----|--------|
| `enable_partitionwise_join` (off by default) | Allow partition-wise joins. |
| `enable_partitionwise_aggregate` (off) | Push partial aggregates below Append. |
| `enable_partition_pruning` (on) | Master switch for both plan-time and run-time pruning. |
| `enable_parallel_append` (on) | Parallel-aware AppendPath. |
| `enable_async_append` (on) | Asynchronous-execution Append (for FDW children mostly). |
| `constraint_exclusion` ('partition') | For non-partition inheritance: enable CHECK-constraint exclusion. |

---

## 13. Cross-references

- Where AppendRelInfo originates for UNION ALL pull-up:
  `component_preprocessing.md` (flatten_simple_union_all)
- Base-rel paths that become children:
  `component_base_relation_paths.md`
- Parameterized child-path translation
  (`reparameterize_path_by_child`):
  `component_join_paths_and_search.md`
- Partition-aware EC inference:
  `component_equivalence_classes_and_pathkeys.md`
- Diagram: `diagrams/11_partition_pruning_plan_time.mermaid`.
