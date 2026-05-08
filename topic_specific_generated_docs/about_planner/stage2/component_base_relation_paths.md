# Component: Base-Relation Path Generation

> Stage 2 documentation for **BASE_PATH_GENERATION**. Source:
> `src/backend/optimizer/path/allpaths.c` and the per-rtekind helpers it
> dispatches to. Path-creation primitives are in
> `src/backend/optimizer/util/pathnode.c`.

## 1. Why this exists

After the jointree is decomposed and base RelOptInfos exist, the planner must
generate **all interesting access paths** for each base relation. "Interesting"
means: any path whose (cost, sort order, parameterization, parallel-safety,
row-count) tuple is not dominated by another path. `set_cheapest` then picks
the survivors.

The "base" layer hides eight different RTE kinds under one dispatch
(`set_rel_pathlist`) plus the appendrel handler (covered in
`component_inheritance_and_partitioning.md`).

---

## 2. Symbol table

| Symbol                                | File:line                                       | Importance | Tier |
|---------------------------------------|-------------------------------------------------|------------|------|
| `make_one_rel`                        | `src/backend/optimizer/path/allpaths.c:171`     | 0.92 | 1 |
| `set_base_rel_pathlists`              | `src/backend/optimizer/path/allpaths.c:333`     | 0.86 | 1 |
| `set_base_rel_sizes`                  | `src/backend/optimizer/path/allpaths.c:290`     | 0.78 | 1 |
| `set_rel_pathlist`                    | `src/backend/optimizer/path/allpaths.c:469`     | 0.88 | 1 |
| `set_plain_rel_pathlist`              | `src/backend/optimizer/path/allpaths.c:764`     | 0.84 | 1 |
| `set_subquery_pathlist`               | `src/backend/optimizer/path/allpaths.c`         | 0.65 | 2 |
| `set_function_pathlist`               | `src/backend/optimizer/path/allpaths.c`         | 0.50 | 2 |
| `set_values_pathlist`                 | `src/backend/optimizer/path/allpaths.c`         | 0.45 | 3 |
| `set_cte_pathlist`                    | `src/backend/optimizer/path/allpaths.c`         | 0.45 | 3 |
| `set_namedtuplestore_pathlist`        | `src/backend/optimizer/path/allpaths.c`         | 0.35 | 3 |
| `set_worktable_pathlist`              | `src/backend/optimizer/path/allpaths.c`         | 0.45 | 3 |
| `set_result_pathlist`                 | `src/backend/optimizer/path/allpaths.c`         | 0.40 | 3 |
| `set_foreign_pathlist`                | `src/backend/optimizer/path/allpaths.c`         | 0.55 | 2 |
| `set_tablesample_rel_pathlist`        | `src/backend/optimizer/path/allpaths.c`         | 0.40 | 3 |
| `set_tablefunc_pathlist`              | `src/backend/optimizer/path/allpaths.c`         | 0.35 | 3 |
| `create_seqscan_path`                 | `src/backend/optimizer/util/pathnode.c:927`     | 0.78 | 1 |
| `create_samplescan_path`              | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |
| `create_subqueryscan_path`            | `src/backend/optimizer/util/pathnode.c`         | 0.55 | 2 |
| `create_functionscan_path`            | `src/backend/optimizer/util/pathnode.c`         | 0.50 | 2 |
| `create_valuesscan_path`              | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |
| `create_ctescan_path`                 | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |
| `create_namedtuplestorescan_path`     | `src/backend/optimizer/util/pathnode.c`         | 0.35 | 3 |
| `create_resultscan_path`              | `src/backend/optimizer/util/pathnode.c`         | 0.40 | 3 |
| `create_worktablescan_path`           | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |
| `create_tidscan_path`                 | `src/backend/optimizer/util/pathnode.c`         | 0.45 | 3 |

---

## 3. `make_one_rel` — the entry point

### 3.1 Signature
```c
RelOptInfo *make_one_rel(PlannerInfo *root, List *joinlist);
```
Source: `src/backend/optimizer/path/allpaths.c:171`.

### 3.2 Body (annotated)

```c
RelOptInfo *
make_one_rel(PlannerInfo *root, List *joinlist)
{
    RelOptInfo *rel;
    Index       rti;
    double      total_pages;

    /* (1) Mark base rels for fast-start consideration */
    set_base_rel_consider_startup(root);

    /* (2) Compute size estimates and consider_parallel flags */
    set_base_rel_sizes(root);

    /* (3) Sum total_table_pages for pro-rated I/O cost calculations */
    total_pages = 0;
    for (rti = 1; rti < root->simple_rel_array_size; rti++)
    {
        RelOptInfo *brel = root->simple_rel_array[rti];
        if (brel == NULL || IS_DUMMY_REL(brel)) continue;
        if (IS_SIMPLE_REL(brel))
            total_pages += (double) brel->pages;
    }
    root->total_table_pages = total_pages;

    /* (4) Generate access paths for each base rel */
    set_base_rel_pathlists(root);

    /* (5) Generate access paths for the entire join tree */
    rel = make_rel_from_joinlist(root, joinlist);

    Assert(bms_equal(rel->relids, root->all_query_rels));
    return rel;
}
```

### 3.3 Why three passes (sizes, pathlists, joinlist)?
- **Sizes first**: parameterized-path cost calculation needs row estimates
  for the OUTER side of a candidate nestloop. So every rel's `rows` must be
  known before any other rel's path generation.
- **`consider_parallel` first**: appendrel parents need this propagated
  before we start building paths for them.
- **`total_table_pages`**: used by `effective_cache_size`-based logic in
  `cost_index` to pro-rate fraction of cache likely allocated to this rel.

---

## 4. `set_base_rel_sizes` and `set_base_rel_pathlists`

### 4.1 `set_base_rel_sizes` (allpaths.c:290)
For each base rel:
1. If parallelism allowed: `set_rel_consider_parallel`.
2. `set_rel_size` (allpaths.c, dispatched per rtekind).

`set_rel_size` populates `rel->rows`, `rel->reltarget->width`, and
`rel->reltarget->cost`. For RTE_SUBQUERY it also runs the subquery
through `subquery_planner` (so its `subroot` is fully planned) and
records the cheapest subpath.

### 4.2 `set_base_rel_pathlists` (allpaths.c:333)
For each base rel: `set_rel_pathlist`. Identical structure to
`set_base_rel_sizes` but generates paths.

---

## 5. `set_rel_pathlist` — the dispatcher

Source: `src/backend/optimizer/path/allpaths.c:469`.

```c
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
                 Index rti, RangeTblEntry *rte)
{
    if (IS_DUMMY_REL(rel))
        ;                                  /* nothing */
    else if (rte->inh)
        set_append_rel_pathlist(...);      /* inheritance / partition */
    else
    {
        switch (rel->rtekind) {
            case RTE_RELATION:
                if (rte->relkind == RELKIND_FOREIGN_TABLE)
                    set_foreign_pathlist(root, rel, rte);
                else if (rte->tablesample != NULL)
                    set_tablesample_rel_pathlist(root, rel, rte);
                else
                    set_plain_rel_pathlist(root, rel, rte);
                break;
            case RTE_SUBQUERY:        break;       /* done in set_rel_size */
            case RTE_FUNCTION:        set_function_pathlist(...); break;
            case RTE_TABLEFUNC:       set_tablefunc_pathlist(...); break;
            case RTE_VALUES:          set_values_pathlist(...); break;
            case RTE_CTE:             break;       /* done in set_rel_size */
            case RTE_NAMEDTUPLESTORE: break;       /* done in set_rel_size */
            case RTE_RESULT:          break;       /* done in set_rel_size */
        }
    }

    /* Plugin hook (extensions add custom paths here) */
    if (set_rel_pathlist_hook)
        (*set_rel_pathlist_hook)(root, rel, rti, rte);

    /* Gather over partial paths -- skipping inheritance children
       and the topmost scan/join rel (handled in grouping_planner) */
    if (rel->reloptkind == RELOPT_BASEREL &&
        !bms_equal(rel->relids, root->all_query_rels))
        generate_useful_gather_paths(root, rel, false);

    set_cheapest(rel);
}
```

### 5.1 The "done in set_rel_size" cases
For RTE_SUBQUERY, RTE_CTE, RTE_NAMEDTUPLESTORE and RTE_RESULT, both
size estimation and path generation happen together inside the
size-pass dispatcher (at `set_rel_size`/`set_subquery_pathlist` etc.).
By the time `set_rel_pathlist` is reached, the rel already has a
populated `pathlist`, so there's nothing more to do except the hook +
gather + set_cheapest tail.

### 5.2 Plugin extensibility
`set_rel_pathlist_hook` runs **before** Gather generation, so plugins
that emit partial paths can have them gathered automatically. See
`component_hooks_and_extensibility.md`.

---

## 6. `set_plain_rel_pathlist`

Source: `src/backend/optimizer/path/allpaths.c:764`.

The most common path. Steps:

1. **`required_outer = rel->lateral_relids`** — parameterization required
   purely by lateral references.
2. **Sequential scan**: `add_path(rel, create_seqscan_path(root, rel,
   required_outer, 0))`. This is the unconditional baseline path.
3. **Parallel sequential scan**: if `rel->consider_parallel && bms_is_empty(required_outer)`:
   `create_plain_partial_paths(root, rel)` adds a parallel SeqScan to
   `partial_pathlist`.
4. **TID scan**: `create_tidscan_paths(root, rel)` (tidpath.c) — generates a
   `TidPath` if any qual matches `CTID = expr` or `CTID = ANY(array)` or a
   range like `CTID < something`.
5. **Index scans**: `create_index_paths(root, rel)` (indxpath.c) — see
   `component_index_paths.md`. This is the heavy lifting; produces
   `IndexPath`, `BitmapHeapPath`, `BitmapAndPath`, `BitmapOrPath`.
6. (Note: parameterized index paths can be emitted from `create_index_paths`
   too, with `param_info` for outer rel(s).)

### 6.1 `create_seqscan_path`
Source: `src/backend/optimizer/util/pathnode.c:927`.

Signature:
```c
Path *create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer, int parallel_workers);
```

- Allocates a plain `Path` with `pathtype = T_SeqScan`.
- Calls `cost_seqscan(path, root, rel, path->param_info)` to fill
  `path->rows`, `startup_cost`, `total_cost`.
- `pathkeys = NIL` (seqscan is unsorted).
- Sets `param_info` via `get_baserel_parampathinfo` if `required_outer`
  is non-empty.
- `parallel_aware = (parallel_workers > 0)`.

---

## 7. RTE-specific path constructors

### 7.1 `set_subquery_pathlist`
Plans the subquery via `subquery_planner` (recursively!). Then for each
surviving path of the subquery's `UPPERREL_FINAL` rel, wraps it in a
`SubqueryScanPath` via `create_subqueryscan_path`. Quals from the parent
that can be safely **pushed down** into the subquery are moved
*before* recursing — see `subquery_is_pushdown_safe` and
`qual_is_pushdown_safe` in `allpaths.c`. Pushdown is gated by:
- No LIMIT in subquery (we'd discard rows we shouldn't).
- No EXCEPT/EXCEPT ALL.
- Volatile quals can't be pushed into DISTINCT, window-function or
  set-returning-function subqueries.
- Grouping sets disallow pushdown entirely.

### 7.2 `set_function_pathlist`
Builds a single `FunctionScanPath` (`create_functionscan_path`). For
multiple `ROWS FROM (...)` functions, the row estimate becomes the max
across them. Cost via `cost_functionscan` in `costsize.c`.

### 7.3 `set_values_pathlist`
Single `Path` of pathtype `T_ValuesScan` whose row count is
`list_length(rte->values_lists)`. Trivial cost from `cost_valuesscan`.

### 7.4 `set_cte_pathlist`
Looks up the precomputed CTE plan via `root->cte_plan_ids`. Builds a
`Path` of pathtype `T_CteScan`. Row estimate inherited from the CTE
subplan's `plan_rows`.

### 7.5 `set_worktable_pathlist`
Recursive-CTE inner reference. Produces a `Path` of pathtype
`T_WorkTableScan` parameterized by the recursive-CTE param id
(`root->wt_param_id`). Used inside the recursive arm.

### 7.6 `set_namedtuplestore_pathlist`
For ENRs (Ephemeral Named Relations), emit a single
`NamedTuplestoreScan` path.

### 7.7 `set_result_pathlist`
RTE_RESULT: emit a `Result`/`GroupResultPath` with no scan; just a
constant-eval node. Used when `replace_empty_jointree` injected an
empty FROM.

### 7.8 `set_foreign_pathlist`
Defers to `rel->fdwroutine->GetForeignPaths(root, rel, rte->relid)`.
The FDW is responsible for emitting one or more `ForeignPath` nodes
via `create_foreignscan_path`.

### 7.9 `set_tablesample_rel_pathlist`
Builds a `SampleScan` Path via `create_samplescan_path`. Cost is
proportional to the sample fraction; parallel workers allowed only if
the tablesample method is parallel-safe.

### 7.10 `set_tablefunc_pathlist`
For `RTE_TABLEFUNC` (`XMLTABLE`, `JSON_TABLE`): produces a
`TableFuncScan` path via `create_tablefuncscan_path`.

---

## 8. Generating partial paths and gather paths

Two helpers in `allpaths.c` are central:
- `create_plain_partial_paths(root, rel)` — for plain rels, computes
  `parallel_workers = compute_parallel_worker(rel, rel->pages, -1,
  max_parallel_workers_per_gather)` and emits a parallel `SeqScan`
  partial path.
- `create_partial_bitmap_paths(root, rel, bitmapqual)` — for parallel
  bitmap heap scans (called from indxpath.c).

After `set_rel_pathlist`'s tail, **`generate_useful_gather_paths`**
(allpaths.c) wraps `partial_pathlist` entries into `GatherPath` /
`GatherMergePath` candidates. Skipped for inheritance children (so the
parent appendrel can manage its own gathers more efficiently) and for
the topmost scan/join rel (handled later in `grouping_planner` once the
final tlist is known).

---

## 9. `set_cheapest` semantics (recap)

After all paths are added, `set_cheapest(rel)` populates four things:
- `cheapest_startup_path` — best `startup_cost` over unparameterized paths
  (only if `consider_startup`).
- `cheapest_total_path` — best `total_cost` over unparameterized paths.
- `cheapest_unique_path` — `NULL` here; computed lazily by
  `create_unique_path` if anyone asks.
- `cheapest_parameterized_paths` — list of cheapest paths per minimum
  parameterization. Built by walking `pathlist` and grouping by
  `PATH_REQ_OUTER`.

Source: `src/backend/optimizer/util/pathnode.c:242`.

Crucial detail: parameterized paths only appear in
`cheapest_parameterized_paths` (and *not* in `cheapest_total_path` unless
no unparameterized path exists). This lets the join planner pick a
parameterized inner path for nestloop without polluting other join
methods that need an unparameterized inner.

---

## 10. Performance characteristics

- `set_base_rel_sizes`: O(N) where N is base rel count. Per-rel work
  dominated by `get_relation_info` (catalog lookup, stats fetch) for
  RELATION rtekind.
- `set_base_rel_pathlists`: dominated by `create_index_paths` for
  many-indexed rels. See `component_index_paths.md`.
- Subquery / CTE rels: O(plan-size of inner). Each `set_subquery_pathlist`
  is a full `subquery_planner` recursion.

---

## 11. Cross-references

- Index paths (called from `set_plain_rel_pathlist`):
  `component_index_paths.md`
- Cost details for these paths: `component_cost_model_and_selectivity.md`
- `create_unique_path` and `create_material_path` are usually invoked from
  the join layer; see `component_join_paths_and_search.md`.
- AppendRel and partition handling:
  `component_inheritance_and_partitioning.md`
- Diagram: `diagrams/01_planner_pipeline.mermaid` (MOR subgraph),
  `diagrams/05_path_to_plan_map.mermaid`.
