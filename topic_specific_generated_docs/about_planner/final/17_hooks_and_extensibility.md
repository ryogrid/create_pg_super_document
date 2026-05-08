# 17. Hooks and Extensibility

Prerequisites: [04 Lifecycle and entry points](04_lifecycle_and_entry_points.md), [08 Base relation paths](08_base_relation_paths.md), [09 Join paths and search](09_join_paths_and_search.md), [16 Plan creation and setrefs](16_plan_creation_and_setrefs.md).

The PostgreSQL planner is a high-stakes piece of code. Rather than have extensions monkey-patch internal logic, the planner exposes a collection of well-defined function-pointer hooks. Plugins **wrap** the standard behavior (call the inner standard function, then mutate the result) or **replace** it entirely.

This module documents every planner hook the core code calls, plus the FDW and CustomScan APIs that build on those hooks. It is the reference for anyone writing a planner-touching extension or trying to reverse-engineer one.

Sources of declarations:
- `src/backend/optimizer/plan/planner.c` — `planner_hook`, `create_upper_paths_hook`.
- `src/backend/optimizer/path/allpaths.c` — `set_rel_pathlist_hook`, `join_search_hook`.
- `src/backend/optimizer/path/joinpath.c` — `set_join_pathlist_hook`.
- `src/backend/optimizer/util/plancat.c` — `get_relation_info_hook`.
- `src/backend/utils/adt/selfuncs.c` — `get_index_stats_hook`, `get_attavgwidth_hook`, `get_relation_stats_hook`.
- FDW API: `src/include/foreign/fdwapi.h`.
- Custom scan API: `src/include/nodes/extensible.h`.

## 17.1 Three categories

The complete set of planner hooks falls into three categories:

1. **Lifecycle hooks** — `planner_hook`, `create_upper_paths_hook`.
2. **Path-modification hooks** — `set_rel_pathlist_hook`, `set_join_pathlist_hook`, `join_search_hook`.
3. **Catalog and statistics hooks** — `get_relation_info_hook`, `get_index_stats_hook`, `get_attavgwidth_hook`, `get_relation_stats_hook`.

Plus the **FDW API** and **CustomScan API** for first-class extension of access methods.

## 17.2 Symbol table

| Hook                          | Site (file:line)                                | Purpose |
|-------------------------------|-------------------------------------------------|---------|
| `planner_hook`                | `src/backend/optimizer/plan/planner.c:280`      | Replace the entire planner. |
| `create_upper_paths_hook`     | `src/backend/optimizer/plan/planner.c` (multiple sites) | Add custom paths to upper rels (group/window/distinct/etc.). |
| `set_rel_pathlist_hook`       | `src/backend/optimizer/path/allpaths.c:538`     | Edit a base rel's pathlist (called per-rel after standard logic). |
| `set_join_pathlist_hook`      | `src/backend/optimizer/path/joinpath.c:342`     | Edit a join rel's pathlist (after standard methods tried). |
| `join_search_hook`            | `src/backend/optimizer/path/allpaths.c:3372`    | Replace `standard_join_search` / GEQO. |
| `get_relation_info_hook`      | `src/backend/optimizer/util/plancat.c:575`      | Editorialize on relation info (indexes, stats) at base-rel build. |
| `get_index_stats_hook`        | `src/backend/utils/adt/selfuncs.c`              | Override pg_statistic for index columns. |
| `get_attavgwidth_hook`        | `src/backend/utils/adt/selfuncs.c`              | Override average-width estimates. |
| `get_relation_stats_hook`     | `src/backend/utils/adt/selfuncs.c`              | Override per-rel statistics. |

FDW-specific entry points (called from path/plan code, not hooks per se):

| FDW callback                       | Site                                           | Purpose |
|------------------------------------|------------------------------------------------|---------|
| `GetForeignRelSize`                | `set_foreign_pathlist` size pass | Estimate size. |
| `GetForeignPaths`                  | `set_foreign_pathlist` pathing pass | Emit `ForeignPath` for the rel. |
| `GetForeignJoinPaths`              | `add_paths_to_joinrel` (joinpath.c:330) | Push joins to remote. |
| `GetForeignUpperPaths`             | each `create_<stage>_paths` | Push upper rels to remote. |
| `GetForeignPlan`                   | `create_foreignscan_plan` | Build the executor node. |
| `IsForeignScanParallelSafe`        | `set_rel_consider_parallel` | FDW-controlled parallel safety. |
| `ReparameterizeForeignPathByChild` | `reparameterize_path_by_child` | Per-child param translation. |

CustomScan provider hooks:

| Function                          | Purpose |
|-----------------------------------|---------|
| `RegisterCustomScanMethods`       | Register at module load. |
| `set_rel_pathlist_hook`           | Provider injects CustomPath at base-rel level. |
| `set_join_pathlist_hook`          | Provider injects join-level CustomPath. |
| `PlanCustomPath` (CustomPathMethods) | Convert CustomPath → CustomScan. |
| `BeginCustomScan`, `ExecCustomScan`, ... | Executor side. |

## 17.3 `planner_hook`

### 17.3.1 Declaration

```c
typedef PlannedStmt *(*planner_hook_type)(Query *parse,
                                           const char *query_string,
                                           int cursorOptions,
                                           ParamListInfo boundParams);
extern PGDLLIMPORT planner_hook_type planner_hook;
```

### 17.3.2 Where it is called

```c
PlannedStmt *
planner(Query *parse, const char *qs, int cursorOptions, ParamListInfo bp)
{
    if (planner_hook)
        result = (*planner_hook)(parse, qs, cursorOptions, bp);
    else
        result = standard_planner(parse, qs, cursorOptions, bp);
    return result;
}
```

`src/backend/optimizer/plan/planner.c:280-284`.

### 17.3.3 Use cases

- **`pg_hint_plan`**: parses comments at the top of the query for hints (Leading, NestLoop, Set, etc.), tweaks GUCs, calls `standard_planner`, then restores GUCs. Citus uses similar machinery to dispatch sharded queries. Timescaledb reroutes hypertables.
- **`auto_explain`**: trampolines `planner_hook` to log slow plans (it does not actually need a planner hook, but combines with `ExecutorEnd_hook`).
- **Plan caching layers**: cache planned statements keyed by query fingerprint.

### 17.3.4 Idiom

```c
static planner_hook_type prev_planner_hook = NULL;

void _PG_init(void) {
    prev_planner_hook = planner_hook;
    planner_hook = my_planner_hook;
}

static PlannedStmt *
my_planner_hook(Query *parse, const char *qs, int co, ParamListInfo bp)
{
    PlannedStmt *result;
    /* Pre-processing: e.g. parse hint comments, set GUCs */
    if (prev_planner_hook)
        result = prev_planner_hook(parse, qs, co, bp);
    else
        result = standard_planner(parse, qs, co, bp);
    /* Post-processing: e.g. log */
    return result;
}
```

## 17.4 `create_upper_paths_hook`

### 17.4.1 Declaration

```c
typedef void (*create_upper_paths_hook_type)(PlannerInfo *root,
                                              UpperRelationKind stage,
                                              RelOptInfo *input_rel,
                                              RelOptInfo *output_rel,
                                              void *extra);
extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;
```

### 17.4.2 Where it is called

After each standard upper-stage path generation in `planner.c`:

- `create_grouping_paths` → `UPPERREL_GROUP_AGG` (around line 4198).
- `create_window_paths` → `UPPERREL_WINDOW` (around line 4638).
- `create_distinct_paths` → `UPPERREL_DISTINCT` (around line 4881).
- `create_partial_distinct_paths` → `UPPERREL_PARTIAL_DISTINCT` (around line 5072).
- `create_ordered_paths` → `UPPERREL_ORDERED` (around line 5479).
- The final stage → `UPPERREL_FINAL` (around line 2063).

The `stage` parameter discriminates which upper-rel kind triggered the call; extensions select behavior based on it.

### 17.4.3 Used by

FDW upper-relation push-down: `postgres_fdw` uses `GetForeignUpperPaths` (which is itself called from `create_grouping_paths` etc.) to push aggregates down to a remote PG server. Citus uses this to push aggregates to shards.

## 17.5 `set_rel_pathlist_hook`

### 17.5.1 Declaration

```c
typedef void (*set_rel_pathlist_hook_type)(PlannerInfo *root,
                                            RelOptInfo *rel,
                                            Index rti,
                                            RangeTblEntry *rte);
extern PGDLLIMPORT set_rel_pathlist_hook_type set_rel_pathlist_hook;
```

### 17.5.2 Where it is called

At the bottom of `set_rel_pathlist` (`src/backend/optimizer/path/allpaths.c:538`), **after** the standard logic adds paths and **before** Gather generation + `set_cheapest`. This positioning matters: extensions can add partial paths that the subsequent Gather generation will pick up automatically.

### 17.5.3 Use cases

- **CustomScan providers** add `CustomPath` (e.g. cstore_fdw, citus, in-memory column stores).
- **Plan rewriters** strip or replace certain paths.
- Test harnesses that force specific access methods.

## 17.6 `set_join_pathlist_hook`

### 17.6.1 Declaration

```c
typedef void (*set_join_pathlist_hook_type)(PlannerInfo *root,
                                             RelOptInfo *joinrel,
                                             RelOptInfo *outerrel,
                                             RelOptInfo *innerrel,
                                             JoinType jointype,
                                             JoinPathExtraData *extra);
extern PGDLLIMPORT set_join_pathlist_hook_type set_join_pathlist_hook;
```

### 17.6.2 Where it is called

At the end of `add_paths_to_joinrel` (`src/backend/optimizer/path/joinpath.c:342`), after FDW join push-down. Similar to `set_rel_pathlist_hook`, this gives extensions a final say on the joinrel's pathlist.

### 17.6.3 Use cases

- CustomPath join providers.
- Cost-tuning experiments.
- Pruning specific join methods that an extension knows are inappropriate.

## 17.7 `join_search_hook`

### 17.7.1 Declaration

```c
typedef RelOptInfo *(*join_search_hook_type)(PlannerInfo *root,
                                              int levels_needed,
                                              List *initial_rels);
extern PGDLLIMPORT join_search_hook_type join_search_hook;
```

### 17.7.2 Where it is called

```c
if (join_search_hook)
    return (*join_search_hook)(root, levels_needed, initial_rels);
else if (enable_geqo && levels_needed >= geqo_threshold)
    return geqo(root, levels_needed, initial_rels);
else
    return standard_join_search(root, levels_needed, initial_rels);
```

`src/backend/optimizer/path/allpaths.c:3372-3377`.

The hook **completely replaces** the join-search algorithm. The return must be a RelOptInfo containing the final joined result. If you only want to influence the search, override `set_rel_pathlist_hook` and/or `set_join_pathlist_hook` instead.

### 17.7.3 Use cases

- Research: alternative search algorithms (simulated annealing, query rewrite + cached plan space).
- Hint-driven join order forcing (some implementations of pg_hint_plan use this rather than tweaking standard_join_search).

## 17.8 Catalog and statistics hooks

### 17.8.1 `get_relation_info_hook`

```c
typedef void (*get_relation_info_hook_type)(PlannerInfo *root,
                                             Oid relationObjectId,
                                             bool inhparent,
                                             RelOptInfo *rel);
extern PGDLLIMPORT get_relation_info_hook_type get_relation_info_hook;
```

Called from `get_relation_info` (`src/backend/optimizer/util/plancat.c:575`) after the standard relation info is filled in. Lets extensions adjust `rel->indexlist`, `rel->statlist`, `rel->pages`, etc. The classic use: hypopg installs hypothetical index entries here.

### 17.8.2 `get_relation_stats_hook` and `get_index_stats_hook`

Lets extensions provide custom statistics. Used by:

- AQO (Adaptive Query Optimization extension) for learned cardinalities.
- hypopg for hypothetical index cost estimates.

### 17.8.3 `get_attavgwidth_hook`

Override average-width estimates per `(relid, attno)`. Less commonly used; mainly relevant when computing tuple width for sort operations.

## 17.9 FDW API integration points

The FDW API (`src/include/foreign/fdwapi.h`) provides a `FdwRoutine` struct full of callbacks. Per-rel and per-join, the optimizer calls FDW callbacks at specific lifecycle points:

| Callback                     | Caller                                            |
|------------------------------|---------------------------------------------------|
| `GetForeignRelSize`          | `set_foreign_pathlist` size pass.                 |
| `GetForeignPaths`            | `set_foreign_pathlist` pathing pass.              |
| `GetForeignJoinPaths`        | `add_paths_to_joinrel` (joinpath.c:330).          |
| `GetForeignUpperPaths`       | each `create_<stage>_paths` (planner.c).          |
| `GetForeignPlan`             | `create_foreignscan_plan` (createplan.c).         |
| `IsForeignScanParallelSafe`  | `set_rel_consider_parallel`.                      |
| `ReparameterizeForeignPathByChild` | `reparameterize_path_by_child`.             |

The FDW returns `ForeignPath` nodes via `create_foreignscan_path` / `create_foreign_join_path` / `create_foreign_upper_path`. Cost is provided directly by the FDW (it knows the remote system best).

## 17.10 CustomScan API

`CustomPath` is a Path subtype intended for extension-provided access methods that do not fit FDW's "external server" model (in-memory column stores, GPU-resident scans, etc.). It carries:

- `flags` describing capabilities.
- `custom_paths` — arbitrary subpaths the provider may want.
- `custom_private` — any data the provider wants.
- `methods` — a `CustomPathMethods` pointer with:
  - `PlanCustomPath(root, rel, best_path, tlist, clauses)` — return a `CustomScan` Plan node.
  - `ReparameterizeCustomPathByChild` (optional).

Providers register their `CustomScanMethods` at module load via `RegisterCustomScanMethods`.

## 17.11 Hooks in the wild

### 17.11.1 `pg_hint_plan` worked example

Sketch of how a hint extension typically uses these hooks:

1. `_PG_init`: install `planner_hook`, `set_rel_pathlist_hook`, `set_join_pathlist_hook`, `join_search_hook`.
2. In `planner_hook`: parse `/*+ ... */` from `query_string`, store the hint table in TLS.
3. In `set_rel_pathlist_hook`: if a Scan hint exists for this rel, prune the rel's pathlist down to the hinted method.
4. In `set_join_pathlist_hook`: if a Join method hint exists for this pair, prune accordingly.
5. In `join_search_hook`: if a Leading hint exists, force the join order by manually building joinrels in the requested order (calling `make_join_rel` directly).
6. Post-`standard_planner`: clear TLS hint table.

See [Module 20.15](20_deep_dives.md#2015-planner-hooks-in-the-wild-pg_hint_plan-citus-timescaledb) for a more detailed comparison of pg_hint_plan, citus, and timescaledb.

### 17.11.2 `auto_explain`

Uses `ExecutorEnd_hook` (not a planner hook strictly), but often paired with `planner_hook` to time planning. Logs the EXPLAIN of slow queries automatically.

### 17.11.3 `postgres_fdw`

Implements every FDW callback. Uses `GetForeignJoinPaths` to push joins to the remote, `GetForeignUpperPaths` to push aggregates, and `PlanDirectModify` (via the `IsForeignRelUpdatable` callback) to push UPDATE/DELETE to the remote.

## 17.12 Hook safety guidelines

- **Always preserve the previous hook value** and chain to it (so multiple modules can coexist).
- **Never call `palloc`** in `_PG_init` — the memory context is not set up yet.
- **Be parallel-aware**: the planner runs only in the leader, but hooks may be called for sub-plans that will be parallelized.
- **Do not modify Query inside hooks unless you understand the ownership rules** — the original Query may be cached by upstream (PreparedStatements) and modifications break re-planning.
- **Test with `force_parallel_mode = regress`** to catch hooks that insert parallel-unsafe expressions silently.
- **Honor existing GUCs** like `enable_*` rather than building new ones for the same purpose.
- **Document** the hook chain order in your README so users know how multiple extensions interact.

## 17.13 Cross-references

- Lifecycle / where each hook fits: [04 Lifecycle and entry points](04_lifecycle_and_entry_points.md).
- Per-stage upper-rel hooks (where `create_upper_paths_hook` fires): [04 Lifecycle and entry points](04_lifecycle_and_entry_points.md) (`grouping_planner`).
- Path generation (where `set_rel_pathlist_hook` and `set_join_pathlist_hook` fire): [08 Base relation paths](08_base_relation_paths.md), [09 Join paths and search](09_join_paths_and_search.md).
- FDW push-down semantics also discussed in: [09 Join paths and search](09_join_paths_and_search.md) (`add_paths_to_joinrel` step 5).
- Plan finalization (`set_plan_references`): the path provided by CustomPath becomes a CustomScan plan via `PlanCustomPath`; see [16 Plan creation and setrefs](16_plan_creation_and_setrefs.md).
- ForeignPath / CustomPath in the path catalog: [18 ForeignPath](18_path_catalog.md#foreignpath-t_foreignpath), [18 CustomPath](18_path_catalog.md#custompath-t_custompath).
- Plan creators: [19 create_foreignscan_plan](19_plan_creator_catalog.md#create_foreignscan_plan), [19 create_customscan_plan](19_plan_creator_catalog.md#create_customscan_plan).
- Real-world hook examples: [Module 20.15](20_deep_dives.md#2015-planner-hooks-in-the-wild-pg_hint_plan-citus-timescaledb).

Next: [18 Path catalog](18_path_catalog.md).
