# Appendix: Planner Data Structures

This appendix collects the canonical struct definitions the planner
operates on. Each `typedef` is quoted directly from the current
`src/include/nodes/pathnodes.h` (the line numbers reflect HEAD at the
time of writing) and is followed by short field-by-field commentary.
The intent is that you can land here from any other module document and
get an authoritative description of the layout without chasing
multi-thousand-line headers.

Where struct definitions are very long (e.g., `PlannerInfo`,
`RelOptInfo`, `RestrictInfo`), this appendix shows the type signature,
opening/closing lines, and field clusters with commentary, with
`pathnodes.h` line numbers so you can read the full body alongside.

> Source-of-truth: `src/include/nodes/pathnodes.h` (and one struct from
> `src/include/nodes/plannodes.h`). Cross-link targets use the modules
> in `final/`.

---

## PlannerGlobal

`src/include/nodes/pathnodes.h:95`

```c
typedef struct PlannerGlobal
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag        type;

    /* Param values provided to planner() */
    ParamListInfo  boundParams pg_node_attr(read_write_ignore);

    /* Plans for SubPlan nodes */
    List          *subplans;

    /* Paths from which the SubPlan Plans were made */
    List          *subpaths;

    /* PlannerInfos for SubPlan nodes */
    List          *subroots pg_node_attr(read_write_ignore);

    /* indices of subplans that require REWIND */
    Bitmapset     *rewindPlanIDs;

    /* "flat" rangetable for executor */
    List          *finalrtable;
    List          *finalrteperminfos;
    List          *finalrowmarks;
    List          *resultRelations;
    List          *appendRelations;

    /* OIDs of relations the plan depends on */
    List          *relationOids;
    List          *invalItems;
    List          *paramExecTypes;

    Index          lastPHId;
    Index          lastRowMarkId;
    int            lastPlanNodeId;

    bool           transientPlan;
    bool           dependsOnRole;
    bool           parallelModeOK;
    bool           parallelModeNeeded;
    char           maxParallelHazard;

    PartitionDirectory partition_directory pg_node_attr(read_write_ignore);
} PlannerGlobal;
```

**Commentary**

- One `PlannerGlobal` exists per top-level call to `standard_planner`,
  shared by all subquery `PlannerInfo`s (see `PlannerInfo.glob`).
- `subplans` / `subpaths` / `subroots` are parallel arrays; index = the
  `SubPlan.plan_id` field.
- The "flat" lists (`finalrtable`, `finalrowmarks`, `resultRelations`)
  are the executor-facing flattened versions assembled in
  `set_plan_references` (`setrefs.c:287`).
- `parallelModeOK` is set by the cost model after determining the
  query has no parallel-unsafe expressions; `parallelModeNeeded`
  becomes true only when a chosen plan actually contains a `Gather`.
- `lastPHId` / `lastPlanNodeId` are running counters preserved across
  subquery boundaries. See [`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md).

---

## PlannerInfo (`root`)

`src/include/nodes/pathnodes.h:195` (typedef forward-declared at line 191).

```c
struct PlannerInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)

    NodeTag        type;

    Query         *parse;            /* the Query being planned */
    PlannerGlobal *glob;             /* global info for this run */
    Index          query_level;      /* 1 at outermost Query */
    PlannerInfo   *parent_root;      /* NULL at outermost level */

    List          *plan_params;      /* params this level exposes */
    Bitmapset     *outer_params;     /* params from outer levels */

    /* indexed by RT index (see also simple_rte_array) */
    struct RelOptInfo **simple_rel_array;
    int            simple_rel_array_size;
    RangeTblEntry **simple_rte_array;
    struct AppendRelInfo **append_rel_array;

    Relids         all_baserels;
    Relids         outer_join_rels;
    Relids         all_query_rels;

    /* DP search workspaces */
    List          *join_rel_list;
    struct HTAB   *join_rel_hash;
    List         **join_rel_level;
    int            join_cur_level;

    List          *init_plans;       /* InitSubPlans */
    List          *cte_plan_ids;
    List          *multiexpr_params;
    List          *join_domains;
    List          *eq_classes;       /* active EquivalenceClasses */
    bool           ec_merging_done;
    List          *canon_pathkeys;
    List          *left_join_clauses;
    List          *right_join_clauses;
    List          *full_join_clauses;
    List          *join_info_list;   /* SpecialJoinInfos */
    int            last_rinfo_serial;

    Relids         all_result_relids;
    Relids         leaf_result_relids;
    List          *append_rel_list;  /* AppendRelInfos */
    List          *row_identity_vars;
    List          *rowMarks;
    List          *placeholder_list; /* PlaceHolderInfos */
    struct PlaceHolderInfo **placeholder_array;
    int            placeholder_array_size;
    List          *fkey_list;

    List          *query_pathkeys;
    List          *group_pathkeys;
    int            num_groupby_pathkeys;
    List          *window_pathkeys;
    List          *distinct_pathkeys;
    List          *sort_pathkeys;
    List          *setop_pathkeys;
    List          *part_schemes;

    List          *initial_rels;     /* RelOptInfos to join */

    /* Upper-rel RelOptInfos. Use fetch_upper_rel(). */
    List          *upper_rels[UPPERREL_FINAL + 1];
    struct PathTarget *upper_targets[UPPERREL_FINAL + 1];
    /* ... grouping/agg/window working state ... */
    /* ... cost / fraction inputs from grouping_planner ... */
    /* ... setrefs.c workspace ... */
    void          *join_search_private;   /* GEQO uses this */
    bool           partColsUpdated;
};
/* closes at pathnodes.h:556 */
```

**Commentary** (highlights — the full struct has roughly 100 fields)

- `parse` is the input `Query`; the planner *mutates it in place* in
  several preprocessing passes — be careful with copies.
- `simple_rel_array[i]` is the `RelOptInfo` for RT index `i` (1-based;
  index 0 is wasted). `simple_rte_array[i]` is the matching
  `RangeTblEntry`. They are lock-step and the canonical way to iterate
  per-baserel.
- `join_rel_level[]` is used by the DP search
  (`standard_join_search`); index `k` lists every `RelOptInfo` of
  exactly `k` baserels.
- `eq_classes` holds the `EquivalenceClass` list; all canonical
  `PathKey`s point into this list.
- `placeholder_list` and `placeholder_array` are kept consistent;
  `placeholder_array[phid]` returns the corresponding PHI.
- `upper_rels[stage]` indexes into the post-grouping pipeline:
  `UPPERREL_GROUP_AGG`, `UPPERREL_WINDOW`, `UPPERREL_DISTINCT`,
  `UPPERREL_ORDERED`, `UPPERREL_FINAL`.
- `init_plans` are the `SubPlan` nodes built by `subselect.c` for
  uncorrelated CTEs and SubLinks.
- `join_search_private` is opaque to core; GEQO stores its working
  context here.

See [`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md).

---

## RelOptInfo

`src/include/nodes/pathnodes.h:853–1046`

Closing line at 1046; full struct shown abbreviated below by clusters.

```c
typedef struct RelOptInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag        type;

    RelOptKind     reloptkind;       /* BASEREL, JOINREL, OTHER_*, UPPER_REL, ... */

    Relids         relids;           /* base + OJ relids in this rel */

    Cardinality    rows;             /* size estimate */

    /* per-relation planner control flags */
    bool           consider_startup;
    bool           consider_param_startup;
    bool           consider_parallel;

    struct PathTarget *reltarget;    /* default targetlist for this rel */

    /* materialization information */
    List          *pathlist;
    List          *ppilist;          /* ParamPathInfos used by pathlist */
    List          *partial_pathlist;
    struct Path   *cheapest_startup_path;
    struct Path   *cheapest_total_path;
    struct Path   *cheapest_unique_path;
    List          *cheapest_parameterized_paths;

    /* parameterization */
    Relids         direct_lateral_relids;
    Relids         lateral_relids;

    /* base-rel-only fields */
    Index          relid;
    Oid            reltablespace;
    RTEKind        rtekind;
    AttrNumber     min_attr;
    AttrNumber     max_attr;
    Relids        *attr_needed;
    int32         *attr_widths;
    Bitmapset     *notnullattnums;
    Relids         nulling_relids;
    List          *lateral_vars;
    Relids         lateral_referencers;
    List          *indexlist;        /* IndexOptInfo */
    List          *statlist;         /* StatisticExtInfo */
    BlockNumber    pages;
    Cardinality    tuples;
    double         allvisfrac;
    Bitmapset     *eclass_indexes;
    PlannerInfo   *subroot;          /* if subquery */
    List          *subplan_params;
    int            rel_parallel_workers;
    uint32         amflags;

    /* foreign-table fields */
    Oid            serverid;
    Oid            userid;
    bool           useridiscurrent;
    struct FdwRoutine *fdwroutine;
    void          *fdw_private;

    /* uniqueness cache */
    List          *unique_for_rels;
    List          *non_unique_for_rels;

    /* qual storage */
    List          *baserestrictinfo;
    QualCost       baserestrictcost;
    Index          baserestrict_min_security;
    List          *joininfo;
    bool           has_eclass_joins;

    /* partitionwise join */
    bool           consider_partitionwise_join;

    /* inheritance / partitioning links */
    struct RelOptInfo *parent;
    struct RelOptInfo *top_parent;
    Relids         top_parent_relids;

    PartitionScheme part_scheme;
    int            nparts;
    struct PartitionBoundInfoData *boundinfo;
    bool           partbounds_merged;
    List          *partition_qual;
    struct RelOptInfo **part_rels;
    Bitmapset     *live_parts;
    Relids         all_partrels;
    List         **partexprs;
    List         **nullable_partexprs;
} RelOptInfo;
```

**Commentary**

- `reloptkind` is the type tag: `BASEREL` (a real RTE), `JOINREL`
  (a synthetic relation produced by joining two children), `UPPER_REL`
  (post-grouping), `OTHER_MEMBER_REL` (an inheritance/partition child),
  or `DEAD_REL` (an excluded child).
- `relids` includes both base relids *and* outer-join relids
  (the latter only for joinrels). Base rels have a singleton `relids`
  set equal to `{relid}` plus possibly nulled-by relids.
- `pathlist` is the unsorted list of all currently-considered paths;
  `cheapest_*_path` are populated by `set_cheapest`. `partial_pathlist`
  holds parallel-partial paths that are later wrapped with
  `GatherPath`/`GatherMergePath`.
- `baserestrictinfo` is the per-baserel quals (single-relation);
  `joininfo` is the join clauses involving this rel and one other.
- `lateral_relids` is the *transitive closure* of LATERAL references;
  `direct_lateral_relids` is only those referenced directly.
- The partitioning fields (`part_scheme`, `nparts`, `boundinfo`,
  `part_rels`, `live_parts`) are only populated when the rel is the
  parent of a partitioned table.
- `consider_startup` triggers retention of cheap-startup paths (used
  for cursors and `LIMIT`); set per `cursor_tuple_fraction`.

See [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md),
[`./06_base_relation_paths.md`](./06_base_relation_paths.md).

---

## Path

`src/include/nodes/pathnodes.h:1621–1668`

```c
typedef struct Path
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag        type;

    /* tag identifying scan/join method */
    NodeTag        pathtype;

    RelOptInfo    *parent;

    PathTarget    *pathtarget;

    ParamPathInfo *param_info;       /* parameterization, or NULL */

    bool           parallel_aware;
    bool           parallel_safe;
    int            parallel_workers; /* 0 = not parallel */

    /* estimated size/costs */
    Cardinality    rows;
    Cost           startup_cost;
    Cost           total_cost;

    /* sort ordering of path's output */
    List          *pathkeys;         /* List of PathKey */
} Path;
```

**Commentary — the polymorphism trick**

- Every Path subtype embeds `Path` as its first field, so `Path *p =
  (Path *) somePath;` is always safe.
- `path->type` is the *struct identity* (e.g., `T_IndexPath`,
  `T_HashPath`); `path->pathtype` is the *executor flavor* (e.g.,
  `T_IndexScan`, `T_IndexOnlyScan`, `T_HashJoin`). For plain `Path`,
  `type == T_Path` and `pathtype` discriminates `SeqScan`,
  `SampleScan`, `FunctionScan`, `Result`, etc. — see
  [Appendix: Path Quick Reference](./appendix_path_quick_reference.md).
- A NULL `param_info` means an unparameterized path. `PATH_REQ_OUTER`
  (macro at pathnodes.h:1671) extracts the outer-relids set or NULL.
- `pathkeys` is canonical: every `PathKey` in it points to a
  canonical `EquivalenceClass`.

---

## IndexPath

`src/include/nodes/pathnodes.h:1709–1719`

```c
typedef struct IndexPath
{
    Path           path;
    IndexOptInfo  *indexinfo;
    List          *indexclauses;          /* IndexClause */
    List          *indexorderbys;         /* bare expressions */
    List          *indexorderbycols;      /* int list */
    ScanDirection  indexscandir;
    Cost           indextotalcost;
    Selectivity    indexselectivity;
} IndexPath;
```

`indexclauses` is a list of `IndexClause` (line 1755) entries that
match index columns; `indexorderbys` holds amcanorderbyop ORDER BY
expressions. `indextotalcost` and `indexselectivity` are cached so
that a sibling `BitmapHeapPath` over the same index doesn't recompute
them. `path.pathtype` is `T_IndexScan` for a plain index scan and
`T_IndexOnlyScan` for index-only.

---

## NestPath

`src/include/nodes/pathnodes.h:2092–2095`

```c
typedef struct NestPath
{
    JoinPath jpath;
} NestPath;
```

A pure derivative of `JoinPath` (line 2065) — a nestloop adds no fields
beyond what every join carries (jointype, inner/outer paths,
joinrestrictinfo, etc.).

---

## MergePath

`src/include/nodes/pathnodes.h:2132–2140`

```c
typedef struct MergePath
{
    JoinPath  jpath;
    List     *path_mergeclauses;
    List     *outersortkeys;
    List     *innersortkeys;
    bool      skip_mark_restore;
    bool      materialize_inner;
} MergePath;
```

`path_mergeclauses` is the subset of the join clauses used as merge
keys. `outersortkeys` / `innersortkeys` are non-NIL only when an
explicit `Sort` node is needed. `skip_mark_restore` allows the
executor to avoid `markpos` overhead in unique-inner cases.
`materialize_inner` is `true` when the planner wraps the inner side
with a `Material` node. See [`./08_join_paths_and_search.md`](./08_join_paths_and_search.md).

---

## HashPath

`src/include/nodes/pathnodes.h:2151–2157`

```c
typedef struct HashPath
{
    JoinPath    jpath;
    List       *path_hashclauses;
    int         num_batches;
    Cardinality inner_rows_total;
} HashPath;
```

`path_hashclauses` is the subset of the join clauses with a hash
operator. `num_batches > 1` indicates the hash table won't fit in
`work_mem * hash_mem_multiplier` and the join will spill. The inner
side becomes the `Hash` build side.

---

## AppendPath

`src/include/nodes/pathnodes.h:1931–1938`

```c
typedef struct AppendPath
{
    Path        path;
    List       *subpaths;
    int         first_partial_path;     /* list_length(subpaths) if none */
    Cardinality limit_tuples;           /* hard limit on output tuples, or -1 */
} AppendPath;
```

Holds heterogeneous subpaths — both regular and partial. Partial
subpaths begin at `first_partial_path` and require the consumer to be
parallel-aware (`enable_parallel_append`). `IS_DUMMY_APPEND` (macro at
1940) tests for an empty subpath list (provably-empty rel).

---

## MergeAppendPath

`src/include/nodes/pathnodes.h:1955–1960`

```c
typedef struct MergeAppendPath
{
    Path        path;
    List       *subpaths;
    Cardinality limit_tuples;
} MergeAppendPath;
```

Like `AppendPath` but each subpath produces output already sorted by
the parent's `pathkeys`, and `MergeAppend` k-way-merges them at
runtime to preserve the order.

---

## GatherPath

`src/include/nodes/pathnodes.h:2041–2047`

```c
typedef struct GatherPath
{
    Path  path;
    Path *subpath;
    bool  single_copy;
    int   num_workers;
} GatherPath;
```

Wraps a single partial subpath. `single_copy = true` forces the leader
to run the subpath alone (used by `GatherMergePath` semantics for
correctness). `num_workers` is the desired worker count;
`compute_parallel_worker` (`allpaths.c:4203`) caps it by
`max_parallel_workers_per_gather`. See [`./14_parallel_planning.md`](./14_parallel_planning.md).

---

## AggPath

`src/include/nodes/pathnodes.h:2253–2263`

```c
typedef struct AggPath
{
    Path        path;
    Path       *subpath;
    AggStrategy aggstrategy;     /* PLAIN, SORTED, HASHED, MIXED */
    AggSplit    aggsplit;        /* split aggregation mode */
    Cardinality numGroups;
    uint64      transitionSpace; /* for pass-by-ref transition data */
    List       *groupClause;
    List       *qual;            /* HAVING quals, if any */
} AggPath;
```

`aggstrategy` selects the algorithm: `AGG_PLAIN` (single group),
`AGG_SORTED` (input sorted by group keys), `AGG_HASHED`, or `AGG_MIXED`
(used in conjunction with `GroupingSetsPath`). `aggsplit` is non-zero
when partial-aggregation is in play (used by `Gather` / partitionwise
aggregation).

---

## SortPath

`src/include/nodes/pathnodes.h:2199–2203`

```c
typedef struct SortPath
{
    Path  path;
    Path *subpath;
} SortPath;
```

Sort keys are *implicit*: they equal the path's own `pathkeys`. A
`Sort` plan node cannot project, so `path.pathtarget` must equal
`subpath->pathtarget`.

---

## IncrementalSortPath

`src/include/nodes/pathnodes.h:2211–2215`

```c
typedef struct IncrementalSortPath
{
    SortPath spath;
    int      nPresortedCols;
} IncrementalSortPath;
```

Embeds `SortPath` (not `Path` directly!), so `IsA(p, SortPath)` is
**false** for an IncrementalSortPath even though it has the same
shape. `nPresortedCols` is the number of leading pathkeys already
satisfied by the input — only the trailing keys are sorted at runtime.

---

## MaterialPath

`src/include/nodes/pathnodes.h:1981–1985`

```c
typedef struct MaterialPath
{
    Path  path;
    Path *subpath;
} MaterialPath;
```

Inserted when an inner side needs `mark/restore` support its child
lacks, or when a parameterized inner needs to rescan cheaply. Cost is
computed by `cost_material`.

---

## MemoizePath

`src/include/nodes/pathnodes.h:1992–2006`

```c
typedef struct MemoizePath
{
    Path        path;
    Path       *subpath;
    List       *hash_operators;
    List       *param_exprs;
    bool        singlerow;
    bool        binary_mode;
    Cardinality calls;        /* expected number of rescans */
    uint32      est_entries;  /* expected cache size */
} MemoizePath;
```

Caches inner-side results in nestloops keyed by `param_exprs`.
`singlerow` is true when each cache entry holds at most one tuple.
`calls` tells the cost model how many rescans are expected.

---

## ProjectionPath

`src/include/nodes/pathnodes.h:2173–2178`

```c
typedef struct ProjectionPath
{
    Path  path;
    Path *subpath;
    bool  dummypp;     /* true if no separate Result is needed */
} ProjectionPath;
```

`dummypp = true` means the projection can be folded into the input
plan node's targetlist; no separate `Result` is emitted by
`create_projection_plan`.

---

## LimitPath

`src/include/nodes/pathnodes.h:2400–2407`

```c
typedef struct LimitPath
{
    Path        path;
    Path       *subpath;
    Node       *limitOffset;
    Node       *limitCount;
    LimitOption limitOption;     /* FETCH FIRST WITH TIES vs exact */
} LimitPath;
```

The cost is computed inline from the parent's `rows` and the
`limitCount` value.

---

## RestrictInfo

`src/include/nodes/pathnodes.h:2559–2711`

The single most-allocated planner struct. Highlighted clusters:

```c
typedef struct RestrictInfo
{
    pg_node_attr(no_read, no_query_jumble)
    NodeTag       type;

    /* the represented clause of WHERE or JOIN */
    Expr         *clause;

    /* clause classification */
    bool          is_pushed_down;
    bool          can_join;
    bool          pseudoconstant;
    bool          has_clone, is_clone;     /* identity-3 clones */
    bool          leakproof;
    VolatileFunctionStatus has_volatile;
    Index         security_level;
    int           num_base_rels;

    /* relids */
    Relids        clause_relids;
    Relids        required_relids;
    Relids        incompatible_relids;
    Relids        outer_relids;
    Relids        left_relids, right_relids;

    /* OR-clause representation */
    Expr         *orclause;

    int           rinfo_serial;            /* unique serial per RI */
    EquivalenceClass *parent_ec;           /* if EC-derived */

    /* cost & selectivity caches */
    QualCost      eval_cost;
    Selectivity   norm_selec, outer_selec;

    /* mergejoin caches */
    List         *mergeopfamilies;
    EquivalenceClass *left_ec, *right_ec;
    EquivalenceMember *left_em, *right_em;
    List         *scansel_cache;           /* MergeScanSelCache list */
    bool          outer_is_left;

    /* hashjoin caches */
    Oid           hashjoinoperator;
    Selectivity   left_bucketsize, right_bucketsize;
    Selectivity   left_mcvfreq, right_mcvfreq;

    /* memoize-ready hash equality ops */
    Oid           left_hasheqoperator, right_hasheqoperator;
} RestrictInfo;
```

**Commentary**

- `clause` is the boolean expression itself (a `Node *` cast to `Expr
  *`). It can be an `OpExpr`, a `BoolExpr`, a `ScalarArrayOpExpr`,
  etc. — anything boolean.
- `clause_relids` is what relids the clause *references*;
  `required_relids` adds outer-join relids the clause depends on. The
  difference matters for outer-join semantics (see *identity 3*).
- `is_pushed_down` records whether the clause originated above an
  outer join and was pushed down. The macro `RINFO_IS_PUSHED_DOWN`
  (pathnodes.h:2716) does the correct test in the presence of clones.
- `mergeopfamilies` non-NIL ⇒ the clause is mergejoinable; the cached
  `left_ec`/`right_ec`/`left_em`/`right_em` make `select_mergejoin_clauses`
  cheap.
- `hashjoinoperator != InvalidOid` ⇒ hashjoinable.
- `rinfo_serial` is unique per `PlannerInfo`; outer-join *clones* and
  child-rel copies *share* the parent's serial, which lets
  `add_paths_to_joinrel` deduplicate quals.

See [`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md).

---

## EquivalenceClass

`src/include/nodes/pathnodes.h:1379–1399`

```c
typedef struct EquivalenceClass
{
    pg_node_attr(custom_read_write, no_copy_equal, no_read, no_query_jumble)
    NodeTag           type;

    List             *ec_opfamilies;       /* btree opfamily OIDs */
    Oid               ec_collation;
    List             *ec_members;          /* EquivalenceMembers */
    List             *ec_sources;          /* generating RestrictInfos */
    List             *ec_derives;          /* derived RestrictInfos */
    Relids            ec_relids;           /* relids in non-child members */
    bool              ec_has_const;        /* any pseudoconstants? */
    bool              ec_has_volatile;     /* sole member volatile? */
    bool              ec_broken;           /* failed to generate clauses? */
    Index             ec_sortref;          /* originating sortclause label */
    Index             ec_min_security;
    Index             ec_max_security;
    struct EquivalenceClass *ec_merged;    /* merged-into-this pointer */
} EquivalenceClass;
```

**Commentary**

- `ec_members` holds *both* baserel members and `em_is_child` members
  (see EquivalenceMember). `ec_relids` excludes child members.
- `ec_has_const` triggers the `EC_MUST_BE_REDUNDANT` macro: any pathkey
  built from such an EC is redundant because the EC has only one
  possible value.
- `ec_broken = true` is set in obscure cases where the planner cannot
  build all needed implied clauses (typically with non-strict outer
  join interactions); the planner falls back to applying the original
  join clauses directly.
- `ec_merged` is set when an EC has been replaced by another via
  `ec_merging_done`; consult the merged target instead.

See [`./10_equivalence_classes_and_pathkeys.md`](./10_equivalence_classes_and_pathkeys.md).

---

## EquivalenceMember

`src/include/nodes/pathnodes.h:1430–1444`

```c
typedef struct EquivalenceMember
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag      type;

    Expr        *em_expr;            /* the expression */
    Relids       em_relids;          /* all relids in em_expr */
    bool         em_is_const;        /* pseudoconstant? */
    bool         em_is_child;        /* derived for an appendrel child? */
    Oid          em_datatype;
    JoinDomain  *em_jdomain;
    struct EquivalenceMember *em_parent;   /* if em_is_child */
} EquivalenceMember;
```

`em_is_child` members are *projections* of a parent EM into a child
relation — they exist solely so that an `IndexScan` on the child has
matchable pathkeys. They never contribute to `ec_relids`.

---

## PathKey

`src/include/nodes/pathnodes.h:1463–1474`

```c
typedef struct PathKey
{
    pg_node_attr(no_read, no_query_jumble)
    NodeTag             type;

    EquivalenceClass   *pk_eclass;       /* the value being ordered */
    Oid                 pk_opfamily;     /* btree opfamily */
    int                 pk_strategy;     /* BTLessStrategyNumber=ASC, etc. */
    bool                pk_nulls_first;
} PathKey;
```

A canonical `PathKey` is unique per (eclass, opfamily, strategy, nulls)
quadruple — see `make_canonical_pathkey` (`pathkeys.c:55`). All
pathkeys in `PlannerInfo.canon_pathkeys` are canonical.

---

## SpecialJoinInfo

`src/include/nodes/pathnodes.h:2891–2912`

```c
struct SpecialJoinInfo
{
    pg_node_attr(no_read, no_query_jumble)
    NodeTag    type;

    Relids     min_lefthand;          /* minimum LHS for legality */
    Relids     min_righthand;         /* minimum RHS for legality */
    Relids     syn_lefthand;          /* syntactic LHS */
    Relids     syn_righthand;         /* syntactic RHS */
    JoinType   jointype;              /* INNER, LEFT, FULL, SEMI, ANTI */
    Index      ojrelid;               /* RT index of the OJ, or 0 */
    Relids     commute_above_l;
    Relids     commute_above_r;
    Relids     commute_below_l;
    Relids     commute_below_r;
    bool       lhs_strict;            /* joinclause strict on some LHS rel */

    /* for SEMI joins: */
    bool       semi_can_btree;
    bool       semi_can_hash;
    List      *semi_operators;
    List      *semi_rhs_exprs;
};
```

**Commentary**

- `jointype` is never `JOIN_RIGHT`; the planner switches inputs to
  produce an equivalent `LEFT JOIN`. Likewise never `JOIN_RIGHT_ANTI`.
- `min_lefthand` and `min_righthand` are minimal *base + OJ* relid
  sets. `join_is_legal` (`joinrels.c:350`) rejects any join attempt
  that would form a join not containing both sets.
- `ojrelid` numbers the outer join itself in the rangetable; this
  value is what gets added to a Var's `varnullingrels` when the var
  becomes nullable through this join.
- The `commute_above/below_l/r` sets enumerate which other outer joins
  can be exchanged with this one — required for correctly applying
  the optimizer's join-order identities.

See [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

---

## PlaceHolderVar

`src/include/nodes/pathnodes.h:2780–2800`

```c
typedef struct PlaceHolderVar
{
    pg_node_attr(no_query_jumble)
    Expr      xpr;

    Expr     *phexpr;          /* the wrapped expression */
    Relids    phrels;          /* base+OJ relids in phexpr's source */
    Relids    phnullingrels;   /* outer-join relids that null this PHV */
    Index     phid;            /* unique PHV id */
    Index     phlevelsup;      /* >0 for outer-query reference */
} PlaceHolderVar;
```

A PHV "freezes" an expression so that, even after pull-up makes it
visible above an outer join, references to it preserve the
not-yet-nullable form. PHVs are matched by `phid` to a single
`PlaceHolderInfo`.

---

## PlaceHolderInfo

`src/include/nodes/pathnodes.h:3074–3100`

```c
typedef struct PlaceHolderInfo
{
    pg_node_attr(no_read, no_query_jumble)
    NodeTag         type;

    Index           phid;
    PlaceHolderVar *ph_var;
    Relids          ph_eval_at;     /* where we may evaluate */
    Relids          ph_lateral;     /* lateral references */
    Relids          ph_needed;      /* highest level where used */
    int32           ph_width;
} PlaceHolderInfo;
```

`ph_eval_at` is the lowest joinrel that contains all relids needed by
the wrapped expression. `ph_needed` is the highest level at which any
output references the PHV. Together they bound where the planner is
allowed to position the evaluation.

---

## ParamPathInfo

`src/include/nodes/pathnodes.h:1575–1585`

```c
typedef struct ParamPathInfo
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag      type;

    Relids       ppi_req_outer;     /* outer rels supplying parameters */
    Cardinality  ppi_rows;          /* row estimate adjusted for ppi_clauses */
    List        *ppi_clauses;       /* joinclauses moved inside */
    Bitmapset   *ppi_serials;       /* rinfo_serial set of enforced quals */
} ParamPathInfo;
```

A parameterized `Path` shares `ParamPathInfo` with all sibling paths
having the same `ppi_req_outer`. Constructed by
`get_baserel_parampathinfo` / `get_joinrel_parampathinfo` /
`get_appendrel_parampathinfo` (in `relnode.c`).

---

## AppendRelInfo

`src/include/nodes/pathnodes.h:2959–3016`

```c
typedef struct AppendRelInfo
{
    pg_node_attr(no_query_jumble)
    NodeTag      type;

    Index        parent_relid;       /* RT index of parent */
    Index        child_relid;        /* RT index of child */

    Oid          parent_reltype;     /* composite OID of parent (or InvalidOid for UNION ALL) */
    Oid          child_reltype;

    /* parent column N -> child expression at index N-1 (NIL for dropped) */
    List        *translated_vars;

    /* reverse map: child column ccolno -> parent column */
    int          num_child_cols;
    AttrNumber  *parent_colnos;

    Oid          parent_reloid;      /* OID of parent relation (or InvalidOid) */
} AppendRelInfo;
```

Two flavors:

1. **Inheritance / partitioning** — both parent and child are real
   relations; `translated_vars` has simple `Var` entries.
2. **UNION ALL flattening** — both parent and child are subqueries;
   `translated_vars` may contain arbitrary expressions.

Used by `adjust_appendrel_attrs` (`appendinfo.c:196`) to translate
quals and pathkeys from a parent into a child context.

---

## JoinDomain

`src/include/nodes/pathnodes.h:1317–1324`

```c
typedef struct JoinDomain
{
    pg_node_attr(no_copy_equal, no_read, no_query_jumble)
    NodeTag    type;

    Relids     jd_relids;            /* all relids inside the domain */
} JoinDomain;
```

A trivial structure: a domain is identified entirely by its relid set.
Stored in `PlannerInfo.join_domains`. Referenced by EquivalenceMembers
(`em_jdomain`) so the planner can detect when two textually-equal
constants come from semantically distinct join contexts.

---

## Cross-references to companion modules

- Lifecycle and `PlannerInfo`/`PlannerGlobal` ↔ [`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md)
- `RelOptInfo` and qual distribution ↔ [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md)
- `RestrictInfo` and clause utilities ↔ [`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md)
- `EquivalenceClass`, `PathKey` ↔ [`./10_equivalence_classes_and_pathkeys.md`](./10_equivalence_classes_and_pathkeys.md)
- All `Path` subtypes table ↔ [Appendix: Path Quick Reference](./appendix_path_quick_reference.md)
- `SpecialJoinInfo`, `PlaceHolderVar`, `AppendRelInfo` ↔ [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md)
- `AppendRelInfo` and partitioning ↔ [`./13_inheritance_and_partitioning.md`](./13_inheritance_and_partitioning.md)
- Glossary (cross-link target for terminology): [Appendix: Glossary](./appendix_glossary.md)
