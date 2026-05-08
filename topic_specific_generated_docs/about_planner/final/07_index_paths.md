# 07. Index Path Generation

Prerequisites: [06_base_relation_paths.md](./06_base_relation_paths.md).

This module documents how `create_index_paths` and friends turn a
rel's index list into `IndexPath` / `BitmapHeapPath` / `BitmapAndPath`
/ `BitmapOrPath` / `TidPath` candidates. Source:
`src/backend/optimizer/path/indxpath.c` (~3700 lines) and
`src/backend/optimizer/path/tidpath.c`.

---

## 1. Why this exists

Index access produces several path subtypes:

- `IndexPath` (regular IndexScan and IndexOnlyScan).
- `BitmapHeapPath` with a `bitmapqual` tree of `IndexPath` /
  `BitmapAndPath` / `BitmapOrPath`.
- `TidPath` and `TidRangePath` for `CTID` quals.

The planner must:

1. Enumerate which RestrictInfos / EC-derived clauses match each
   index.
2. Build per-index plain paths (and parameterized variants when
   join clauses can be used as index quals through nestloop
   parameterization).
3. Combine multiple indexes with AND / OR using bitmap paths.

---

## 2. Symbol table

| Symbol                                | File:line                                     | Importance |
|---------------------------------------|-----------------------------------------------|------------|
| `create_index_paths`                  | `src/backend/optimizer/path/indxpath.c:234`   | 0.86 |
| `build_index_paths`                   | `src/backend/optimizer/path/indxpath.c:804`   | 0.78 |
| `match_clause_to_indexcol`            | `src/backend/optimizer/path/indxpath.c`       | 0.65 |
| `match_restriction_clauses_to_index`  | `src/backend/optimizer/path/indxpath.c`       | 0.55 |
| `match_join_clauses_to_index`         | `src/backend/optimizer/path/indxpath.c`       | 0.55 |
| `match_eclass_clauses_to_index`       | `src/backend/optimizer/path/indxpath.c`       | 0.55 |
| `consider_index_join_clauses`         | `src/backend/optimizer/path/indxpath.c`       | 0.55 |
| `choose_bitmap_and`                   | `src/backend/optimizer/path/indxpath.c`       | 0.65 |
| `generate_bitmap_or_paths`            | `src/backend/optimizer/path/indxpath.c`       | 0.55 |
| `bitmap_and_cost_est` / `bitmap_scan_cost_est` | `src/backend/optimizer/path/indxpath.c` | 0.45 |
| `create_index_path`                   | `src/backend/optimizer/util/pathnode.c`       | 0.65 |
| `create_bitmap_heap_path`             | `src/backend/optimizer/util/pathnode.c`       | 0.55 |
| `create_bitmap_and_path` / `create_bitmap_or_path` | `src/backend/optimizer/util/pathnode.c` | 0.45 |
| `create_tidscan_paths`                | `src/backend/optimizer/path/tidpath.c`        | 0.50 |
| `IndexOptInfo`                        | `src/include/nodes/pathnodes.h:1106`          | 0.65 |
| `IndexClause`                         | `src/include/nodes/pathnodes.h:1755`          | 0.50 |

---

## 3. `create_index_paths` — main entry

### 3.1 Signature

```c
void create_index_paths(PlannerInfo *root, RelOptInfo *rel);
```

Source: `src/backend/optimizer/path/indxpath.c:234`.

### 3.2 Per-index loop (annotated)

```c
/* Skip if no indexes */
if (rel->indexlist == NIL) return;

bitindexpaths = bitjoinpaths = joinorclauses = NIL;
foreach(lc, rel->indexlist) {
    IndexOptInfo *index = ...;
    if (index->indpred != NIL && !index->predOK)
        continue;     /* partial index whose predicate is not implied */

    /* (a) Match plain restriction clauses to this index */
    MemSet(&rclauseset, 0, sizeof(rclauseset));
    match_restriction_clauses_to_index(root, index, &rclauseset);

    /* (b) Build paths from those clauses (plain + bitmap candidates) */
    get_index_paths(root, rel, index, &rclauseset, &bitindexpaths);

    /* (c) Match available join clauses (RestrictInfos) -- collect
           OR-clauses for later */
    MemSet(&jclauseset, 0, sizeof(jclauseset));
    match_join_clauses_to_index(root, rel, index, &jclauseset,
                                 &joinorclauses);

    /* (d) Match EC-derived join clauses (created on demand) */
    MemSet(&eclauseset, 0, sizeof(eclauseset));
    match_eclass_clauses_to_index(root, index, &eclauseset);

    /* (e) Build parameterized index paths from join clauses */
    if (jclauseset.nonempty || eclauseset.nonempty)
        consider_index_join_clauses(root, rel, index,
                                     &rclauseset, &jclauseset,
                                     &eclauseset, &bitjoinpaths);
}

/* (f) Generate BitmapOrPaths from OR-form restriction quals */
indexpaths = generate_bitmap_or_paths(root, rel,
                                       rel->baserestrictinfo, NIL);
bitindexpaths = list_concat(bitindexpaths, indexpaths);

/* (g) Generate BitmapOrPaths from join OR-clauses */
indexpaths = generate_bitmap_or_paths(root, rel, joinorclauses,
                                       rel->baserestrictinfo);
bitjoinpaths = list_concat(bitjoinpaths, indexpaths);

/* (h) Pick best AND combination for plain bitmap paths */
if (bitindexpaths != NIL) {
    bitmapqual = choose_bitmap_and(root, rel, bitindexpaths);
    bpath = create_bitmap_heap_path(root, rel, bitmapqual,
                                     rel->lateral_relids, 1.0, 0);
    add_path(rel, (Path *) bpath);
    if (rel->consider_parallel && rel->lateral_relids == NULL)
        create_partial_bitmap_paths(root, rel, bitmapqual);
}

/* (i) For each distinct parameterization in bitjoinpaths,
       choose best AND combination and emit a BitmapHeapPath */
... (loops over all_path_outers)
```

### 3.3 Three clause sources

Per index, three `IndexClauseSet`s are built (each indexed by index
column):

- **Restriction clauses** (`rclauseset`) — drawn from
  `rel->baserestrictinfo`.
- **Join clauses** (`jclauseset`) — drawn from `rel->joininfo`.
  These produce **parameterized** paths whose `param_info` is the
  outer rel(s) supplying the parameter.
- **EC-derived clauses** (`eclauseset`) — generated via
  `generate_implied_equalities_for_column` from EquivalenceClasses
  containing this index column.

The third source is what makes ECs so powerful: even when no
literal qual mentions an index column, an EC can synthesize one
from any chain of equalities.

---

## 4. `match_clause_to_indexcol`

Source: `src/backend/optimizer/path/indxpath.c`.

The matcher decides whether a single qual can be used as an
indexqual on a particular index column. It dispatches based on
clause shape:

| Clause shape | Handler |
|--------------|---------|
| `boolvar` / `NOT boolvar` (boolean column) | `match_boolean_index_clause` |
| `OpExpr indexcol op pseudoconst` | `match_opclause_to_indexcol` |
| `FuncExpr` (e.g. `lower(...) = const`) — only if the index is on the function | `match_funcclause_to_indexcol` |
| `ScalarArrayOpExpr indexcol op ANY/ALL(array)` | `match_saopclause_to_indexcol` |
| `RowCompareExpr` (`(a, b) < (c, d)`) | `match_rowcompare_to_indexcol` |
| Pulling out leading-key index conditions from LIKE/regex via `expand_indexqual_conditions` | helper |

Result: an `IndexClause` with:

```c
typedef struct IndexClause {
    NodeTag        type;
    RestrictInfo  *rinfo;
    List          *indexquals;
    bool           lossy;
    AttrNumber     indexcol;
    List          *indexcols;       /* for RowCompare */
} IndexClause;
```

Source: `src/include/nodes/pathnodes.h:1755`.

`indexquals` may differ from `rinfo->clause` (e.g. the matcher
rewrites `pseudoconst OP indexcol` → `indexcol commuted-OP
pseudoconst` to match the index machinery's expectation). When
`lossy = true`, the heap rechecks the original clause.

---

## 5. `build_index_paths`

### 5.1 Signature

```c
List *build_index_paths(PlannerInfo *root, RelOptInfo *rel,
                        IndexOptInfo *index, IndexClauseSet *clauses,
                        bool useful_predicate, ScanTypeControl scantype,
                        bool *skip_nonnative_saop, bool *skip_lower_saop);
```

Source: `src/backend/optimizer/path/indxpath.c:804`.

### 5.2 What it does

Given a set of matched IndexClauses, build:

- A regular `IndexPath` (forward scan).
- An `IndexOnlyScan` variant when the index can satisfy all needed
  outputs (`indextlist` covers `attr_needed`).
- A backward scan when the query asks for descending order matching
  the index's reverse direction.
- (When useful and supported) a non-native SAOP path: SAOP that the
  AM can't treat as native; presented as a BitmapIndexScan path
  candidate.
- (When useful) a "lower SAOP" path: SAOP not on the leading
  column, again only viable as a bitmap index scan.

### 5.3 Path construction

Each surviving path is:

- `create_index_path(root, index, indexclauses, indexorderbys,
   indexorderbycols, pathkeys, indexscandir, indexonly,
   required_outer, loop_count, partial_path)`
- where `pathkeys = build_index_pathkeys(...)` if the index has a
  sort ordering and the query wants it. See
  [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md).
- `loop_count` is computed by `get_loop_count(root, rel->relid,
   required_outer)` using cached row estimates of the outer rel(s).

`cost_index` (costsize.c) costs the path. It uses `amcostestimate`
(per-AM index cost estimator) to compute `indexStartupCost`,
`indexTotalCost`, `indexSelectivity`, `indexCorrelation`. The total
includes `indexCorrelation`-weighted random/sequential I/O for heap
fetches plus the index's own I/O. See
[09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md#cost_index-deep-dive).

---

## 6. `choose_bitmap_and`

Source: `src/backend/optimizer/path/indxpath.c`.

Selects which subset of bitmap-able paths to AND together. Greedy
algorithm:

1. Sort candidate bitmap paths by selectivity × cost (cheapest most
   useful first).
2. Iteratively add the next path if:
   - It is not redundant (its selectivity adds something compared
     to the current AND).
   - The total estimated cost of the AND so far stays below the
     cost of the same scan without this addition.
3. The cost of an AND combination is computed via
   `bitmap_and_cost_est` / `bitmap_scan_cost_est`, which:
   - Multiplies selectivities to derive total bitmap selectivity.
   - Adds bitmap-build cost (one per child) and the heap-recheck
     cost based on the resulting tuple count.

The result is wrapped in a `BitmapAndPath` if more than one path
was chosen, else returned as the single path.

---

## 7. `generate_bitmap_or_paths`

For an OR-clause whose arms are bitmap-able, build a `BitmapOrPath`
where each arm is itself a (possibly AND-combined) bitmap subpath.
Pre-existing `bitindexpaths` for the parent's restriction list can
be ANDed in *inside* each arm — see the `other_clauses` parameter
— so an OR can still benefit from common AND restrictions.

---

## 8. Parameterized index paths

The interesting case for nested-loop joins:

- A join clause `t1.a = t2.a` is in `t1->joininfo` and
  `t2->joininfo`.
- When generating index paths for `t2` and there's an index on
  `t2.a`, the join clause is treated as an indexqual. The resulting
  `IndexPath` has `param_info = get_baserel_parampathinfo(root, rel,
  required_outer = {t1})`.
- During join planning, `try_nestloop_path` will pair this
  paramized inner with an outer that supplies `{t1}`, producing a
  nestloop where `t1.a` is plugged into the index probe at runtime.

`consider_index_join_clauses` (indxpath.c) explores subsets of join
clauses to avoid building an explosion of parameterizations. It
uses `expand_indexqual_clauses` and ECs to expand a clause to all
available index conditions across rels.

The way these parameterized paths participate in join search is
documented in [08_join_paths_and_search.md](./08_join_paths_and_search.md#try_nestloop_path-deep-dive).

---

## 9. TID paths

Source: `src/backend/optimizer/path/tidpath.c`.

`create_tidscan_paths` examines `rel->baserestrictinfo` for clauses
of the form:

- `CTID = pseudoconstant` (single TID).
- `CTID = ANY (pseudoconstant_array)` (multiple TIDs).
- `CurrentOfExpr` (cursor reference).

If found, builds a `TidPath` whose `tidquals` are these. Cost via
`cost_tidscan` is essentially `len(tidquals)` × `random_page_cost`
plus per-tuple CPU.

For range comparisons `CTID < x AND CTID > y`, a `TidRangePath` is
built via `create_tidrangescan_path` (cost via
`cost_tidrangescan`).

---

## 10. `IndexOptInfo` struct

Source: `src/include/nodes/pathnodes.h:1106`.

Key fields:

- `indexoid`, `reltablespace`.
- `pages`, `tuples`, `tree_height`.
- `ncolumns`, `nkeycolumns`.
- `indexkeys[]`, `indexcollations[]`, `opfamily[]`, `opcintype[]`.
- `sortopfamily[]`, `reverse_sort[]`, `nulls_first[]` — non-NULL
  only for ordered indexes.
- `unique`, `nullsnotdistinct`, `immediate` — uniqueness
  properties.
- `predOK` — partial index predicate satisfiable from quals.
- `indrestrictinfo` — list of base RestrictInfos available against
  the rel (filtered by predicate match for partial indexes).
- `relam` — the AM oid.
- `amcostestimate`, `amcanorderbyop`, `amsearcharray`, etc. — AM
  API pointers cached.

`check_index_predicates` populates `predOK` and `indrestrictinfo`
per index. Run once during `set_plain_rel_size`.

A more comprehensive struct walkthrough is in
[appendix_data_structures.md](./appendix_data_structures.md#indexoptinfo).

---

## 11. Performance characteristics

- O(I × R) where I is index count and R is restriction-clause count
  (per-clause matching is cheap per index column).
- `consider_index_join_clauses` is O(2^k) in the worst case for an
  index with k join-eligible clauses (it considers all subsets) —
  but k is usually small.
- `choose_bitmap_and` is O(B²) in the number of bitmap paths —
  fine in practice because usually ≤ 5 bitmaps.

---

## 12. Cross-references

- Costs: [09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md)
- Equivalence classes (used in `match_eclass_clauses_to_index`):
  [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md)
- ParamPathInfo and lateral handling:
  [08_join_paths_and_search.md](./08_join_paths_and_search.md)
- Path catalog (`IndexPath`, `BitmapHeapPath`, `BitmapAndPath`,
  `BitmapOrPath`, `TidPath`, `TidRangePath`):
  [18_path_catalog.md](./18_path_catalog.md#scan-paths)

---

Next: [08_join_paths_and_search.md](./08_join_paths_and_search.md)
