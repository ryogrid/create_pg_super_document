# Appendix: Glossary

This glossary defines the planner-specific terms that recur throughout
the rest of the documentation. Where a term is the name of a struct, an
identifier, or a function, the first occurrence in this list links to
the most relevant module under `final/`. Terms are alphabetical.

---

### AppendRelInfo
A planner data structure (`pathnodes.h:2959`) that records the
parent-child relationship between an inheritance/partition parent table
and one of its children, or between a UNION ALL parent subquery and one
of its branches. It carries the per-column translation list
`translated_vars` so that quals expressed against the parent can be
rewritten to refer to a child relation. Stored in
`PlannerInfo.append_rel_list`. See
[`./13_inheritance_and_partitioning.md`](./13_inheritance_and_partitioning.md).

### Baserel
A `RelOptInfo` whose `reloptkind == RELOPT_BASEREL`. It represents one
of the original relations in the FROM clause (an ordinary table, a
function-RTE, etc.). Baserels are the leaves of the join search tree
and are populated by `set_base_rel_pathlists`
(`allpaths.c:333`). See [`./06_base_relation_paths.md`](./06_base_relation_paths.md).

### Broken EquivalenceClass
An `EquivalenceClass` whose `ec_broken` flag is `true`. Marked when the
planner could not generate every implied equality clause (for example,
because of outer-join nullability). Once broken, `add_path` falls back
to applying the original join clauses individually instead of using the
EC-derived shortcut clauses. See
[`./10_equivalence_classes_and_pathkeys.md`](./10_equivalence_classes_and_pathkeys.md).

### Chromosome (GEQO context)
In GEQO (`geqo_main.c:72`), a chromosome is a permutation of the input
relations representing one candidate join order. A pool of chromosomes
is evolved with mutation and crossover; each is "evaluated" by
`geqo_eval` (`geqo_eval.c:57`), which builds the corresponding tree of
joinrels with `gimme_tree` and `merge_clump`. See
[`./15_geqo.md`](./15_geqo.md).

### cheapest_total_path / cheapest_startup_path
Two of several "winners" attached to every `RelOptInfo` after
`set_cheapest` (`pathnode.c:242`) runs. `cheapest_total_path` is the
path with the smallest `total_cost`; `cheapest_startup_path` is the
path with the smallest `startup_cost` (relevant for cursors / `LIMIT`).
A `RelOptInfo` may also expose `cheapest_unique_path` and a list of
`cheapest_parameterized_paths`.

### Cost (startup vs total)
Every `Path` carries `startup_cost` (cost expended before the *first*
output tuple is available; e.g., a Sort must read its full input) and
`total_cost` (cost when all output tuples have been produced). The pair
is used by `compare_path_costs_fuzzily` (`pathnode.c:164`) so that
`add_path` can keep paths on the Pareto frontier.

### cost_diff_fuzz_factor (`STD_FUZZ_FACTOR`)
A 1 % fuzz tolerance (`STD_FUZZ_FACTOR = 1.01`, `pathnode.c:47`) used by
`compare_path_costs_fuzzily` to decide that two costs are "essentially
equal." This stops the planner from churning through near-identical
paths whose differences would not survive estimation noise. (The
proposal name `MAX_FUZZY_PATH_DELTA` referenced in older notes is the
same idea.)

### Dummy rel
A `RelOptInfo` provably empty: typically because a constraint or
exclusion clause makes its predicate `FALSE`. Recognized by
`IS_DUMMY_REL` / `is_dummy_rel` (`joinrels.c:1333`). The planner
short-circuits joins involving dummy rels and uses a degenerate
`AppendPath` with no children to materialize them.

### EquivalenceClass (EC)
A set of expressions known to be transitively equal under the same
btree opfamily (`pathnodes.h:1379`). ECs let the planner discover
implied equalities (e.g., from `a.x = b.x AND b.x = c.x` it derives
`a.x = c.x`) and represent sort orderings via `PathKey`. Built by
`process_equivalence` (`equivclass.c:117`), processed by
`generate_base_implied_equalities` (`equivclass.c:1028`) and
`generate_join_implied_equalities` (`equivclass.c:1376`). See
[`./10_equivalence_classes_and_pathkeys.md`](./10_equivalence_classes_and_pathkeys.md).

### EquivalenceMember (EM)
One expression inside an `EquivalenceClass` (`pathnodes.h:1430`). An EM
records its own `Relids`, datatype, and (for child rels) a back pointer
to the corresponding parent EM. Members with `em_is_child = true`
provide per-child translations of a parent EM and do **not** count
toward `ec_relids`.

### GEQO (Genetic Query Optimizer)
A genetic-algorithm-based join order search used in place of the
exhaustive DP search when the join list size meets `geqo_threshold`.
Implemented in `src/backend/optimizer/geqo/`. See
[`./15_geqo.md`](./15_geqo.md).

### geqo_threshold
The GUC controlling when GEQO is used (`guc_tables.c:2103`, default 12).
If `length(initial_rels) >= geqo_threshold`, the join-search path runs
through `geqo()` instead of `standard_join_search`. See
[Appendix: GUCs](./appendix_guc_parameters.md#geqo).

### Identity 3
PostgreSQL's terminology (from `optimizer/README`) for the outer-join
identity that lets the planner safely commute an inner join above an
outer join when a strict qual references the outer-joined side. The
planner enforces this with `min_lefthand`/`min_righthand` (see
`SpecialJoinInfo`) and with **clone** RestrictInfos
(`RestrictInfo.has_clone` / `is_clone`). See
[`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

### InitPlan
A SubPlan that is evaluated **once** per query execution; the result
becomes a Param visible to the rest of the plan. Built by
`SS_process_ctes` (`subselect.c:880`) for non-correlated CTEs and by
`make_subplan` (`subselect.c:162`) for non-correlated SubLinks. See
[`./12_subquery_and_sublink.md`](./12_subquery_and_sublink.md).

### Joinclause
A `RestrictInfo` whose `clause_relids` spans more than one base
relation. Distinguished from a *scan-qual* (single-relation
restriction) and from a *pseudoconstant* qual. Joinclauses are stored
on each input rel's `joininfo` list and are filtered to the current
join in `add_paths_to_joinrel`.

### JoinDomain
A `JoinDomain` (`pathnodes.h:1317`) is the set of relations that are
inner-joined together at a given level of the join tree. Used by
`EquivalenceClass` to mark the provenance of pseudoconstant members so
that two textually-identical constants from different join domains
won't be incorrectly merged.

### Joinrel
A `RelOptInfo` with `reloptkind == RELOPT_JOINREL`, representing a join
of two or more baserels. Built by `build_join_rel` and populated by
`add_paths_to_joinrel` (`joinpath.c:124`).

### Junk column
A targetlist entry whose `resjunk` flag is true. The column is needed
internally (e.g., a row-identity column for UPDATE, or a sort key not
selected by the user) but is stripped from the final query result. Set
up by `preprocess_targetlist` (`preptlist.c:64`).

### Lateral
A FROM-list item whose subquery / function references columns from
preceding FROM-list items (SQL `LATERAL`). The planner records lateral
reference relids in `RelOptInfo.lateral_relids` and `direct_lateral_relids`
and inserts them into a path's `ParamPathInfo.ppi_req_outer`, so that
lateral references are treated like nestloop parameters. See
`extract_lateral_references` (`initsplan.c:406`).

### min_lefthand / min_righthand
Fields on `SpecialJoinInfo` (`pathnodes.h:2896-2897`). These minimal
relid sets must be available on each side of an outer join for it to be
legal at all. `join_is_legal` (`joinrels.c:350`) consults them to
reject join orderings that would violate outer-join semantics.

### otherrel
A `RelOptInfo` with `reloptkind == RELOPT_OTHER_MEMBER_REL` (a child of
an inheritance/partitioning parent) or `RELOPT_OTHER_JOINREL` /
`RELOPT_OTHER_UPPER_REL`. Otherrels carry the same plumbing as
baserels/joinrels but represent a *child* in an Append tree.

### OuterJoinInfo
Historical name (still seen in some comments) for what is now called a
`SpecialJoinInfo`. Created by `make_outerjoininfo` (`initsplan.c:1360`).

### parallel-safe / parallel-restricted / parallel-unsafe
The three parallel-hazard levels (lowest to highest) computed by
`max_parallel_hazard` (`clauses.c:734`). A *parallel-safe* expression
can run in any parallel worker. *Parallel-restricted* expressions can
run in the leader of a parallel plan but not in workers (e.g., they
read parameters set by the leader). *Parallel-unsafe* expressions
disqualify the entire query from any parallelism. See
[`./14_parallel_planning.md`](./14_parallel_planning.md).

### Parameterized path
A `Path` with a non-NULL `param_info` (`ParamPathInfo`). It can produce
output only when an outer rel supplies parameters at runtime; the path
is therefore only joinable on the *inside* of a nestloop with the
outer-rel(s) named in `ppi_req_outer`. See
[`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md).

### ParamPathInfo
The struct (`pathnodes.h:1575`) that records a path's parameterization:
the outer relids it depends on (`ppi_req_outer`), the joinclauses moved
inside (`ppi_clauses`), and the row estimate adjusted for those
clauses. Constructed by `get_baserel_parampathinfo` /
`get_joinrel_parampathinfo` / `get_appendrel_parampathinfo`.

### Path
The base struct of every algebraic plan node (`pathnodes.h:1621`). It
is *polymorphic via pathtype*: the field `pathtype` (a `NodeTag`)
identifies which kind of executor plan would be produced. Many simple
Path subtypes (SeqScan, ValuesScan, FunctionScan, Result, …) reuse the
plain `Path` struct itself and are distinguished only by `pathtype`.
See [`./06_base_relation_paths.md`](./06_base_relation_paths.md) and
[Appendix: Path Quick Reference](./appendix_path_quick_reference.md).

### Path/Plan duality
PostgreSQL's planner manipulates two independent trees: a **Path** tree
(algebraic, supports cheap copying and adding alternatives), and a
**Plan** tree (executable, immutable once built). The planner picks
one Path per relation (the cheapest under each interesting ordering)
and converts it to a Plan in `create_plan` (`createplan.c:338`). The
duality is what lets `add_path` keep many alternatives alive cheaply.

### PathKey
A struct (`pathnodes.h:1463`) that encodes one component of a sort
ordering, by referencing the `EquivalenceClass` of the value being
ordered and the strategy/nulls-first flags. A path's sort order is a
`List *pathkeys` of `PathKey` nodes.

### Pathkey-contained-in
The relation `pathkeys_contained_in(L, R)` (`pathkeys.c:341`) is true
when every prefix-match required by `L` is satisfied by `R`. It is the
test the planner uses to decide whether an existing path's sort order
already meets a desired order, avoiding a `Sort` step.

### PlaceHolderInfo
The "global" part of a `PlaceHolderVar` (`pathnodes.h:3074`). It records
the lowest level (`ph_eval_at`) at which the placeholder can be
evaluated and the highest level (`ph_needed`) where it is referenced.
One PHI exists per distinct PHV expression in the query. See
[`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

### PlaceHolderVar (PHV)
A wrapper expression (`pathnodes.h:2780`) created when a non-strict
expression must be evaluated below an outer join but referenced above
it. The PHV "carries" the expression up the tree and keeps the
not-yet-nullable form available where needed. PHV state lives in
`PlannerInfo.placeholder_list` and is also indexed by `phid`.

### Plan
The executable plan-tree node defined in `plannodes.h:119`. The output
of the planner; consumed by the executor. Each Path subtype has a
counterpart Plan node — see
[Appendix: Path Quick Reference](./appendix_path_quick_reference.md).

### PlannerGlobal
Per-planner-run state (`pathnodes.h:96`) shared across query levels:
flat rangetable, list of subplans, parallel-mode flags, plan
invalidation items. Pointed to by `PlannerInfo.glob`.

### PlannerInfo (`root`)
Per-Query state (`pathnodes.h:195`, with the typedef at line 220). The
single most-passed-around argument in the planner; conventionally
named `root`. Holds every working data structure: `simple_rel_array`,
`join_rel_list`, `eq_classes`, `placeholder_list`, `init_plans`,
`upper_rels[...]`, etc. See
[`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md).

### Qual
Short for *qualification clause*. Any boolean expression in a WHERE,
ON, or HAVING that filters tuples. Quals are wrapped in `RestrictInfo`
nodes during qual distribution; see `distribute_qual_to_rels`
(`initsplan.c:2197`).

### RelOptInfo
The planner's representation of a relation (`pathnodes.h:853`).
`reloptkind` distinguishes baserel, joinrel, otherrel, upperrel, dead
rel, etc. Holds `pathlist` (candidate paths), `cheapest_total_path` and
friends, size estimates (`rows`, `tuples`, `pages`), `baserestrictinfo`,
`joininfo`, partitioning fields, and so on. See
[`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

### RestrictInfo (RI)
A wrapper around a qual clause (`pathnodes.h:2559`) that carries
planner-private metadata: the relids it references, evaluation cost,
selectivity estimates, mergejoinable / hashjoinable status, an
`EquivalenceClass` link (`parent_ec`) if the clause is EC-derived,
clone-tracking flags for outer-join identity 3, and so on. Build with
`make_restrictinfo` (`restrictinfo.c:63`). See
[`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md).

### Scan-qual
A `RestrictInfo` confined to a single base relation
(`clause_relids` is a singleton). Stored on
`RelOptInfo.baserestrictinfo` and applied by every scan path on that
rel.

### SpecialJoinInfo
The struct (`pathnodes.h:2891`) that captures the legality envelope of
one outer/semi/anti join: its jointype, the minimum relids required on
each side (`min_lefthand`/`min_righthand`), the syntactic relids
(`syn_lefthand`/`syn_righthand`), and which OJs commute above/below it.
Created by `make_outerjoininfo` (`initsplan.c:1360`); checked by
`join_is_legal` (`joinrels.c:350`).

### SubLink
A parsetree node representing a sub-SELECT in expression position
(EXISTS, IN, ANY/ALL/SOME, scalar subquery). The planner either pulls
it up to a join (`pull_up_sublinks`, `prepjointree.c:453`,
`convert_ANY_sublink_to_join`, `convert_EXISTS_sublink_to_join`) or
turns it into a `SubPlan` (`make_subplan`, `subselect.c:162`).

### SubPlan
An executor-time plan produced from a SubLink that could not be pulled
up. Stored in `PlannerInfo.glob->subplans`; correlated and uncorrelated
SubPlans are differentiated by their `parParam` and `args` fields. See
[`./12_subquery_and_sublink.md`](./12_subquery_and_sublink.md).

### Tlist (target list)
The list of `TargetEntry` nodes that defines a relation's output
columns. The Path-level analog is `PathTarget` (`pathnodes.h:1528`),
which stores expressions plus per-expression cost and width.

### varnullingrels
A `Relids` set on each `Var` (and `PlaceHolderVar`) that lists the
outer joins above which this Var becomes nullable. Introduced for the
identity-3 fix; used to keep equal Vars from different join contexts
distinct. See
[`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md).

### Partition pruning
Two flavors:

1. **Plan-time pruning** — `prune_append_rel_partitions`
   (`partprune.c:750`) eliminates child partitions whose
   constraint-implied range is disjoint from the qual.
2. **Run-time pruning** — `make_partition_pruneinfo`
   (`partprune.c:220`) attaches a `PartitionPruneInfo` to the plan so
   that the executor can prune again once parameter values are
   available. See
   [`./13_inheritance_and_partitioning.md`](./13_inheritance_and_partitioning.md).

---

## Cross-references back to documentation modules

| Term cluster | Primary module |
|---|---|
| `Path`, `PathKey`, paths quick reference | [Appendix: Path Quick Reference](./appendix_path_quick_reference.md) |
| `PlannerInfo`, `PlannerGlobal`, lifecycle | [`./03_lifecycle_and_entry_points.md`](./03_lifecycle_and_entry_points.md) |
| `RelOptInfo`, base rels, dummy rel | [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md) |
| EC, EM, broken EC, pathkey-contained-in | [`./10_equivalence_classes_and_pathkeys.md`](./10_equivalence_classes_and_pathkeys.md) |
| RestrictInfo, qual, joinclause, scan-qual | [`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md) |
| `SpecialJoinInfo`, identity 3, varnullingrels | [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md) |
| PlaceHolderVar, PlaceHolderInfo | [`./05_initial_setup_and_jointree.md`](./05_initial_setup_and_jointree.md) |
| ParamPathInfo, parameterized path, lateral | [`./11_restrictinfo_and_clause_utils.md`](./11_restrictinfo_and_clause_utils.md), [`./08_join_paths_and_search.md`](./08_join_paths_and_search.md) |
| AppendRelInfo, partition pruning | [`./13_inheritance_and_partitioning.md`](./13_inheritance_and_partitioning.md) |
| GEQO, geqo_threshold, chromosome | [`./15_geqo.md`](./15_geqo.md) |
| `cheapest_*_path`, cost knobs, fuzz factor | [`./09_cost_model_and_selectivity.md`](./09_cost_model_and_selectivity.md) |
| Parallel safety levels | [`./14_parallel_planning.md`](./14_parallel_planning.md) |
| tlist, junk column | [`./04_preprocessing.md`](./04_preprocessing.md), [`./16_plan_creation_and_setrefs.md`](./16_plan_creation_and_setrefs.md) |
| SubLink, SubPlan, InitPlan | [`./12_subquery_and_sublink.md`](./12_subquery_and_sublink.md) |
