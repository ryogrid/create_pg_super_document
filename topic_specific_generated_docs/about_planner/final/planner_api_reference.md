# Planner API Reference

This is a comprehensive list of public planner functions, grouped by
subsystem. Each entry shows the simplified signature, a one-line
description, and a `file:line` source pointer. Signatures are paraphrased
when full prototypes are long; consult the listed source line for the
authoritative version. Symbol coverage is taken from
`stage1/architecture_map.json`.

---

## Lifecycle / entry points

```c
PlannedStmt *planner(Query *parse, const char *query_string,
                     int cursorOptions, ParamListInfo boundParams);
```
Public entry point invoked by `pg_plan_query`. Calls `planner_hook` if
set, else `standard_planner`. — `src/backend/optimizer/plan/planner.c:275`.

```c
PlannedStmt *standard_planner(Query *parse, const char *query_string,
                              int cursorOptions, ParamListInfo boundParams);
```
Default implementation: sets up `PlannerGlobal`, calls `subquery_planner`,
finalizes the resulting Plan. — `planner.c:288`.

```c
PlannerInfo *subquery_planner(PlannerGlobal *glob, Query *parse,
                              PlannerInfo *parent_root,
                              bool hasRecursion, double tuple_fraction,
                              SetOperationStmt *setops);
```
Per-Query-level driver: preprocessing → query_planner → grouping_planner.
Recurses into child Querys. — `planner.c:629`.

```c
RelOptInfo *query_planner(PlannerInfo *root, query_pathkeys_callback qp_callback,
                          void *qp_extra);
```
First half of grouping_planner: builds baserel + joinrel paths,
returns the final joinrel. — `src/backend/optimizer/plan/planmain.c:53`.

```c
void grouping_planner(PlannerInfo *root, double tuple_fraction,
                      SetOperationStmt *setops);
```
Second half: builds upper-rel paths (group, window, distinct, order, final)
and stores them in `root->upper_rels[]`. — `planner.c:1335`.

```c
void create_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
                           PathTarget *target, bool target_parallel_safe,
                           grouping_sets_data *gd);
```
Adds GROUP BY / aggregate paths over the join-tree's output. — `planner.c:3820`.

```c
void create_window_paths(PlannerInfo *root, RelOptInfo *input_rel,
                         PathTarget *input_target, PathTarget *output_target,
                         bool output_target_parallel_safe,
                         List *tlist, WindowFuncLists *wflists,
                         List *activeWindows);
```
Adds WindowAgg paths. — `planner.c:4573`.

```c
void create_distinct_paths(PlannerInfo *root, RelOptInfo *input_rel,
                           PathTarget *target);
```
Adds DISTINCT paths to UPPERREL_DISTINCT. — `planner.c:4830`.

```c
void create_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
                          PathTarget *target, bool target_parallel_safe,
                          double limit_tuples);
```
Adds ORDER BY / cursor-friendly paths. — `planner.c:5306`.

```c
RelOptInfo *fetch_upper_rel(PlannerInfo *root, UpperRelationKind kind,
                            Relids relids);
```
Gets (creates if needed) the RelOptInfo for an upper-pipeline stage. —
`src/backend/optimizer/util/relnode.c`.

---

## Preprocessing

```c
List *pull_up_sublinks(PlannerInfo *root);
```
Recurses the jointree and converts EXISTS/IN/ANY SubLinks to joins.
— `src/backend/optimizer/prep/prepjointree.c:453`.

```c
Node *pull_up_subqueries(PlannerInfo *root);
```
Top-level subquery-pull-up driver. — `prepjointree.c:934`.

```c
Node *pull_up_simple_subquery(PlannerInfo *root, Node *jtnode,
                              RangeTblEntry *rte, JoinExpr *lowest_outer_join,
                              JoinExpr *containing_appendrel);
```
Flattens one simple subquery into the parent's FROM list. — `prepjointree.c:1123`.

```c
bool is_simple_subquery(PlannerInfo *root, Query *subquery, RangeTblEntry *rte,
                        JoinExpr *lowest_outer_join);
```
Tests pull-up eligibility. — `prepjointree.c:1659`.

```c
bool flatten_simple_union_all(PlannerInfo *root);
```
Pulls UNION ALL subqueries up via AppendRelInfos. — `prepjointree.c`.

```c
Node *reduce_outer_joins(PlannerInfo *root);
```
Demotes outer joins to inner when quals make the RHS-NULL case impossible.
— `prepjointree.c`.

```c
Expr *canonicalize_qual(Expr *qual, bool is_check);
```
Normalizes a boolean expression (push NOTs, flatten AND/OR). —
`src/backend/optimizer/prep/prepqual.c:293`.

```c
Node *eval_const_expressions(PlannerInfo *root, Node *node);
```
Constant-folds and inlines simple SQL functions. —
`src/backend/optimizer/util/clauses.c`.

```c
Node *preprocess_expression(PlannerInfo *root, Node *expr, int kind);
```
Full preprocessing pass over one expression (constants → SubLinks →
canonicalize). — `planner.c:1156`.

```c
List *preprocess_targetlist(PlannerInfo *root);
```
Adjusts targetlist for resjunk columns, RETURNING, etc. —
`src/backend/optimizer/prep/preptlist.c:64`.

```c
void preprocess_aggrefs(PlannerInfo *root, Node *clause);
```
Collects `Aggref` expressions and per-aggregate state. —
`src/backend/optimizer/prep/prepagg.c`.

---

## Initial query setup

```c
RelOptInfo *build_simple_rel(PlannerInfo *root, int relid, RelOptInfo *parent);
```
Creates and initializes a baserel RelOptInfo. — `relnode.c`.

```c
void build_base_rel_tlists(PlannerInfo *root, List *final_tlist);
```
Marks Vars referenced by the targetlist on each baserel's `attr_needed`. —
`src/backend/optimizer/plan/initsplan.c:234`.

```c
List *deconstruct_jointree(PlannerInfo *root);
```
Walks the FROM tree: computes relids, lateral state, SpecialJoinInfos. —
`initsplan.c:740`.

```c
void distribute_qual_to_rels(PlannerInfo *root, Node *clause, ...);
```
Wraps a qual in a RestrictInfo and stores it on the appropriate rel(s).
— `initsplan.c:2197`.

```c
SpecialJoinInfo *make_outerjoininfo(PlannerInfo *root,
                                    Relids left_rels, Relids right_rels,
                                    Relids inner_join_rels,
                                    JoinType jointype, Index ojrelid,
                                    List *clause);
```
Derives a SpecialJoinInfo for one outer/semi/anti join. — `initsplan.c:1360`.

```c
void find_lateral_references(PlannerInfo *root);
```
Populates per-rel `lateral_vars` / `lateral_referencers`. —
`initsplan.c:358`.

```c
void extract_lateral_references(PlannerInfo *root, RelOptInfo *brel,
                                Index rtindex);
```
Gathers Vars/PHVs that lateral-reference outer rels. — `initsplan.c:406`.

```c
void get_relation_info(PlannerInfo *root, Oid relationObjectId,
                       bool inhparent, RelOptInfo *rel);
```
Populates pg_class/pg_index info onto a baserel. —
`src/backend/optimizer/util/plancat.c`.

---

## Base-relation paths

```c
RelOptInfo *make_one_rel(PlannerInfo *root, List *joinlist);
```
Top-level driver: builds the single RelOptInfo for the entire FROM
clause. — `src/backend/optimizer/path/allpaths.c:171`.

```c
void set_base_rel_sizes(PlannerInfo *root);
```
Populates per-baserel `rows` estimates. — `allpaths.c:290`.

```c
void set_base_rel_pathlists(PlannerInfo *root);
```
Calls `set_rel_pathlist` for every baserel. — `allpaths.c:333`.

```c
void set_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti,
                  RangeTblEntry *rte);
```
Per-RTE-kind dispatcher that sets `rel->rows`. — `allpaths.c:360`.

```c
void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                      RangeTblEntry *rte);
```
Per-RTE-kind dispatcher to the matching `set_*_pathlist`. — `allpaths.c:469`.

```c
void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
                            RangeTblEntry *rte);
```
Generates paths for an ordinary RTE_RELATION. — `allpaths.c:764`.

```c
void set_subquery_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
                           RangeTblEntry *rte);
void set_function_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
void set_values_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
void set_cte_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
void set_worktable_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
void set_result_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
void set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
```
RTE-kind-specific path generators. — `allpaths.c:2482, 2749, 2816, 2860, 2993, 2966, 926`.

```c
Path *create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
                          Relids required_outer, int parallel_workers);
```
Constructs a SeqScan Path. — `src/backend/optimizer/util/pathnode.c:927`.

```c
void create_tidscan_paths(PlannerInfo *root, RelOptInfo *rel);
```
TID/TID-range scan path generator. — `src/backend/optimizer/path/tidpath.c:364`.

---

## Index paths

```c
void create_index_paths(PlannerInfo *root, RelOptInfo *rel);
```
Top-level driver: enumerates all index paths for a base rel. —
`src/backend/optimizer/path/indxpath.c:234`.

```c
List *build_index_paths(PlannerInfo *root, RelOptInfo *rel,
                        IndexOptInfo *index, IndexClauseSet *clauses,
                        bool useful_predicate, ScanTypeControl scantype,
                        bool *skip_nonnative_saop);
```
Generates IndexPaths from a single index for given matching clauses.
— `indxpath.c:804`.

```c
List *build_paths_for_OR(PlannerInfo *root, RelOptInfo *rel,
                         List *clauses, List *other_clauses);
```
Generates index paths for OR clauses with intra-relation arms. —
`indxpath.c:1086`.

```c
List *generate_bitmap_or_paths(PlannerInfo *root, RelOptInfo *rel,
                               List *clauses, List *other_clauses);
```
Produces BitmapOrPaths for OR-clauses with index-friendly arms. —
`indxpath.c:1180`.

```c
Path *choose_bitmap_and(PlannerInfo *root, RelOptInfo *rel, List *paths);
```
Chooses the best subset of bitmap index paths to AND together. —
`indxpath.c:1287`.

```c
bool match_clause_to_indexcol(PlannerInfo *root, IndexClause **iclause,
                              RestrictInfo *rinfo, IndexOptInfo *index,
                              int indexcol);
```
Tests whether a RestrictInfo is usable as a qual on a given index column.
— `indxpath.c:2203`.

```c
IndexPath *create_index_path(PlannerInfo *root, IndexOptInfo *index,
                             List *indexclauses, List *indexorderbys,
                             List *indexorderbycols, List *pathkeys,
                             ScanDirection indexscandir,
                             bool indexonly, Relids required_outer,
                             double loop_count, bool partial_path);
```
Constructs an IndexPath from a chosen index. — `pathnode.c:993`.

```c
BitmapHeapPath *create_bitmap_heap_path(PlannerInfo *root, RelOptInfo *rel,
                                        Path *bitmapqual, Relids required_outer,
                                        double loop_count, int parallel_degree);
```
Wraps a bitmap-tree subpath as a `BitmapHeapPath`. — `pathnode.c:1042`.

---

## Join paths and search

```c
RelOptInfo *make_rel_from_joinlist(PlannerInfo *root, List *joinlist);
```
Dispatches to standard_join_search or GEQO depending on size. —
`allpaths.c:3306`.

```c
RelOptInfo *standard_join_search(PlannerInfo *root, int levels_needed,
                                 List *initial_rels);
```
Default DP join-search algorithm. — `allpaths.c:3411`.

```c
void join_search_one_level(PlannerInfo *root, int level);
```
DP step: build all joinrels of size `level` from those of size `level-1`. —
`src/backend/optimizer/path/joinrels.c:73`.

```c
void make_rels_by_clause_joins(PlannerInfo *root, RelOptInfo *old_rel,
                               List *other_rels);
void make_rels_by_clauseless_joins(PlannerInfo *root, RelOptInfo *old_rel,
                                   List *other_rels);
```
Generate join candidates by clause-driven and Cartesian pairing. —
`joinrels.c:280, 314`.

```c
bool join_is_legal(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2,
                   Relids joinrelids, SpecialJoinInfo **sjinfo_p,
                   bool *reversed_p);
```
SpecialJoinInfo legality check for forming a candidate join. —
`joinrels.c:350`.

```c
RelOptInfo *make_join_rel(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2);
```
Joinrel-creation driver: legality + populate_joinrel_with_paths. —
`joinrels.c:705`.

```c
void populate_joinrel_with_paths(PlannerInfo *root, RelOptInfo *rel1,
                                 RelOptInfo *rel2, RelOptInfo *joinrel,
                                 SpecialJoinInfo *sjinfo, List *restrictlist);
```
Calls add_paths_to_joinrel and try_partitionwise_join. — `joinrels.c:894`.

```c
void add_paths_to_joinrel(PlannerInfo *root, RelOptInfo *joinrel,
                          RelOptInfo *outerrel, RelOptInfo *innerrel,
                          JoinType jointype, SpecialJoinInfo *sjinfo,
                          List *restrictlist);
```
Entry point that explores join algorithms. — `joinpath.c:124`.

```c
void try_nestloop_path(PlannerInfo *root, RelOptInfo *joinrel,
                       Path *outer_path, Path *inner_path,
                       List *pathkeys, JoinType jointype,
                       JoinPathExtraData *extra);
void try_mergejoin_path(...);
void try_hashjoin_path(...);
```
Add the corresponding join Path if costing survives screening. —
`joinpath.c:721, 920, 1096`.

```c
Path *get_memoize_path(PlannerInfo *root, RelOptInfo *innerrel,
                       RelOptInfo *outerrel, Path *inner_path);
```
Wraps an inner-side parameterized path in a MemoizePath. —
`joinpath.c:581`.

```c
void sort_inner_and_outer(...);
void match_unsorted_outer(...);
void hash_inner_and_outer(...);
```
Per-strategy mergejoin/hashjoin path generators. — `joinpath.c:1266, 1717, 2093`.

```c
List *select_mergejoin_clauses(PlannerInfo *root, RelOptInfo *joinrel,
                               RelOptInfo *outerrel, RelOptInfo *innerrel,
                               List *restrictlist, JoinType jointype,
                               bool *mergejoin_allowed);
```
Selects RestrictInfos suitable as merge clauses. — `joinpath.c:2347`.

```c
RelOptInfo *build_join_rel(PlannerInfo *root, Relids joinrelids,
                           RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                           SpecialJoinInfo *sjinfo, List **restrictlist_ptr);
```
Returns (creating if needed) the joinrel for two child rel sets. —
`relnode.c`.

```c
void add_path(RelOptInfo *parent_rel, Path *new_path);
void set_cheapest(RelOptInfo *parent_rel);
```
Path Pareto-frontier management. — `pathnode.c:420, 242`.

```c
bool is_dummy_rel(RelOptInfo *rel);
```
True if the rel is provably empty. — `joinrels.c:1333`.

---

## Cost model and selectivity

```c
void cost_seqscan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
                  ParamPathInfo *param_info);
void cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
                bool partial_path);
void cost_bitmap_heap_scan(...);
void cost_subqueryscan(...);
```
Per-scan cost functions. — `costsize.c:284, 549, 1013, 1451`.

```c
void cost_sort(Path *path, PlannerInfo *root, List *pathkeys,
               Cost input_cost, double tuples, int width,
               Cost comparison_cost, int sort_mem, double limit_tuples);
void cost_incremental_sort(...);
void cost_agg(...);
void cost_memoize_rescan(...);
void cost_subplan(...);
```
Upper-rel cost functions. — `costsize.c:2124, 1986, 2650, 2509, 4435`.

```c
void cost_gather(GatherPath *path, PlannerInfo *root, RelOptInfo *rel,
                 ParamPathInfo *param_info, double *rows);
void cost_gather_merge(...);
```
Parallel cost functions. — `costsize.c:436, 474`.

```c
void initial_cost_nestloop(PlannerInfo *root, JoinCostWorkspace *workspace, ...);
void final_cost_nestloop(PlannerInfo *root, NestPath *path,
                         JoinCostWorkspace *workspace, JoinPathExtraData *extra);
void initial_cost_mergejoin(...);
void final_cost_mergejoin(...);
void initial_cost_hashjoin(...);
void final_cost_hashjoin(...);
```
Two-stage join cost. — `costsize.c:3233, 3308, 3514, 3745, 4073, 4181`.

```c
void cost_qual_eval(QualCost *cost, List *quals, PlannerInfo *root);
```
Total CPU cost to evaluate a list of quals. — `costsize.c:4643`.

```c
Selectivity clauselist_selectivity(PlannerInfo *root, List *clauses,
                                   int varRelid, JoinType jointype,
                                   SpecialJoinInfo *sjinfo);
Selectivity clauselist_selectivity_or(PlannerInfo *root, List *clauses, ...);
Selectivity clause_selectivity(PlannerInfo *root, Node *clause, ...);
```
Selectivity drivers. — `clausesel.c:100, 359, 667`.

```c
void examine_variable(PlannerInfo *root, Node *node, int varRelid,
                      VariableStatData *vardata);
```
Looks up `pg_statistic` for a Var. — `selfuncs.c`.

```c
int compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor);
```
Fuzzy three-way path comparison used by `add_path`. — `pathnode.c:164`.

---

## Equivalence classes / pathkeys

```c
bool process_equivalence(PlannerInfo *root, RestrictInfo **p_restrictinfo,
                         JoinDomain *jdomain);
```
Promotes a mergejoinable equality clause into an EC. — `equivclass.c:117`.

```c
EquivalenceClass *get_eclass_for_sort_expr(PlannerInfo *root, Expr *expr,
                                           List *opfamilies, Oid opcintype,
                                           Oid collation, Index sortref,
                                           Relids rel, bool create_it);
```
Returns an EC for an expression that should appear in a sort key. —
`equivclass.c:586`.

```c
void generate_base_implied_equalities(PlannerInfo *root);
List *generate_join_implied_equalities(PlannerInfo *root, Relids join_relids,
                                       Relids outer_relids,
                                       RelOptInfo *inner_rel,
                                       SpecialJoinInfo *sjinfo);
```
Emit per-rel and per-join EC-derived clauses. — `equivclass.c:1028, 1376`.

```c
PathKey *make_canonical_pathkey(PlannerInfo *root, EquivalenceClass *eclass,
                                Oid opfamily, int strategy, bool nulls_first);
PathKey *make_pathkey_from_sortinfo(PlannerInfo *root, Expr *expr,
                                    Oid opfamily, Oid opcintype,
                                    Oid collation, bool reverse_sort,
                                    bool nulls_first, Index sortref,
                                    Relids rel, bool create_it);
```
Build canonical pathkeys. — `pathkeys.c:55, 197`.

```c
List *make_pathkeys_for_sortclauses(PlannerInfo *root, List *sortclauses,
                                    List *tlist);
List *build_index_pathkeys(PlannerInfo *root, IndexOptInfo *index,
                           ScanDirection scandir);
List *build_join_pathkeys(PlannerInfo *root, RelOptInfo *joinrel,
                          JoinType jointype, List *outer_pathkeys);
```
Pathkey generators. — `pathkeys.c:1330, 738, 1292`.

```c
bool pathkeys_contained_in(List *keys1, List *keys2);
Path *get_cheapest_path_for_pathkeys(List *paths, List *pathkeys,
                                     Relids required_outer,
                                     CostSelector cost_criterion,
                                     bool require_parallel_safe);
```
Pathkey query helpers. — `pathkeys.c:341, 618`.

```c
void process_implied_equality(PlannerInfo *root, Oid opno, Oid collation,
                              Expr *item1, Expr *item2, ...);
```
Adds a derived equality clause as a RestrictInfo on its rel(s). —
`initsplan.c:2961`.

---

## RestrictInfo and clause utilities

```c
RestrictInfo *make_restrictinfo(PlannerInfo *root, Expr *clause,
                                bool is_pushed_down, bool has_clone,
                                bool is_clone, bool pseudoconstant,
                                Index security_level,
                                Relids required_relids, Relids incompatible_relids,
                                Relids outer_relids);
```
Wraps a clause expression in a RestrictInfo with all caches initialized. —
`src/backend/optimizer/util/restrictinfo.c:63`.

```c
bool contain_subplans(Node *clause);
bool contain_mutable_functions(Node *clause);
bool contain_volatile_functions(Node *clause);
```
Expression classification. — `clauses.c:330, 370, 538`.

```c
Relids find_nonnullable_rels(Node *clause);
```
Relids known to produce no NULL output for a strict expression. —
`src/backend/optimizer/util/var.c`.

```c
bool predicate_implied_by(List *predicate_list, List *clause_list,
                          bool weak);
bool predicate_refuted_by(List *predicate_list, List *clause_list,
                          bool weak);
```
Predicate logic for partial-index matching and pruning. —
`src/backend/optimizer/util/predtest.c:152, 222`.

```c
List *extract_or_clause(RestrictInfo *or_rinfo, RelOptInfo *rel);
```
Extracts a per-relation index-able qual from a multi-relation OR. —
`src/backend/optimizer/util/orclauses.c`.

---

## Subquery, SubLink, and analyzejoins

```c
Node *make_subplan(PlannerInfo *root, Query *orig_subquery,
                   SubLinkType subLinkType, int subLinkId, Node *testexpr,
                   bool isTopQual);
SubPlan *build_subplan(PlannerInfo *root, Plan *plan, PlannerInfo *subroot,
                       List *plan_params, SubLinkType subLinkType,
                       int subLinkId, Node *testexpr, List *testexpr_paramids,
                       bool unknownEqFalse);
```
SubLink → SubPlan transformation. — `subselect.c:162, 319`.

```c
void SS_process_ctes(PlannerInfo *root);
```
Builds InitPlans for CTEs. — `subselect.c:880`.

```c
JoinExpr *convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                                      Relids available_rels);
JoinExpr *convert_EXISTS_sublink_to_join(PlannerInfo *root, SubLink *sublink,
                                         bool under_not, Relids available_rels);
```
Pull SubLinks up as semijoins. — `subselect.c:1254, 1371`.

```c
void preprocess_minmax_aggregates(PlannerInfo *root);
Path *build_minmax_path(PlannerInfo *root, MinMaxAggInfo *mminfo,
                        Oid eqop, Oid sortop, bool nulls_first);
```
MIN/MAX-from-index optimization. — `src/backend/optimizer/plan/planagg.c:72, 316`.

```c
List *remove_useless_joins(PlannerInfo *root, List *joinlist);
void reduce_unique_semijoins(PlannerInfo *root);
```
Post-deconstruct join simplifications. —
`src/backend/optimizer/plan/analyzejoins.c:64, 730`.

---

## Inheritance and partitioning

```c
void expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
                              RangeTblEntry *parentrte, Index parentRTindex);
void expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
                                RangeTblEntry *parentrte, Index parentRTindex,
                                Relation parentrel,
                                PlanRowMark *top_parentrc, LOCKMODE lockmode);
```
Expand inheritance / partition trees into AppendRelInfos. —
`src/backend/optimizer/util/inherit.c:86, 318`.

```c
AppendRelInfo *make_append_rel_info(Relation parentrel, Relation childrel,
                                    Index parentRTindex, Index childRTindex);
Node *adjust_appendrel_attrs(PlannerInfo *root, Node *node, int nappinfos,
                             AppendRelInfo **appinfos);
AppendRelInfo **find_appinfos_by_relids(PlannerInfo *root, Relids relids,
                                        int *nappinfos);
```
AppendRelInfo construction and translation utilities. —
`src/backend/optimizer/util/appendinfo.c:51, 196, 733`.

```c
void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
                             Index rti, RangeTblEntry *rte);
void add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel,
                             List *live_childrels);
```
Append/MergeAppend path generation. — `allpaths.c:1232, 1302`.

```c
void try_partitionwise_join(PlannerInfo *root, RelOptInfo *rel1,
                            RelOptInfo *rel2, RelOptInfo *joinrel,
                            SpecialJoinInfo *sjinfo, List *restrictlist);
void generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel);
```
Partitionwise-join path generation. — `joinrels.c:1479`, `allpaths.c:4291`.

```c
void set_plan_references_in_partitionwise_join(...);
```
Var-reference fixing for partitionwise-join plans. — `joinrels.c:1694`.

```c
PartitionPruneInfo *make_partition_pruneinfo(PlannerInfo *root,
                                             RelOptInfo *parentrel,
                                             List *subpaths,
                                             List *partitioned_rels,
                                             List *prunequal);
PartitionPruneStep *gen_partprune_steps(...);
List *prune_append_rel_partitions(RelOptInfo *rel);
```
Partition-pruning helpers. —
`src/backend/partitioning/partprune.c:220, 714, 750`.

---

## Parallel planning

```c
char max_parallel_hazard(Query *parse);
bool is_parallel_safe(PlannerInfo *root, Node *node);
```
Parallel-safety checks. — `clauses.c:734, 753`.

```c
int compute_parallel_worker(RelOptInfo *rel, double heap_pages,
                            double index_pages, int max_workers);
```
Picks a worker count for a partial path. — `allpaths.c:4203`.

```c
void create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel);
void create_partial_bitmap_paths(PlannerInfo *root, RelOptInfo *rel,
                                 Path *bitmapqual);
void generate_gather_paths(PlannerInfo *root, RelOptInfo *rel,
                           bool override_rows);
void generate_useful_gather_paths(PlannerInfo *root, RelOptInfo *rel,
                                  bool override_rows);
```
Partial path generators and Gather wrappers. — `allpaths.c:794, 4167, 3052, 3190`.

```c
void try_partial_nestloop_path(...);
void try_partial_mergejoin_path(...);
void try_partial_hashjoin_path(...);
```
Parallel-aware versions of the three try_*_path helpers. —
`joinpath.c:843, 1026, 1173`.

```c
void add_partial_path(RelOptInfo *parent_rel, Path *new_path);
```
Pareto-frontier management for partial paths. — `pathnode.c:747`.

---

## GEQO

```c
RelOptInfo *geqo(PlannerInfo *root, int number_of_rels, List *initial_rels);
Cost geqo_eval(PlannerInfo *root, Gene *tour, int num_gene);
RelOptInfo *gimme_tree(PlannerInfo *root, Gene *tour, int num_gene);
List *merge_clump(PlannerInfo *root, List *clumps, Clump *new_clump,
                  int num_gene, bool force);
```
GEQO entry, fitness, joinrel-build, and clump-merging helpers. —
`src/backend/optimizer/geqo/geqo_main.c:72`,
`geqo_eval.c:57, 163, 238`.

---

## Plan creation

```c
Plan *create_plan(PlannerInfo *root, Path *best_path);
Plan *create_plan_recurse(PlannerInfo *root, Path *best_path, int flags);
```
Top-level Path → Plan converter. — `createplan.c:338, 389`.

```c
Plan *create_scan_plan(PlannerInfo *root, Path *best_path, int flags);
Plan *create_join_plan(PlannerInfo *root, JoinPath *best_path);
Plan *create_append_plan(PlannerInfo *root, AppendPath *best_path, int flags);
Scan *create_indexscan_plan(PlannerInfo *root, IndexPath *best_path, ...);
Scan *create_bitmap_scan_plan(PlannerInfo *root, BitmapHeapPath *best_path, ...);
NestLoop *create_nestloop_plan(PlannerInfo *root, NestPath *best_path);
MergeJoin *create_mergejoin_plan(PlannerInfo *root, MergePath *best_path);
HashJoin *create_hashjoin_plan(PlannerInfo *root, HashPath *best_path);
ModifyTable *create_modifytable_plan(PlannerInfo *root, ModifyTablePath *best_path);
```
Per-Path-subtype plan creators. — `createplan.c:560, 1082, 1217, 3006, 3202, 4348, 4440, 4747, 2815`.

(See [Appendix: Path Quick Reference](./appendix_path_quick_reference.md)
for the full mapping of Path → plan creator.)

---

## setrefs.c

```c
Plan *set_plan_references(PlannerInfo *root, Plan *plan);
Plan *set_plan_refs(PlannerInfo *root, Plan *plan, int rtoffset);
void set_join_references(PlannerInfo *root, Join *join, int rtoffset);
void set_upper_references(PlannerInfo *root, Plan *plan, int rtoffset);
```
Var-reference flattening across the finished Plan tree. —
`src/backend/optimizer/plan/setrefs.c:287, 608, 2282, 2431`.

```c
void SS_finalize_plan(PlannerInfo *root, Plan *plan);
```
Finds required Params and fills `extParam`/`allParam` on each Plan. —
`subselect.c:2900`.

---

## Hooks

```c
extern PGDLLIMPORT planner_hook_type planner_hook;
extern PGDLLIMPORT join_search_hook_type join_search_hook;
extern PGDLLIMPORT set_rel_pathlist_hook_type set_rel_pathlist_hook;
extern PGDLLIMPORT set_join_pathlist_hook_type set_join_pathlist_hook;
extern PGDLLIMPORT create_upper_paths_hook_type create_upper_paths_hook;
extern PGDLLIMPORT get_relation_info_hook_type get_relation_info_hook;
```
Public hook globals. — `src/include/optimizer/planner.h:30, 38`,
`src/include/optimizer/paths.h:34, 43, 49`,
`src/include/optimizer/plancat.h`.

See [`./17_hooks_and_extensibility.md`](./17_hooks_and_extensibility.md) for
worked examples and hook ordering.

---

For full struct field references, see
[Appendix: Data Structures](./appendix_data_structures.md). For each
GUC mentioned above, see
[Appendix: GUC Parameters](./appendix_guc_parameters.md).
