# Appendix: Symbol Index

This is an alphabetical index of every planner symbol that appears in
this documentation set. Entries are derived from
`stage1/architecture_map.json` (207 symbols total) and
`stage1/key_symbols.txt`. Each entry has the form:

> **`<symbol>`** — *one-line description* · `<file>:<line>` · [→ module]

Entries are grouped by their first letter (case-insensitive). Module
links point to the most relevant file under `final/`.

> Tip: a symbol whose definition is not at a precise line shows
> `<file>:0` because the source-map extractor flagged it as
> "definition not pinpointed" (typically a struct in a header that uses
> a forward typedef, or a GUC declared via `DefineCustomXxxVariable`).
> The file path is still authoritative.

---

## A

- **`add_partial_path`** — appends a partial Path to `RelOptInfo.partial_pathlist`, with cost-based de-duplication. · `src/backend/optimizer/util/pathnode.c:747` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`add_path`** — adds a Path to a RelOptInfo's pathlist, dropping dominated alternatives. · `src/backend/optimizer/util/pathnode.c:420` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`add_paths_to_append_rel`** — generates Append/MergeAppend paths for an inheritance/partition parent. · `src/backend/optimizer/path/allpaths.c:1302` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`add_paths_to_joinrel`** — entry point that explores join algorithms for a pair of relations. · `src/backend/optimizer/path/joinpath.c:124` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`adjust_appendrel_attrs`** — translates expressions from a parent relation into the namespace of one child. · `src/backend/optimizer/util/appendinfo.c:196` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`AppendRelInfo`** — relates an inheritance/partition parent or UNION ALL parent to one child. · `src/include/nodes/pathnodes.h:0` · [→ Appendix: Data Structures](./appendix_data_structures.md#appendrelinfo)

## B

- **`build_base_rel_tlists`** — adds Vars referenced by the targetlist to per-baserel `attr_needed`. · `src/backend/optimizer/plan/initsplan.c:234` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`build_index_pathkeys`** — produces a list of PathKeys for a path that scans an index in order. · `src/backend/optimizer/path/pathkeys.c:738` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`build_index_paths`** — generates IndexPaths from a single index, considering all matching clauses. · `src/backend/optimizer/path/indxpath.c:804` · [→ 07_index_paths](./07_index_paths.md)
- **`build_join_pathkeys`** — derives the pathkeys of a join from its inputs and join type. · `src/backend/optimizer/path/pathkeys.c:1292` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`build_join_rel`** — returns (creating if needed) the joinrel RelOptInfo for two child rel sets. · `src/backend/optimizer/util/relnode.c:0` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`build_minmax_path`** — builds an IndexScan + LIMIT 1 path used by the MIN/MAX optimization. · `src/backend/optimizer/plan/planagg.c:316` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`build_paths_for_OR`** — generates index paths for the branches of an OR clause that share a relation. · `src/backend/optimizer/path/indxpath.c:1086` · [→ 07_index_paths](./07_index_paths.md)
- **`build_simple_rel`** — creates and initializes a baserel RelOptInfo for a single RTE. · `src/backend/optimizer/util/relnode.c:0` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`build_subplan`** — converts a SubLink into a SubPlan/AlternativeSubPlan node. · `src/backend/optimizer/plan/subselect.c:319` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)

## C

- **`canonicalize_qual`** — normalizes a boolean expression (push NOTs down, flatten AND/OR). · `src/backend/optimizer/prep/prepqual.c:293` · [→ 04_preprocessing](./04_preprocessing.md)
- **`choose_bitmap_and`** — chooses the best subset of bitmap index paths to AND together. · `src/backend/optimizer/path/indxpath.c:1287` · [→ 07_index_paths](./07_index_paths.md)
- **`clause_selectivity`** — selectivity of a single clause; recurses into AND/OR/NOT. · `src/backend/optimizer/path/clausesel.c:667` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`clauselist_selectivity`** — selectivity of an implicit-AND list of clauses. · `src/backend/optimizer/path/clausesel.c:100` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`clauselist_selectivity_or`** — selectivity of an OR-list of clauses. · `src/backend/optimizer/path/clausesel.c:359` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`compare_path_costs_fuzzily`** — fuzzy three-way path comparison used by `add_path`. · `src/backend/optimizer/util/pathnode.c:164` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`compute_parallel_worker`** — picks a worker count for a partial path based on table size. · `src/backend/optimizer/path/allpaths.c:4203` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`contain_mutable_functions`** — true if an expression contains a mutable function call. · `src/backend/optimizer/util/clauses.c:370` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`contain_subplans`** — true if an expression contains a SubPlan node. · `src/backend/optimizer/util/clauses.c:330` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`contain_volatile_functions`** — true if an expression has a volatile function. · `src/backend/optimizer/util/clauses.c:538` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`convert_ANY_sublink_to_join`** — pulls an ANY/IN SubLink up as a SEMI join. · `src/backend/optimizer/plan/subselect.c:1254` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`convert_EXISTS_sublink_to_join`** — pulls an EXISTS SubLink up as a SEMI/ANTI join. · `src/backend/optimizer/plan/subselect.c:1371` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`cost_agg`** — costs an Agg node (PLAIN/SORTED/HASHED/MIXED). · `src/backend/optimizer/path/costsize.c:2650` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_bitmap_heap_scan`** — costs a BitmapHeapScan over a bitmap index sub-tree. · `src/backend/optimizer/path/costsize.c:1013` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_gather`** — costs a Gather: leader overhead + parallel_tuple_cost*rows. · `src/backend/optimizer/path/costsize.c:436` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`cost_gather_merge`** — costs a GatherMerge: cost_gather + per-tuple merge step. · `src/backend/optimizer/path/costsize.c:474` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`cost_incremental_sort`** — costs an IncrementalSort given the number of presorted columns. · `src/backend/optimizer/path/costsize.c:1986` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_index`** — costs an IndexScan / IndexOnlyScan including correlation effects. · `src/backend/optimizer/path/costsize.c:549` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_memoize_rescan`** — costs a Memoize rescan based on cache hit ratio estimate. · `src/backend/optimizer/path/costsize.c:2509` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_qual_eval`** — total CPU cost to evaluate a list of quals. · `src/backend/optimizer/path/costsize.c:4643` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_seqscan`** — costs a SeqScan: pages*seq_page_cost + tuples*cpu_tuple_cost. · `src/backend/optimizer/path/costsize.c:284` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_sort`** — costs an explicit Sort, including spill if input > work_mem. · `src/backend/optimizer/path/costsize.c:2124` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_subplan`** — costs a SubPlan and adds it to its referencing expression. · `src/backend/optimizer/path/costsize.c:4435` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cost_subqueryscan`** — costs a SubqueryScan; mostly the cost of the subpath. · `src/backend/optimizer/path/costsize.c:1451` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`cpu_index_tuple_cost`** — GUC: per-tuple CPU cost for index entries examined. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#cpu_index_tuple_cost)
- **`cpu_operator_cost`** — GUC: per-operator-call CPU cost. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#cpu_operator_cost)
- **`cpu_tuple_cost`** — GUC: per-tuple CPU cost for any tuple processed. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#cpu_tuple_cost)
- **`create_append_plan`** — Path → Plan for AppendPath. · `src/backend/optimizer/plan/createplan.c:1217` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_bitmap_heap_path`** — wraps a bitmap-tree subpath as a `BitmapHeapPath`. · `src/backend/optimizer/util/pathnode.c:1042` · [→ 07_index_paths](./07_index_paths.md)
- **`create_bitmap_scan_plan`** — Path → Plan for BitmapHeapPath (recursing into BitmapAnd/Or). · `src/backend/optimizer/plan/createplan.c:3202` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_distinct_paths`** — adds DISTINCT-step paths to the post-grouping upper rels. · `src/backend/optimizer/plan/planner.c:4830` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`create_grouping_paths`** — adds GROUP BY / aggregate paths over the join tree. · `src/backend/optimizer/plan/planner.c:3820` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`create_hashjoin_plan`** — Path → Plan for HashPath. · `src/backend/optimizer/plan/createplan.c:4747` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_index_path`** — constructs an `IndexPath` from a chosen index and matching clauses. · `src/backend/optimizer/util/pathnode.c:993` · [→ 07_index_paths](./07_index_paths.md)
- **`create_index_paths`** — top-level driver: enumerates all index paths for a base rel. · `src/backend/optimizer/path/indxpath.c:234` · [→ 07_index_paths](./07_index_paths.md)
- **`create_indexscan_plan`** — Path → Plan for IndexPath (regular or index-only). · `src/backend/optimizer/plan/createplan.c:3006` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_join_plan`** — dispatches to nestloop/merge/hash creators for any JoinPath. · `src/backend/optimizer/plan/createplan.c:1082` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_mergejoin_plan`** — Path → Plan for MergePath; inserts implicit Sort/Material as needed. · `src/backend/optimizer/plan/createplan.c:4440` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_modifytable_plan`** — Path → Plan for ModifyTablePath (INSERT/UPDATE/DELETE/MERGE). · `src/backend/optimizer/plan/createplan.c:2815` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_nestloop_plan`** — Path → Plan for NestPath. · `src/backend/optimizer/plan/createplan.c:4348` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_ordered_paths`** — adds ORDER BY / cursor-friendly paths to UPPERREL_ORDERED. · `src/backend/optimizer/plan/planner.c:5306` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`create_partial_bitmap_paths`** — generates parallel-aware bitmap paths from regular bitmap paths. · `src/backend/optimizer/path/allpaths.c:4167` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`create_plain_partial_paths`** — generates parallel partial seq-scan paths for a baserel. · `src/backend/optimizer/path/allpaths.c:794` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`create_plan`** — top-level Path → Plan converter; called once per query. · `src/backend/optimizer/plan/createplan.c:338` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_plan_recurse`** — main switch on path->pathtype that drives all *_plan creators. · `src/backend/optimizer/plan/createplan.c:389` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_scan_plan`** — Path → Plan dispatcher for all scan paths. · `src/backend/optimizer/plan/createplan.c:560` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`create_seqscan_path`** — constructs a SeqScan Path; cost via `cost_seqscan`. · `src/backend/optimizer/util/pathnode.c:927` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`create_tidscan_paths`** — entry point that generates TidPath / TidRangePath candidates. · `src/backend/optimizer/path/tidpath.c:364` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`create_upper_paths_hook`** — extension hook called after each upper-pipeline stage. · `src/include/optimizer/planner.h:38` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`create_window_paths`** — adds WindowAgg paths for queries with window functions. · `src/backend/optimizer/plan/planner.c:4573` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)

## D

- **`deconstruct_jointree`** — walks the FROM tree, computing relids, lateral state, and SpecialJoinInfos. · `src/backend/optimizer/plan/initsplan.c:740` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`distribute_qual_to_rels`** — wraps a qual in a RestrictInfo and stores it on the appropriate rel(s). · `src/backend/optimizer/plan/initsplan.c:2197` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)

## E

- **`effective_cache_size`** — GUC: assumed OS+shared-buffer cache size in pages. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#effective_cache_size)
- **`enable_bitmapscan`** — GUC: when false, adds disable_cost to BitmapHeapPath. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_hashjoin`** — GUC: when false, discourages HashPath. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_indexscan`** — GUC: when false, suppresses IndexPath. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_memoize`** — GUC: when false, suppresses MemoizePath. · `src/backend/optimizer/path/joinpath.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_mergejoin`** — GUC: when false, discourages MergePath. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_nestloop`** — GUC: when false, discourages NestPath. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_partitionwise_join`** — GUC: when true, enables partitionwise join consideration. · `src/backend/optimizer/path/joinrels.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`enable_seqscan`** — GUC: when false, adds disable_cost to SeqScan. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#enable-flags)
- **`EquivalenceClass`** — set of expressions known transitively equal under the same opfamily. · `src/include/nodes/pathnodes.h:1356` · [→ Appendix: Data Structures](./appendix_data_structures.md#equivalenceclass)
- **`eval_const_expressions`** — folds constants and inlines simple SQL functions in an expression tree. · `src/backend/optimizer/util/clauses.c:0` · [→ 04_preprocessing](./04_preprocessing.md)
- **`examine_variable`** — looks up pg_statistic stats for a Var; used by selectivity estimation. · `src/backend/utils/adt/selfuncs.c:0` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`expand_inherited_rtentry`** — expands an inheritance parent RTE into AppendRelInfos for children. · `src/backend/optimizer/util/inherit.c:86` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`expand_partitioned_rtentry`** — recursive expansion for partitioned tables (multi-level). · `src/backend/optimizer/util/inherit.c:318` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`extract_lateral_references`** — gathers Vars/PHVs that lateral-reference outer rels. · `src/backend/optimizer/plan/initsplan.c:406` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`extract_or_clause`** — extracts a per-relation index-able qual from a multi-relation OR. · `src/backend/optimizer/util/orclauses.c:0` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)

## F

- **`fetch_upper_rel`** — gets (creating if needed) the RelOptInfo for an upper-pipeline stage. · `src/backend/optimizer/util/relnode.c:0` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`final_cost_hashjoin`** — full hashjoin cost after add_paths_to_joinrel chooses to keep the path. · `src/backend/optimizer/path/costsize.c:4181` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`final_cost_mergejoin`** — full mergejoin cost; called after `initial_cost_mergejoin` survives screening. · `src/backend/optimizer/path/costsize.c:3745` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`final_cost_nestloop`** — full nestloop cost. · `src/backend/optimizer/path/costsize.c:3308` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`find_appinfos_by_relids`** — locates AppendRelInfos for a child relid set. · `src/backend/optimizer/util/appendinfo.c:733` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`find_lateral_references`** — populates per-rel lateral_vars/lateral_referencers. · `src/backend/optimizer/plan/initsplan.c:358` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`find_nonnullable_rels`** — relids known to produce no NULL output rows for a strict expression. · `src/backend/optimizer/util/var.c:0` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`flatten_simple_union_all`** — pulls UNION ALL subqueries up via AppendRelInfos. · `src/backend/optimizer/prep/prepjointree.c:0` · [→ 04_preprocessing](./04_preprocessing.md)
- **`from_collapse_limit`** — GUC: maximum FROM-list size after subquery pull-up. · `src/backend/optimizer/prep/prepjointree.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#from_collapse_limit)

## G

- **`generate_base_implied_equalities`** — emits implied baserel quals derived from each EC. · `src/backend/optimizer/path/equivclass.c:1028` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`generate_bitmap_or_paths`** — produces BitmapOrPaths for OR-clauses with index-friendly arms. · `src/backend/optimizer/path/indxpath.c:1180` · [→ 07_index_paths](./07_index_paths.md)
- **`generate_gather_paths`** — wraps each partial path with a Gather to make it usable. · `src/backend/optimizer/path/allpaths.c:3052` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`generate_join_implied_equalities`** — emits implied joinclauses derived from each EC at join time. · `src/backend/optimizer/path/equivclass.c:1376` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`generate_partitionwise_join_paths`** — generates per-partition join paths and combines them into an Append. · `src/backend/optimizer/path/allpaths.c:4291` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`generate_useful_gather_paths`** — also tries Gather+IncrementalSort and GatherMerge variants. · `src/backend/optimizer/path/allpaths.c:3190` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`gen_partprune_steps`** — turns prunable quals into PartitionPruneSteps for runtime pruning. · `src/backend/partitioning/partprune.c:714` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`geqo`** — GEQO entry point; runs the genetic algorithm and returns the best joinrel found. · `src/backend/optimizer/geqo/geqo_main.c:72` · [→ 15_geqo](./15_geqo.md)
- **`geqo_eval`** — fitness function: builds a joinrel for one chromosome and reads its cheapest cost. · `src/backend/optimizer/geqo/geqo_eval.c:57` · [→ 15_geqo](./15_geqo.md)
- **`geqo_threshold`** — GUC: number of relations beyond which GEQO is used. · `src/backend/optimizer/path/allpaths.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#geqo_threshold)
- **`get_cheapest_path_for_pathkeys`** — finds the cheapest Path matching a desired pathkey order. · `src/backend/optimizer/path/pathkeys.c:618` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`get_eclass_for_sort_expr`** — returns an EC for an expression that should appear in a sort key. · `src/backend/optimizer/path/equivclass.c:586` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`get_memoize_path`** — wraps an inner-side parameterized path in a MemoizePath when worthwhile. · `src/backend/optimizer/path/joinpath.c:581` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`get_relation_info`** — populates pg_class/pg_index info onto a baserel RelOptInfo. · `src/backend/optimizer/util/plancat.c:0` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`get_relation_info_hook`** — extension hook to add synthetic indexes / statistics. · `src/include/optimizer/plancat.h:0` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`gimme_tree`** — GEQO helper: builds a joinrel tree from a permutation by greedily merging clumps. · `src/backend/optimizer/geqo/geqo_eval.c:163` · [→ 15_geqo](./15_geqo.md)
- **`grouping_planner`** — second-half driver of subquery_planner: scan/join/upper paths. · `src/backend/optimizer/plan/planner.c:1335` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)

## H

- **`hash_inner_and_outer`** — produces hashjoin paths with each side as the hash side, when applicable. · `src/backend/optimizer/path/joinpath.c:2093` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)

## I

- **`IndexOptInfo`** — per-index planner state (catalog data + planner-derived flags). · `src/include/nodes/pathnodes.h:0` · [→ 07_index_paths](./07_index_paths.md)
- **`initial_cost_hashjoin`** — preliminary hashjoin cost (fast screen). · `src/backend/optimizer/path/costsize.c:4073` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`initial_cost_mergejoin`** — preliminary mergejoin cost (fast screen). · `src/backend/optimizer/path/costsize.c:3514` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`initial_cost_nestloop`** — preliminary nestloop cost (fast screen). · `src/backend/optimizer/path/costsize.c:3233` · [→ 09_cost_model_and_selectivity](./09_cost_model_and_selectivity.md)
- **`is_dummy_rel`** — true if the rel has only a dummy AppendPath (provably empty). · `src/backend/optimizer/path/joinrels.c:1333` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`is_parallel_safe`** — recurses an expression checking max_parallel_hazard. · `src/backend/optimizer/util/clauses.c:753` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`is_simple_subquery`** — true if a subquery may be flattened into its parent. · `src/backend/optimizer/prep/prepjointree.c:1659` · [→ 04_preprocessing](./04_preprocessing.md)

## J

- **`join_collapse_limit`** — GUC: maximum FROM-list size after JOIN flattening. · `src/backend/optimizer/prep/prepjointree.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#join_collapse_limit)
- **`join_is_legal`** — checks SpecialJoinInfo legality of forming a candidate join. · `src/backend/optimizer/path/joinrels.c:350` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`join_search_hook`** — extension hook replacing standard_join_search (e.g., GEQO). · `src/include/optimizer/paths.h:49` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`join_search_one_level`** — DP search step: build all joinrels of size N from ones of size N-1. · `src/backend/optimizer/path/joinrels.c:73` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)

## M

- **`make_append_rel_info`** — constructs an AppendRelInfo for a parent/child pair. · `src/backend/optimizer/util/appendinfo.c:51` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`make_canonical_pathkey`** — interns a (eclass, opfamily, strategy, nulls_first) PathKey. · `src/backend/optimizer/path/pathkeys.c:55` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`make_join_rel`** — drives joinrel creation: legality check + add_paths_to_joinrel. · `src/backend/optimizer/path/joinrels.c:705` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`make_one_rel`** — entry point: builds the single RelOptInfo for the entire FROM clause. · `src/backend/optimizer/path/allpaths.c:171` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`make_outerjoininfo`** — derives a SpecialJoinInfo for one outer/semi/anti join. · `src/backend/optimizer/plan/initsplan.c:1360` · [→ 05_initial_setup_and_jointree](./05_initial_setup_and_jointree.md)
- **`make_partition_pruneinfo`** — emits a PartitionPruneInfo for runtime pruning. · `src/backend/partitioning/partprune.c:220` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`make_pathkey_from_sortinfo`** — builds a PathKey for a sortgroupclause / index column. · `src/backend/optimizer/path/pathkeys.c:197` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`make_pathkeys_for_sortclauses`** — turns a query's ORDER BY into a list of PathKeys. · `src/backend/optimizer/path/pathkeys.c:1330` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`make_rel_from_joinlist`** — top-level join driver: dispatches to DP search or GEQO. · `src/backend/optimizer/path/allpaths.c:3306` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`make_rels_by_clause_joins`** — pairs an outer rel with rels mentioned by its joinclauses. · `src/backend/optimizer/path/joinrels.c:280` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`make_rels_by_clauseless_joins`** — Cartesian-product join generation. · `src/backend/optimizer/path/joinrels.c:314` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`make_restrictinfo`** — wraps a clause expression in a RestrictInfo with all caches initialized. · `src/backend/optimizer/util/restrictinfo.c:63` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`make_subplan`** — turns a SubLink into a SubPlan/InitPlan. · `src/backend/optimizer/plan/subselect.c:162` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`match_clause_to_indexcol`** — checks whether a RestrictInfo is usable as a qual on a given index column. · `src/backend/optimizer/path/indxpath.c:2203` · [→ 07_index_paths](./07_index_paths.md)
- **`match_unsorted_outer`** — try-all-the-mergejoin-orderings for one outer path. · `src/backend/optimizer/path/joinpath.c:1717` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`max_parallel_hazard`** — three-valued parallel-safety check on an expression. · `src/backend/optimizer/util/clauses.c:734` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`max_parallel_workers_per_gather`** — GUC: cap on workers per Gather. · `src/backend/optimizer/path/allpaths.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#max_parallel_workers_per_gather)
- **`merge_clump`** — GEQO helper: merges adjacent baserels in a chromosome into joinrels. · `src/backend/optimizer/geqo/geqo_eval.c:238` · [→ 15_geqo](./15_geqo.md)

## P

- **`Path`** — base class for every algebraic plan node, polymorphic via `pathtype`. · `src/include/nodes/pathnodes.h:1621` · [→ Appendix: Data Structures](./appendix_data_structures.md#path)
- **`PathKey`** — one column of a sort ordering, referencing an EquivalenceClass. · `src/include/nodes/pathnodes.h:1463` · [→ Appendix: Data Structures](./appendix_data_structures.md#pathkey)
- **`pathkeys_contained_in`** — true if pathkey list L is a prefix-superset of R. · `src/backend/optimizer/path/pathkeys.c:341` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`PlaceHolderVar`** — wrapper expression used to delay evaluation across outer joins. · `src/include/nodes/pathnodes.h:0` · [→ Appendix: Data Structures](./appendix_data_structures.md#placeholdervar)
- **`Plan`** — base class for every executable plan node. · `src/include/nodes/plannodes.h:119` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`PlannedStmt`** — the top-level executable plan tree returned to the executor. · `src/include/nodes/plannodes.h:46` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`planner`** — public entry point invoked by `pg_plan_query`. · `src/backend/optimizer/plan/planner.c:275` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`PlannerGlobal`** — per-planner-run shared state. · `src/include/nodes/pathnodes.h:96` · [→ Appendix: Data Structures](./appendix_data_structures.md#plannerglobal)
- **`PlannerInfo`** — per-Query state (the ubiquitous `root` argument). · `src/include/nodes/pathnodes.h:220` · [→ Appendix: Data Structures](./appendix_data_structures.md#plannerinfo-root)
- **`planner_hook`** — extension hook replacing the entire planner. · `src/include/optimizer/planner.h:30` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`populate_joinrel_with_paths`** — invokes add_paths_to_joinrel and tries partitionwise join. · `src/backend/optimizer/path/joinrels.c:894` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`predicate_implied_by`** — true if predicate A logically implies predicate B. · `src/backend/optimizer/util/predtest.c:152` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`predicate_refuted_by`** — true if predicate A logically refutes predicate B. · `src/backend/optimizer/util/predtest.c:222` · [→ 11_restrictinfo_and_clause_utils](./11_restrictinfo_and_clause_utils.md)
- **`preprocess_aggrefs`** — collects Aggref expressions and computes per-aggregate planner state. · `src/backend/optimizer/prep/prepagg.c:0` · [→ 04_preprocessing](./04_preprocessing.md)
- **`preprocess_expression`** — full preprocessing pass on one expression (constants, SubLinks, etc.). · `src/backend/optimizer/plan/planner.c:1156` · [→ 04_preprocessing](./04_preprocessing.md)
- **`preprocess_minmax_aggregates`** — detects MIN/MAX-from-index optimization opportunities. · `src/backend/optimizer/plan/planagg.c:72` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`preprocess_targetlist`** — adjusts the targetlist for junk columns, returning columns, etc. · `src/backend/optimizer/prep/preptlist.c:64` · [→ 04_preprocessing](./04_preprocessing.md)
- **`process_equivalence`** — promotes a mergejoinable equality clause into an EC. · `src/backend/optimizer/path/equivclass.c:117` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`process_implied_equality`** — adds a derived equality clause as a RestrictInfo on its rel(s). · `src/backend/optimizer/plan/initsplan.c:2961` · [→ 10_equivalence_classes_and_pathkeys](./10_equivalence_classes_and_pathkeys.md)
- **`prune_append_rel_partitions`** — plan-time partition pruning. · `src/backend/partitioning/partprune.c:750` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`pull_up_simple_subquery`** — flattens a subquery into the parent FROM list. · `src/backend/optimizer/prep/prepjointree.c:1123` · [→ 04_preprocessing](./04_preprocessing.md)
- **`pull_up_sublinks`** — recurses the jointree converting SubLinks to joins where possible. · `src/backend/optimizer/prep/prepjointree.c:453` · [→ 04_preprocessing](./04_preprocessing.md)
- **`pull_up_subqueries`** — top-level subquery pull-up driver. · `src/backend/optimizer/prep/prepjointree.c:934` · [→ 04_preprocessing](./04_preprocessing.md)

## Q

- **`query_planner`** — first-half driver of grouping_planner: builds baserel and join paths. · `src/backend/optimizer/plan/planmain.c:53` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)

## R

- **`random_page_cost`** — GUC: cost of fetching a random 8 kB page. · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#random_page_cost)
- **`reduce_outer_joins`** — converts outer joins to inner joins when the qual makes RHS-NULL impossible. · `src/backend/optimizer/prep/prepjointree.c:0` · [→ 04_preprocessing](./04_preprocessing.md)
- **`reduce_unique_semijoins`** — converts SEMI joins on a unique inner side to plain inner joins. · `src/backend/optimizer/plan/analyzejoins.c:730` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`RelOptInfo`** — planner's view of a relation (base, join, upper, other, dead). · `src/include/nodes/pathnodes.h:853` · [→ Appendix: Data Structures](./appendix_data_structures.md#reloptinfo)
- **`remove_useless_joins`** — drops joins that the planner can prove don't restrict or duplicate. · `src/backend/optimizer/plan/analyzejoins.c:64` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`RestrictInfo`** — wrapper around a qual carrying planner-private metadata. · `src/include/nodes/pathnodes.h:0` · [→ Appendix: Data Structures](./appendix_data_structures.md#restrictinfo)

## S

- **`select_mergejoin_clauses`** — selects RestrictInfos suitable as merge clauses for a join. · `src/backend/optimizer/path/joinpath.c:2347` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`seq_page_cost`** — GUC: cost of fetching a sequential 8 kB page (the unit). · `src/backend/optimizer/path/costsize.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#seq_page_cost)
- **`set_append_rel_pathlist`** — drives Append/MergeAppend path generation for an inheritance parent. · `src/backend/optimizer/path/allpaths.c:1232` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)
- **`set_base_rel_pathlists`** — calls `set_rel_pathlist` for every baserel. · `src/backend/optimizer/path/allpaths.c:333` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_base_rel_sizes`** — populates per-baserel `rows` estimates. · `src/backend/optimizer/path/allpaths.c:290` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_cheapest`** — selects cheapest_total / cheapest_startup / cheapest_unique / cheapest_parameterized paths. · `src/backend/optimizer/util/pathnode.c:242` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`set_cte_pathlist`** — generates paths for an RTE_CTE rel. · `src/backend/optimizer/path/allpaths.c:2860` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_foreign_pathlist`** — generates paths for an RTE_RELATION with FDW. · `src/backend/optimizer/path/allpaths.c:926` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_function_pathlist`** — generates paths for an RTE_FUNCTION (SRF). · `src/backend/optimizer/path/allpaths.c:2749` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_join_pathlist_hook`** — extension hook called inside add_paths_to_joinrel. · `src/include/optimizer/paths.h:43` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`set_join_references`** — fixes Var references inside a join Plan tree. · `src/backend/optimizer/plan/setrefs.c:2282` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`set_plain_rel_pathlist`** — generates paths for an ordinary RTE_RELATION (heap). · `src/backend/optimizer/path/allpaths.c:764` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_plan_references`** — flattens rangetable refs throughout the finished Plan tree. · `src/backend/optimizer/plan/setrefs.c:287` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`set_plan_references_in_partitionwise_join`** — partitionwise variant called during plan ref fixing. · `src/backend/optimizer/path/joinrels.c:1694` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`set_plan_refs`** — recursive worker for set_plan_references. · `src/backend/optimizer/plan/setrefs.c:608` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`set_rel_pathlist`** — switch dispatcher to the per-RTE-kind set_*_pathlist. · `src/backend/optimizer/path/allpaths.c:469` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_rel_pathlist_hook`** — extension hook called after each rel's paths are generated. · `src/include/optimizer/paths.h:34` · [→ 17_hooks_and_extensibility](./17_hooks_and_extensibility.md)
- **`set_rel_size`** — per-RTE-kind dispatcher that sets `rel->rows`. · `src/backend/optimizer/path/allpaths.c:360` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_result_pathlist`** — generates paths for an RTE_RESULT rel (no real input). · `src/backend/optimizer/path/allpaths.c:2966` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_subquery_pathlist`** — recurses into a subquery and generates paths for its RTE. · `src/backend/optimizer/path/allpaths.c:2482` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_upper_references`** — fixes Var references inside upper-rel Plan nodes. · `src/backend/optimizer/plan/setrefs.c:2431` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`set_values_pathlist`** — generates paths for an RTE_VALUES rel. · `src/backend/optimizer/path/allpaths.c:2816` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`set_worktable_pathlist`** — generates paths for an RTE_CTE recursive worktable. · `src/backend/optimizer/path/allpaths.c:2993` · [→ 06_base_relation_paths](./06_base_relation_paths.md)
- **`sort_inner_and_outer`** — explicit-sort merge join variant: sorts both sides into mergeable order. · `src/backend/optimizer/path/joinpath.c:1266` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`SpecialJoinInfo`** — legality envelope of one outer/semi/anti join. · `src/include/nodes/pathnodes.h:0` · [→ Appendix: Data Structures](./appendix_data_structures.md#specialjoininfo)
- **`SS_finalize_plan`** — walks the finished Plan tree to find required Params, fix `extParam`/`allParam`. · `src/backend/optimizer/plan/subselect.c:2900` · [→ 16_plan_creation_and_setrefs](./16_plan_creation_and_setrefs.md)
- **`SS_process_ctes`** — builds InitPlans for CTEs (used for non-recursive, non-MOD WITH). · `src/backend/optimizer/plan/subselect.c:880` · [→ 12_subquery_and_sublink](./12_subquery_and_sublink.md)
- **`standard_join_search`** — DP join-search algorithm; default `join_search_hook`. · `src/backend/optimizer/path/allpaths.c:3411` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`standard_planner`** — default implementation behind `planner_hook`. · `src/backend/optimizer/plan/planner.c:288` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)
- **`subquery_planner`** — recursive planner entry per query level. · `src/backend/optimizer/plan/planner.c:629` · [→ 03_lifecycle_and_entry_points](./03_lifecycle_and_entry_points.md)

## T

- **`try_hashjoin_path`** — adds a HashPath if costing survives initial screening. · `src/backend/optimizer/path/joinpath.c:1096` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`try_mergejoin_path`** — adds a MergePath if costing survives initial screening. · `src/backend/optimizer/path/joinpath.c:920` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`try_nestloop_path`** — adds a NestPath if legality and cost screening succeed. · `src/backend/optimizer/path/joinpath.c:721` · [→ 08_join_paths_and_search](./08_join_paths_and_search.md)
- **`try_partial_hashjoin_path`** — parallel-aware version of try_hashjoin_path (partial inner). · `src/backend/optimizer/path/joinpath.c:1173` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`try_partial_mergejoin_path`** — parallel-aware version of try_mergejoin_path. · `src/backend/optimizer/path/joinpath.c:1026` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`try_partial_nestloop_path`** — parallel-aware version of try_nestloop_path. · `src/backend/optimizer/path/joinpath.c:843` · [→ 14_parallel_planning](./14_parallel_planning.md)
- **`try_partitionwise_join`** — generates a join of two partitioned rels as an Append of per-partition joins. · `src/backend/optimizer/path/joinrels.c:1479` · [→ 13_inheritance_and_partitioning](./13_inheritance_and_partitioning.md)

## W

- **`work_mem`** — GUC: per-operation working-memory budget (kB). · `src/backend/utils/misc/guc.c:0` · [→ Appendix: GUCs](./appendix_guc_parameters.md#work_mem)
