# PostgreSQL Planner — Stage 2 Documentation Outline

This outline is derived from Stage 1 dependency analysis
(`architecture_map.json`, `path_type_inventory.txt`,
`key_symbols.txt`) and from `src/backend/optimizer/README`. It
proposes a Stage 2 component structure that mirrors the 15
functional areas identified in the prompt and the planner's
actual call graph.

Estimated total Stage 2 size: 14 component files + 1 cross-cutting
"glossary" file, roughly **75–95 KB of Markdown** in total
(approx. 9,000–11,000 lines), plus diagrams.

---

## 1. Top-level structure (proposed file layout under `stage2/`)

| # | File | Functional area | Coverage depth | Est. size |
|---|------|-----------------|----------------|-----------|
| 01 | `01_entry_and_lifecycle.md` | ENTRY_AND_LIFECYCLE | Deep | 8–10 KB |
| 02 | `02_preprocessing.md` | PREPROCESSING | Deep | 6–8 KB |
| 03 | `03_initial_query_setup.md` | INITIAL_QUERY_SETUP | Deep | 6–8 KB |
| 04 | `04_base_path_generation.md` | BASE_PATH_GENERATION | Deep | 7–9 KB |
| 05 | `05_index_path_generation.md` | INDEX_PATH_GENERATION | Deep | 6–8 KB |
| 06 | `06_join_path_generation.md` | JOIN_PATH_GENERATION | Very deep | 9–12 KB |
| 07 | `07_cost_and_selectivity.md` | COST_AND_SELECTIVITY | Deep | 7–9 KB |
| 08 | `08_equivalence_and_pathkeys.md` | EQUIVALENCE_AND_PATHKEYS | Deep | 6–8 KB |
| 09 | `09_qual_and_clause_util.md` | QUAL_AND_CLAUSE_UTIL | Medium | 4–6 KB |
| 10 | `10_subquery_and_transformations.md` | SUBQUERY_AND_TRANSFORMATIONS | Medium | 5–7 KB |
| 11 | `11_inherit_and_partition.md` | INHERIT_AND_PARTITION | Deep | 6–8 KB |
| 12 | `12_parallel_planning.md` | PARALLEL_PLANNING | Medium | 5–7 KB |
| 13 | `13_geqo.md` | GEQO | Medium | 4–6 KB |
| 14 | `14_plan_creation_and_final.md` | PLAN_CREATION_AND_FINAL | Deep | 7–9 KB |
| 15 | `15_hooks_and_extensions.md` | HOOKS_AND_EXTENSIONS | Shallow | 2–3 KB |
| 99 | `99_glossary_and_data_structures.md` | Cross-cutting | Reference | 5–7 KB |

"Coverage depth" guides how thoroughly each function/struct should
be documented (deep = full call graph + pseudo-code + diagrams;
shallow = high-level summary plus pointers).

---

## 2. Per-file content checklist

### 01_entry_and_lifecycle.md
- `planner()`, `standard_planner()` end-to-end (incl. `planner_hook`)
- `subquery_planner()` recursion model + return values
- `query_planner()` bridge (initsplan → make_one_rel)
- `grouping_planner()` upper-level pipeline
- Walkthrough of `create_grouping_paths`, `create_window_paths`,
  `create_distinct_paths`, `create_ordered_paths`,
  `fetch_upper_rel`
- Lifecycle diagram (Mermaid)

### 02_preprocessing.md
- `subquery_planner` preprocessing order: SS_process_ctes →
  pull_up_sublinks → reduce_outer_joins → pull_up_subqueries →
  flatten_simple_union_all → preprocess_expression →
  canonicalize_qual → preprocess_targetlist
- prepjointree.c: pull_up_sublinks_jointree_recurse, is_simple_subquery,
  pull_up_simple_union_all, replace_empty_jointree,
  transform_MERGE_to_join
- prepqual.c: canonicalize_qual / pull_ands / pull_ors /
  find_duplicate_ors
- prepagg.c: preprocess_aggrefs
- prepunion.c: setop tree planning
- GUC influences: from_collapse_limit, join_collapse_limit
- Diagram: preprocessing pipeline (Mermaid)

### 03_initial_query_setup.md
- build_base_rel_tlists / add_vars_to_targetlist
- find_lateral_references / extract_lateral_references /
  create_lateral_join_info
- deconstruct_jointree / deconstruct_recurse /
  deconstruct_distribute / make_outerjoininfo /
  compute_semijoin_info / deconstruct_distribute_oj_quals
- distribute_qual_to_rels: where each qual is anchored
- process_implied_equality / build_implied_join_equality
- Diagram: jointree → SpecialJoinInfo + RestrictInfo placement

### 04_base_path_generation.md
- set_base_rel_pathlists / set_rel_pathlist dispatch
- 9 branch handlers (set_plain_rel_pathlist,
  set_append_rel_pathlist, set_subquery_pathlist,
  set_function_pathlist, set_values_pathlist,
  set_cte_pathlist, set_namedtuplestore_pathlist,
  set_worktable_pathlist, set_result_pathlist,
  set_foreign_pathlist)
- create_seqscan_path, create_samplescan_path
- generate_useful_gather_paths interaction
- Diagram: rtekind dispatch table → constructors

### 05_index_path_generation.md
- create_index_paths flow, IndexOptInfo
- match_*_clauses_to_index family
- match_clause_to_indexcol (boolean / op / func / row-compare /
  scalar array op)
- build_index_paths, build_paths_for_OR
- choose_bitmap_and / generate_bitmap_or_paths /
  bitmap_and_cost_est / bitmap_scan_cost_est
- create_index_path / create_bitmap_heap_path
- ParamPathInfo and parameterized scans (paramassign.c)
- tidpath.c: create_tidscan_paths
- Diagram: bitmap path tree composition

### 06_join_path_generation.md  (the largest file)
- DP search (standard_join_search, join_search_one_level)
- make_join_rel + join_is_legal + SpecialJoinInfo gating
- populate_joinrel_with_paths dispatch by JoinType
- add_paths_to_joinrel
- try_nestloop_path / try_mergejoin_path / try_hashjoin_path
- sort_inner_and_outer / match_unsorted_outer /
  hash_inner_and_outer / generate_mergejoin_paths
- get_memoize_path
- set_cheapest / add_path / add_partial_path semantics
- Identity 1/2/3 handling, varnullingrels usage at this layer
- Diagrams: DP level expansion; join-method selection decision tree

### 07_cost_and_selectivity.md
- All cost_*() functions, organized by operator family
- initial_cost_* / final_cost_* split (why two phases)
- clauselist_selectivity / clauselist_selectivity_or /
  clause_selectivity / treat_as_join_clause
- selfuncs.c: examine_variable, get_variable_numdistinct, mcv,
  histograms
- GUC parameter table (seq_page_cost, random_page_cost,
  cpu_*_cost, parallel_*_cost, effective_cache_size, work_mem,
  enable_*)
- Diagram: cost-equation flow for nestloop / mergejoin / hashjoin

### 08_equivalence_and_pathkeys.md
- EquivalenceClass, EquivalenceMember
- process_equivalence, add_eq_member, merge_equiv_classes
- get_eclass_for_sort_expr
- generate_base_implied_equalities (const / no_const / broken)
- generate_join_implied_equalities (and _for_ecs / _normal /
  _broken)
- PathKey data model
- make_canonical_pathkey, make_pathkey_from_sortinfo,
  make_pathkeys_for_sortclauses
- build_index_pathkeys, build_join_pathkeys,
  build_expression_pathkey, build_partition_pathkeys
- pathkeys_contained_in, get_cheapest_path_for_pathkeys,
  pathkeys_count_contained_in
- Diagram: ECs from quals → PathKeys → sort/merge use

### 09_qual_and_clause_util.md
- RestrictInfo lifecycle and metadata
- make_restrictinfo, restriction_is_or_clause
- eval_const_expressions, contain_volatile_functions /
  contain_mutable_functions / contain_subplans
- find_nonnullable_rels / find_nonnullable_vars /
  find_forced_null_vars
- predicate_implied_by, predicate_refuted_by
- expression_returns_set, expression_returns_set_rows
- extract_or_clause / orclauses.c

### 10_subquery_and_transformations.md
- SubLink kinds and their conversion
- convert_ANY_sublink_to_join, convert_EXISTS_sublink_to_join,
  simplify_EXISTS_query, convert_EXISTS_to_ANY,
  convert_VALUES_to_ANY
- SS_process_ctes / SS_process_sublinks /
  SS_replace_correlation_vars / SS_assign_special_param /
  SS_finalize_plan
- InitPlan vs SubPlan distinction
- inline_cte
- planagg.c: preprocess_minmax_aggregates / build_minmax_path
- analyzejoins.c: remove_useless_joins / join_is_removable /
  reduce_unique_semijoins / rel_supports_distinctness
- Diagram: sublink → join transformation cases

### 11_inherit_and_partition.md
- inherit.c: expand_inherited_rtentry,
  expand_partitioned_rtentry, expand_single_inheritance_child,
  apply_child_basequals
- appendinfo.c: make_append_rel_info, adjust_appendrel_attrs,
  find_appinfos_by_relids
- allpaths.c: set_append_rel_size, set_append_rel_pathlist,
  add_paths_to_append_rel, generate_orderedappend_paths,
  get_cheapest_parameterized_child_path,
  accumulate_append_subpath
- partprune.c: make_partition_pruneinfo, gen_partprune_steps,
  prune_append_rel_partitions, get_matching_partitions,
  match_clause_to_partition_key
- joinrels.c: try_partitionwise_join, build_child_join_sjinfo,
  compute_partition_bounds, get_matching_part_pairs
- Partition-wise aggregate (create_partitionwise_grouping_paths)
- Diagram: parent-rel → AppendRel → partition pruning steps

### 12_parallel_planning.md
- compute_parallel_worker, max_parallel_hazard, is_parallel_safe,
  max_parallel_hazard_walker
- create_plain_partial_paths, create_partial_bitmap_paths
- try_partial_nestloop_path / try_partial_mergejoin_path /
  try_partial_hashjoin_path
- add_partial_path / add_partial_path_precheck
- generate_gather_paths, generate_useful_gather_paths
- cost_gather / cost_gather_merge
- Partial aggregation (AGGSPLIT_INITIAL_SERIAL,
  AGGSPLIT_FINAL_DESERIAL)
- Parallel HashJoin / Parallel Append details
- Diagram: serial-vs-parallel path layering

### 13_geqo.md
- geqo_threshold gating
- geqo() main loop + pool model (geqo_pool.c)
- gimme_pool_size, gimme_number_generations
- geqo_eval / gimme_tree / merge_clump
- Selection (geqo_selection.c) + recombination operators
  (geqo_cx, geqo_erx, geqo_ox1, geqo_ox2, geqo_pmx, geqo_px)
- Mutation (geqo_mutation.c)
- Random (geqo_random.c)
- GUC parameters: geqo, geqo_threshold, geqo_effort,
  geqo_pool_size, geqo_generations, geqo_selection_bias,
  geqo_seed
- Diagram: GEQO genome → join-tree decoding via merge_clump

### 14_plan_creation_and_final.md
- create_plan / create_plan_recurse big switch
- create_scan_plan dispatch (10 scan creators)
- create_join_plan (3 join creators) + create_gating_plan
- mark_async_capable_plan
- All 41+ create_*_plan functions tabulated
- setrefs.c: set_plan_references, set_plan_refs,
  add_rtes_to_flat_rtable, fix_scan_expr, set_join_references,
  set_upper_references, set_indexonlyscan_references,
  set_subqueryscan_references, set_foreignscan_references,
  set_param_references
- SS_finalize_plan: extParam/allParam computation
- Diagram: Path-tree → Plan-tree mapping table

### 15_hooks_and_extensions.md
- planner_hook, create_upper_paths_hook, join_search_hook
- set_rel_pathlist_hook, set_join_pathlist_hook
- get_relation_info_hook, get_index_stats_hook,
  get_attavgwidth_hook, get_relation_stats_hook
- FDW interaction (GetForeignPaths, GetForeignJoinPaths,
  GetForeignUpperPaths, GetForeignPlan)
- CustomScan provider interaction
- Real-world examples (pg_hint_plan, pg_stat_statements
  query-jumble interaction)

### 99_glossary_and_data_structures.md
- PlannerInfo, PlannerGlobal, RelOptInfo, IndexOptInfo
- Path (plus all 32 Path subtypes — links to
  `path_type_inventory.txt`)
- RestrictInfo, EquivalenceClass / EquivalenceMember, PathKey,
  PathTarget, ParamPathInfo, SpecialJoinInfo, AppendRelInfo,
  PlaceHolderVar, JoinDomain
- Plan / PlannedStmt and major Plan subtypes
- varnullingrels, phnullingrels, incompatible_relids semantics

---

## 3. Mermaid diagram targets (≥ 12 required)

| # | Diagram name | Type | File | Purpose |
|---|--------------|------|------|---------|
| D1 | Planner top-level lifecycle | flowchart | 01 | planner → standard_planner → ... → ExecutorStart |
| D2 | subquery_planner preprocessing pipeline | flowchart | 02 | linear preprocessing order |
| D3 | jointree decomposition | flowchart | 03 | deconstruct_jointree → SpecialJoinInfo / RestrictInfo placement |
| D4 | rtekind → set_*_pathlist dispatch | classDiagram or flowchart | 04 | base path generation routing |
| D5 | Bitmap path composition | flowchart | 05 | choose_bitmap_and / OR; BitmapAnd & BitmapOr trees |
| D6 | DP join search level expansion | flowchart | 06 | join_search_one_level over levels 2..N |
| D7 | Join-method decision tree | flowchart | 06 | nestloop / mergejoin / hashjoin selection per pair |
| D8 | Cost equation flow | flowchart | 07 | initial_cost vs final_cost two-phase costing |
| D9 | EquivalenceClass → PathKey derivation | flowchart | 08 | quals → ECs → PathKeys → sort/merge use |
| D10 | SubLink → join transformation cases | flowchart | 10 | ANY/EXISTS conversion gating |
| D11 | Partition-pruning step graph | flowchart | 11 | gen_partprune_steps → execution-time pruning |
| D12 | Parallel path layering | flowchart | 12 | partial_pathlist → Gather/GatherMerge insertion |
| D13 | GEQO genome decoding | flowchart | 13 | gimme_tree using merge_clump |
| D14 | Path → Plan conversion mapping | classDiagram or flowchart | 14 | pathnode tag → create_*_plan → plannode tag |
| D15 | Hooks call-out diagram | sequenceDiagram | 15 | extension entry points overlaid on lifecycle |

(Targeting 15 diagrams — 3 above the minimum — to cover both
top-level lifecycle and per-area zoom views.)

---

## 4. Cross-references and conventions

- Each component file should:
  1. Open with a short "Why this exists" paragraph rooted in
     `src/backend/optimizer/README` text.
  2. Provide a top-of-file table mapping the area's symbols to
     `architecture_map.json` entries (importance score + file:line).
  3. Use Mermaid for any structural diagram.
  4. Use Markdown tables for function-signature / GUC reference.
  5. Cross-link related areas (e.g., 06 ↔ 07, 06 ↔ 08, 04 ↔ 05).
  6. Cite `src/backend/optimizer/README` line ranges when
     restating identities or invariants.
- All file paths in citations must be absolute repo-relative:
  e.g., `src/backend/optimizer/path/joinpath.c:124`.
- Symbol importance is referenced from `key_symbols.txt` /
  `architecture_map.json` — Stage 2 should not invent new
  importance scores; it consumes Stage 1's.

---

## 5. Key paths for Stage 2 to follow

These are the named call chains from `architecture_map.json`'s
`critical_paths`. Stage 2 documentation must reproduce the
narrative for each, with the chain rendered as a Mermaid
flowchart in the relevant component file:

1. `planner_entry_path` — file 01
2. `preprocessing_path` — file 02
3. `base_path_generation_path` — file 04
4. `index_path_generation_path` — file 05
5. `join_search_path` — file 06
6. `geqo_path` — file 13
7. `cost_model_path` — file 07
8. `plan_creation_path` — file 14
9. `parallel_planning_path` — file 12
10. `partitionwise_path` — file 11
11. `equivalence_pathkey_path` — file 08
12. `subquery_transformation_path` — file 10

---

## 6. Stage 2 deliverables summary

```
stage2/
  README.md                                  (index linking 01..15 + 99)
  01_entry_and_lifecycle.md
  02_preprocessing.md
  03_initial_query_setup.md
  04_base_path_generation.md
  05_index_path_generation.md
  06_join_path_generation.md
  07_cost_and_selectivity.md
  08_equivalence_and_pathkeys.md
  09_qual_and_clause_util.md
  10_subquery_and_transformations.md
  11_inherit_and_partition.md
  12_parallel_planning.md
  13_geqo.md
  14_plan_creation_and_final.md
  15_hooks_and_extensions.md
  99_glossary_and_data_structures.md
  diagrams/                                  (rendered Mermaid sources, one .mmd per diagram)
```

Total estimated documentation footprint: ~95 KB Markdown +
15 Mermaid diagrams.
