# PostgreSQL Planner Super-Document

> A self-contained reference to the PostgreSQL query planner / optimizer,
> assembled from Stage 1 dependency analysis and Stage 2 component
> documentation. Sources cited live under `src/backend/optimizer/` and
> `src/include/nodes/pathnodes.h` of the PostgreSQL repository (this
> document targets the current `master` branch).

The planner sits between the parser/rewriter and the executor. Given a
`Query` tree, it produces a `PlannedStmt` whose `Plan` tree the
executor walks. Internally the planner runs a **two-stage pipeline**:
first it builds a search space of `Path` objects (cost-annotated access
strategies) and prunes them with Pareto-style dominance; then it
converts the cheapest surviving `Path` tree into the executor-facing
`Plan` tree. This duality — Path during search, Plan after — keeps the
search cheap (Paths share substructure) while the final Plan is
self-contained (no shared substructure across siblings). The whole
machinery is driven by a cost model fed by `pg_statistic` data and
tuned by GUC parameters.

---

## 1. Reading paths

This book is large. Pick a route based on your goal.

### Newcomer route (linear)
Start at [01_executive_summary.md](./01_executive_summary.md) and read
modules in order: 01 → 02 → 03 → ... → 20. The numbering follows the
runtime order of the planner pipeline (entry → preprocessing → setup
→ paths → joins → costs → upper rels → plan creation → extras).

### Reference route (random access)
- Need a Path subtype? [18_path_catalog.md](./18_path_catalog.md)
- Need a `create_*_plan` lookup? [19_plan_creator_catalog.md](./19_plan_creator_catalog.md)
- Symbol or struct lookup? [appendix_symbol_index.md](./appendix_symbol_index.md), [appendix_data_structures.md](./appendix_data_structures.md)
- GUC reference? [appendix_guc_parameters.md](./appendix_guc_parameters.md)
- Quick refresher? [planner_quick_reference.md](./planner_quick_reference.md)

### Deep-dive route (specific topic)
- Outer-join legality + identity-3 clones: [05_initial_setup_and_jointree.md](./05_initial_setup_and_jointree.md), then [20_deep_dives.md](./20_deep_dives.md)
- DP search internals: [08_join_paths_and_search.md](./08_join_paths_and_search.md), then [20_deep_dives.md](./20_deep_dives.md)
- Selectivity / cost model accuracy: [09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md)
- Equivalence classes / pathkeys: [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md)
- Plan → executor handoff: [16_plan_creation_and_setrefs.md](./16_plan_creation_and_setrefs.md)

---

## 2. Table of contents

### Main book — numbered modules

| # | File | Topic |
|---|------|-------|
| 01 | [01_executive_summary.md](./01_executive_summary.md) | What the planner does and why it's structured the way it is |
| 02 | [02_architecture_overview.md](./02_architecture_overview.md) | End-to-end pipeline, sub-systems, and data flow |
| 03 | [03_lifecycle_and_entry_points.md](./03_lifecycle_and_entry_points.md) | `planner` / `standard_planner` / `subquery_planner` / `query_planner` / `grouping_planner` |
| 04 | [04_preprocessing.md](./04_preprocessing.md) | Sublink/subquery pull-up, qual canonicalization, set-op planning |
| 05 | [05_initial_setup_and_jointree.md](./05_initial_setup_and_jointree.md) | Jointree decomposition, `SpecialJoinInfo`, `RestrictInfo` placement, lateral |
| 06 | [06_base_relation_paths.md](./06_base_relation_paths.md) | `set_rel_pathlist` dispatch and per-RTE-kind path generators |
| 07 | [07_index_paths.md](./07_index_paths.md) | `create_index_paths`, bitmap paths, parameterized index scans |
| 08 | [08_join_paths_and_search.md](./08_join_paths_and_search.md) | DP search, three join methods, `add_path` Pareto pruning |
| 09 | [09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md) | `costsize.c`, selectivity estimators, `pg_statistic`, GUCs |
| 10 | [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md) | EC machinery, pathkeys, sort-order canonicalization |
| 11 | [11_restrictinfo_and_clause_utils.md](./11_restrictinfo_and_clause_utils.md) | `RestrictInfo`, predicate implication, `eval_const_expressions` |
| 12 | [12_subquery_and_sublink.md](./12_subquery_and_sublink.md) | SubLink kinds, `SS_process_*`, useless-join removal |
| 13 | [13_inheritance_and_partitioning.md](./13_inheritance_and_partitioning.md) | AppendRel, partition pruning, partitionwise join |
| 14 | [14_parallel_planning.md](./14_parallel_planning.md) | Partial paths, `Gather` / `GatherMerge`, parallel-safety |
| 15 | [15_geqo.md](./15_geqo.md) | Genetic-query optimizer for large join searches |
| 16 | [16_plan_creation_and_setrefs.md](./16_plan_creation_and_setrefs.md) | `create_plan_recurse`, `set_plan_references`, `SS_finalize_plan` |
| 17 | [17_hooks_and_extensibility.md](./17_hooks_and_extensibility.md) | `planner_hook`, `set_rel_pathlist_hook`, FDW, CustomScan |
| 18 | [18_path_catalog.md](./18_path_catalog.md) | All `Path` subtypes with constructors and cost functions |
| 19 | [19_plan_creator_catalog.md](./19_plan_creator_catalog.md) | All `create_*_plan` functions tabulated |
| 20 | [20_deep_dives.md](./20_deep_dives.md) | Algorithmic deep-dives: identity-3, DP complexity, broken ECs |

### Appendices

| File | Topic |
|------|-------|
| [appendix_symbol_index.md](./appendix_symbol_index.md) | Alphabetical index of every documented function/struct/macro |
| [appendix_glossary.md](./appendix_glossary.md) | Definitions of planner-specific terminology |
| [appendix_data_structures.md](./appendix_data_structures.md) | Field-level reference for `PlannerInfo`, `RelOptInfo`, `Path`, `Plan`, etc. |
| [appendix_path_quick_reference.md](./appendix_path_quick_reference.md) | One-page summary of all 32 `Path` subtypes |
| [appendix_guc_parameters.md](./appendix_guc_parameters.md) | Every GUC the planner reads, with defaults and effects |

### Supplementary

| File | Topic |
|------|-------|
| [planner_quick_reference.md](./planner_quick_reference.md) | Two-page condensed cheat sheet |
| [planner_api_reference.md](./planner_api_reference.md) | Function signatures (the planner's public-ish C API) |
| [quality_report.md](./quality_report.md) | Coverage metrics and known gaps |

---

## 3. Diagrams

All diagrams live under [`../diagrams/`](../diagrams/) as Mermaid sources.
They are also embedded inline in the modules listed below.

| # | Diagram | Embedded in |
|---|---------|-------------|
| 01 | `01_planner_pipeline.mermaid` | [02_architecture_overview.md](./02_architecture_overview.md) |
| 02 | `02_object_model.mermaid` | [appendix_data_structures.md](./appendix_data_structures.md) |
| 03 | `03_dp_join_search.mermaid` | [08_join_paths_and_search.md](./08_join_paths_and_search.md) |
| 04 | `04_geqo_main_loop.mermaid` | [15_geqo.md](./15_geqo.md) |
| 05 | `05_path_to_plan_map.mermaid` | [16_plan_creation_and_setrefs.md](./16_plan_creation_and_setrefs.md) |
| 06 | `06_eclass_derivation.mermaid` | [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md) |
| 07 | `07_pathkey_propagation.mermaid` | [10_equivalence_classes_and_pathkeys.md](./10_equivalence_classes_and_pathkeys.md) |
| 08 | `08_specialjoininfo_legality.mermaid` | [05_initial_setup_and_jointree.md](./05_initial_setup_and_jointree.md) |
| 09 | `09_parallel_path_gen.mermaid` | [14_parallel_planning.md](./14_parallel_planning.md) |
| 10 | `10_subquery_handling_decision.mermaid` | [12_subquery_and_sublink.md](./12_subquery_and_sublink.md) |
| 11 | `11_partition_pruning_plan_time.mermaid` | [13_inheritance_and_partitioning.md](./13_inheritance_and_partitioning.md) |
| 12 | `12_join_cost_decomposition.mermaid` | [08_join_paths_and_search.md](./08_join_paths_and_search.md), [09_cost_model_and_selectivity.md](./09_cost_model_and_selectivity.md) |

---

## 4. Prerequisites

This document assumes the reader is comfortable with:

- **Parser/rewriter output**: a `Query` tree as produced by
  `parse_analyze_*` and rewritten by `rewriteHandler.c`. The planner
  consumes the post-rewrite `Query`. The structure of `Query`,
  `RangeTblEntry`, `JoinExpr`, `FromExpr`, `SortGroupClause`,
  `Aggref`, etc. is presumed familiar.
- **Executor basics**: how `ExecutorRun` walks a `Plan` tree, how
  `ExecProject` evaluates target lists, what a `TupleTableSlot`
  is. See the executor super-document at
  [`../about_executor/`](../about_executor/) for a deep treatment.
- **Buffer manager and storage layer**: the planner only models the
  *cost* of fetching pages — it does not fetch them. Cost calculations
  (`cost_seqscan`, `cost_index`) refer to `seq_page_cost` and
  `random_page_cost` units that ultimately bottom out in the buffer
  manager. See [`../about_buffer_management/`](../about_buffer_management/).
- **MVCC and visibility**: planning generally ignores MVCC; an
  `IndexOnlyScan`'s cost considers `allvisfrac` from
  `pg_class.relallvisible` (the all-visible map). For deeper context
  see [`../about_mvcc/`](../about_mvcc/).
- **System catalogs**: at least `pg_class`, `pg_index`, `pg_statistic`,
  `pg_operator`, `pg_amop`. The planner reads these via
  `get_relation_info` (plancat.c), `get_op_btree_interpretation`,
  and friends.

Useful but not strictly required:
- Familiarity with relational algebra (joins, projections, selections,
  aggregations).
- Awareness of dynamic-programming optimization techniques (Selinger
  et al.'s System R paper) and genetic algorithms (for the GEQO
  module).

---

## 5. Conventions

- **File paths** in this document are repo-relative (e.g.
  `src/backend/optimizer/plan/planner.c:288`). They name a file *and
  a line number* in the PostgreSQL source tree.
- **Symbol names** like `planner`, `RelOptInfo`, `Path` use the same
  spelling as in the C source. Functions are written without
  parentheses unless calling them; structs/types are capitalized in
  CamelCase exactly as declared.
- **PostgreSQL implementation terms** are preferred over generic
  database vocabulary: `RelOptInfo` not "relation", `qual` not
  "predicate" (when the C code says `qual`), `pathkey` not "ordering
  key", `varnullingrels` not "outer-join nullification set".
- **Cross-references** use relative Markdown links:
  `[text](./08_join_paths_and_search.md#section-id)`.
- **Code blocks** carry a language tag (` ```c `, ` ```sql `,
  ` ```mermaid `).

---

## 6. How to update this document

When the upstream planner changes:
1. Re-run the Stage 1 architecture mapper to refresh
   `stage1/architecture_map.json` and `stage1/key_symbols.txt`.
2. Re-run the Stage 2 component documenter to refresh
   `stage2/component_*.md`.
3. Re-run Stage 3 (this integration step) to regenerate `final/`
   files.

The integration is idempotent in the sense that re-running it on
unchanged Stage 2 inputs produces unchanged Stage 3 outputs; line
numbers in citations are the only fragile element.

---

Next: [01_executive_summary.md](./01_executive_summary.md)
