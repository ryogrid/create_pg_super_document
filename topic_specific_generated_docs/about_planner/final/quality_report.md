# Quality Report — PostgreSQL Planner Super-Document

This report measures the coverage and quality of the assembled
`final/` documentation set for the PostgreSQL planner. All numbers are
derived programmatically from the source tree and the Stage 1 / Stage 2
inputs; the methodology for each metric is shown.

> **Generated as part of Stage 3 / Batch C** (the integration and
> appendix batch). Batches A and B are responsible for the index and
> the 16 numbered modules; Batch C produced 5 appendices, 2
> supplementary references, and this report.

---

## 1. Symbol coverage (top 50)

**Method**: `awk` over the 50 prioritized symbols in
`stage1/key_symbols.txt` (header lines excluded). For each symbol,
`grep -lr -F` over `final/*.md` checks whether the symbol name occurs
literally anywhere in the assembled documentation set.

- **Total top-50 key symbols**: 50
- **Symbols documented in `final/`**: **50**
- **Coverage**: **100 %** (target ≥ 80 %)
- **Missing**: none

## 2. Symbol coverage (full architecture map)

**Method**: same as (1), but iterating over the full 207-symbol roster
emitted by Stage 1's `architecture_map.json`.

- **Total architecture-map symbols**: 207
- **Symbols documented in `final/`**: **207**
- **Coverage**: **100 %**
- **Missing**: none

> Note: many of these symbols appear primarily in
> `appendix_symbol_index.md` and `planner_api_reference.md`, with deeper
> coverage in the numbered modules. Together they form a complete
> two-tier index: shallow (one-line description, location, link) for
> every symbol, deep (prose plus calling context) for the high-importance
> ones.

## 3. Path-subtype catalog coverage

**Method**: extract every `T_*Path` row from
`stage1/path_type_inventory.txt` (38 distinct subtypes) and verify that
the corresponding `Struct` name is present in
`stage2/path_catalog/*.md`.

- **Distinct Path subtypes inventoried**: 38
- **Subtypes documented in `stage2/path_catalog/`**: **38**
- **Coverage**: **100 %** (target 100 %)
- **Missing**: none

The 38 subtypes are organized across six catalog files:

| Catalog file | Subtypes covered |
|---|---|
| `scan_paths.md`                | Path (T_SeqScan / T_SampleScan / T_FunctionScan / T_TableFuncScan / T_ValuesScan / T_CteScan / T_NamedTuplestoreScan / T_Result / T_WorkTableScan), IndexPath, BitmapHeapPath, BitmapAndPath, BitmapOrPath, TidPath, TidRangePath, SubqueryScanPath, ForeignPath, CustomPath |
| `join_paths.md`                | NestPath, MergePath, HashPath |
| `upper_paths.md`               | SortPath, IncrementalSortPath, AggPath, GroupingSetsPath, MinMaxAggPath, WindowAggPath, UniquePath, SetOpPath, RecursiveUnionPath, LimitPath, ProjectionPath, ProjectSetPath, MaterialPath, MemoizePath, GroupResultPath, UpperUniquePath, GroupPath |
| `parallel_paths.md`            | GatherPath, GatherMergePath |
| `append_and_partition_paths.md`| AppendPath, MergeAppendPath |
| `modify_paths.md`              | ModifyTablePath, LockRowsPath |

The same data is consolidated for fast reference in
`final/appendix_path_quick_reference.md`.

## 4. Plan-creator catalog coverage

**Method**:
`grep -nE '^create_[a-z_]+_plan' src/backend/optimizer/plan/createplan.c`
returns every external linkage definition of a Path → Plan creator (45
entries). For each, verify it is mentioned by name in
`stage2/plan_creator_catalog/*.md`.

- **`create_*_plan` functions in `createplan.c`**: 45
- **Functions documented in `stage2/plan_creator_catalog/`**: **45**
- **Coverage**: **100 %** (target 100 %)
- **Missing**: none

The 45 creators are split across four catalog files:

| Catalog file | Creators covered |
|---|---|
| `scan_creators.md`     | create_scan_plan, create_seqscan_plan, create_samplescan_plan, create_indexscan_plan, create_bitmap_scan_plan, create_tidscan_plan, create_tidrangescan_plan, create_subqueryscan_plan, create_functionscan_plan, create_tablefuncscan_plan, create_valuesscan_plan, create_ctescan_plan, create_namedtuplestorescan_plan, create_resultscan_plan, create_worktablescan_plan, create_foreignscan_plan, create_customscan_plan |
| `join_creators.md`     | create_join_plan, create_nestloop_plan, create_mergejoin_plan, create_hashjoin_plan |
| `upper_creators.md`    | create_append_plan, create_merge_append_plan, create_group_result_plan, create_project_set_plan, create_material_plan, create_memoize_plan, create_unique_plan, create_gather_plan, create_gather_merge_plan, create_projection_plan, create_sort_plan, create_incrementalsort_plan, create_group_plan, create_upper_unique_plan, create_agg_plan, create_groupingsets_plan, create_minmaxagg_plan, create_windowagg_plan, create_setop_plan, create_recursiveunion_plan, create_limit_plan |
| `modify_creators.md`   | create_modifytable_plan, create_lockrows_plan, create_gating_plan |

## 5. Diagram count

**Method**: `ls stage2/diagrams/`.

- **Diagrams produced**: **12** (target ≥ 12)
- **Files** (all `*.mermaid`):
  1. `01_planner_pipeline.mermaid`
  2. `02_object_model.mermaid`
  3. `03_dp_join_search.mermaid`
  4. `04_geqo_main_loop.mermaid`
  5. `05_path_to_plan_map.mermaid`
  6. `06_eclass_derivation.mermaid`
  7. `07_pathkey_propagation.mermaid`
  8. `08_specialjoininfo_legality.mermaid`
  9. `09_parallel_path_gen.mermaid`
  10. `10_subquery_handling_decision.mermaid`
  11. `11_partition_pruning_plan_time.mermaid`
  12. `12_join_cost_decomposition.mermaid`

The diagram-to-module mapping is published in `final/index.md`.

## 6. TODO / FIXME markers

**Method**: `grep -lE '\bTODO\b|\bFIXME\b' final/*.md`.

- **Files with TODO/FIXME markers**: **0**

The Stage 2 source documents (`stage2/component_*.md`,
`stage2/path_catalog/*.md`, `stage2/plan_creator_catalog/*.md`) were
also clean.

## 7. Spot-check discrepancies between Stage 2 docs and `./src/`

Each line below was verified by a fresh `grep`/`Read` of the local
source tree at HEAD.

| Item | Stage 1 / Stage 2 says | Source reality | Verdict |
|---|---|---|---|
| `PlannerInfo` line | `pathnodes.h:220` (key_symbols.txt) | `typedef struct PlannerInfo PlannerInfo;` at line 191; `struct PlannerInfo {` at line 195; closes at line 556 | **Off-by-26**. The Stage 1 line 220 points into the body of the struct (a `List *plan_params;` field), not the typedef. Updated in `appendix_data_structures.md` to call out both lines. |
| `RestrictInfo` line | `pathnodes.h:0` | typedef at line 2559, closes at 2711 | **Stage 1 had `0`** (extractor failed); appendix gives the real line. |
| `PlaceHolderVar` / `SpecialJoinInfo` / `AppendRelInfo` / `IndexOptInfo` | `pathnodes.h:0` | 2780 / 2891 / 2959 / 1100 (typedef) | Same extractor problem, all corrected in `appendix_data_structures.md`. |
| `EquivalenceClass` line | `pathnodes.h:1356` (key_symbols.txt) | typedef at 1379; line 1356 is mid-comment-block immediately above | Pointed to the doc-block lead-in; corrected to 1379 in `appendix_data_structures.md`. |
| `cost_seqscan` | `costsize.c:284` | confirmed | OK |
| `cost_index` | `costsize.c:549` | confirmed | OK |
| `final_cost_nestloop` line | architecture_map says `costsize.c:3308` | confirmed at 3308 | OK |
| `STD_FUZZ_FACTOR` value | docs say 1.01 | `pathnode.c:47` confirms `#define STD_FUZZ_FACTOR 1.01` | OK |
| `geqo_threshold` default | `12` | `guc_tables.c:2103` confirms | OK |
| `from_collapse_limit` / `join_collapse_limit` defaults | `8` each | `guc_tables.c:2080, 2093` confirm | OK |
| GUCs claimed at `costsize.c:0` | various | actual variable declarations live at `costsize.c:119–154` | Stage 1 extractor output `:0` for GUC declarations because they are not function definitions; the appendices give the real line ranges. |

**No semantic discrepancies were found.** All "discrepancies" in
the Stage 1 outputs are systematic line-extraction artifacts; the
appendices in this batch correct them and quote the canonical source.

## 8. Known gaps and improvement suggestions

1. **Module 10 (`10_equivalence_classes_and_pathkeys.md`) — pending**.
   Cross-references in this batch's appendices already point to it via
   the canonical filename; the link will resolve once Batch B finishes.
2. **Stage 1 line numbers for structs and GUCs are unreliable.** Future
   extractor runs should use the same regex-based scanner used for this
   batch (`grep -nE 'typedef struct (Foo)$'` followed by a paired
   `} Foo;` search) rather than the current "first declaration" heuristic.
3. **Some component documents quote line numbers that drift over time.**
   When the planner is re-documented after a major version bump,
   a verification pass should re-run the same grep used here.
4. **Path catalog and plan creator catalog could be merged into a
   single appendix.** The information is highly correlated; right now
   the relationship is only visible in
   `appendix_path_quick_reference.md`.
5. **No dedicated module for upper paths / DML paths exists in the
   numbered series**; they are documented inside
   `16_plan_creation_and_setrefs.md` and
   `appendix_path_quick_reference.md`. Splitting them out into modules
   17 and 18 would balance chapter sizes (`16_plan_creation_and_setrefs.md`
   is currently 421 lines; `08_join_paths_and_search.md` is 699).

## 9. Quality scorecard

| Criterion | Target | Achieved | Status |
|---|---|---|---|
| Top-50 symbol coverage in final/ | ≥ 80 %       | 100 % | PASS |
| All 207 architecture-map symbols in final/ | best-effort | 100 % | PASS |
| Path-subtype catalog coverage | 100 %         | 100 % | PASS |
| Plan-creator catalog coverage | 100 %         | 100 % | PASS |
| Diagram count                 | ≥ 12          | 12    | PASS |
| No TODO/FIXME markers in final/ | 0           | 0     | PASS |
| Cross-references resolve to existing files | all internal | 14/15 (10_*.md still pending) | WAIVED — pending Batch B |
| Source verification of struct definitions | quoted from header | yes (HEAD) | PASS |
| Self-contained (minimal external links) | yes        | yes; only links are to local `final/` and absolute `src/` paths | PASS |

## 10. Files written by this batch

```
appendix_symbol_index.md           277 lines
appendix_glossary.md               345 lines
appendix_data_structures.md      1,040 lines
appendix_path_quick_reference.md   148 lines
appendix_guc_parameters.md         321 lines
planner_quick_reference.md         177 lines
planner_api_reference.md           849 lines
quality_report.md                  this file
```

Total appendix + supplementary lines: ≈ 3,157 (excluding this report).
