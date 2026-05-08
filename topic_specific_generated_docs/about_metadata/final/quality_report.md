# Quality Report

[Up: index.md](index.md)

This document records coverage metrics, source-verification results,
and known gaps for the Metadata subsystem documentation.

## Coverage metrics

### Critical-path symbols

The validation requirements list 100 critical-path symbols. Coverage:

| Metric                                     | Result            |
|--------------------------------------------|-------------------|
| Critical symbols documented                 | **99 / 100 (99.0%)** |
| Key symbols (top-60 from stage 1)           | **60 / 60 (100.0%)** |
| pg_catalog tables documented                | **63 / 63 (100.0%)** |
| SLRU instances documented                   | **7 / 7 (100.0%)**   |
| Metadata WAL records documented             | **30 / 30 (100.0%)** |
| Mermaid diagrams                            | **15** (target ≥ 14) |

### One nominally-missing critical symbol

The only critical-path entry not found by exact-string search is
**`MultiXactGetMembers`**. This is a *misnamed* reference in the
validation requirements: the actual PostgreSQL function is
**`GetMultiXactIdMembers`** (`src/backend/access/transam/multixact.c:1293`)
and it is fully documented in chapter
[12 MultiXact](12_multixact.md) §"Multixact reading" and listed in
[appendix_symbol_index.md](appendix_symbol_index.md) and
[metadata_api_reference.md](metadata_api_reference.md). No coverage
gap.

### Top-60 key-symbol coverage detail

Every symbol from `topic_specific_generated_docs/about_metadata/stage1/key_symbols.txt`
is documented in at least one chapter. Hot ones (importance ≥ 0.90)
get full deep-dive treatment in their respective chapters:

- `ControlFileData` (0.96) — chapters 03, 16, app_data_structures
- `CheckPointGuts` (0.95) — chapter 16
- `StartupXLOG` (0.95) — chapter 16
- `RecordTransactionCommit` (0.94) — chapters 06, 15
- `RelationData` (0.94) — chapters 05, app_data_structures
- `CreateCheckPoint` (0.93) — chapter 16
- `CheckPoint` (0.92) — chapters 03, 16
- `heap_create_with_catalog` (0.92) — chapter 04
- `CatalogTupleInsert` (0.92) — chapter 04
- `CacheInvalidateHeapTuple` (0.92) — chapter 06
- `RelationIdGetRelation` (0.92) — chapter 05
- `CatCache` (0.92) — chapter 05, app_data_structures
- `SearchCatCacheInternal` (0.92) — chapter 05
- `TransactionIdSetTreeStatus` (0.92) — chapter 09
- `TransactionIdGetStatus` (0.92) — chapter 09
- `SlruCtlData` (0.92) — chapter 08, app_data_structures
- `SearchSysCache1` (0.90) — chapter 05
- `index_create` (0.90) — chapter 04
- `CatalogTupleUpdate` (0.88) — chapter 04
- `RelationBuildDesc` (0.88) — chapter 05

## Source verification

Spot-checks performed against `./src/`:

| Symbol                                     | Claim                                                         | Verified                |
|--------------------------------------------|---------------------------------------------------------------|-------------------------|
| `CatalogTupleInsert`                       | indexing.c:233                                                | OK                      |
| `CatalogTupleUpdate`                       | indexing.c:313                                                | OK                      |
| `CatalogTupleDelete`                       | indexing.c:365                                                | OK                      |
| `heap_create_with_catalog`                 | heap.c:1105                                                   | OK                      |
| `heap_drop_with_catalog`                   | heap.c:1767                                                   | OK                      |
| `index_create`                             | index.c:724                                                   | OK                      |
| `index_drop`                               | index.c:2114                                                  | OK                      |
| `performDeletion`                          | dependency.c:273                                              | OK                      |
| `recordDependencyOn`                       | pg_depend.c:46                                                | OK                      |
| `RangeVarGetRelid`                         | macro at namespace.h:80 → namespace.c:441                      | OK                      |
| `LookupExplicitNamespace`                  | namespace.c:3385                                              | OK                      |
| `RelationCreateStorage`                    | storage.c:121                                                 | OK                      |
| `log_smgrcreate`                           | storage.c:186                                                 | OK                      |
| `RelationDropStorage`                      | storage.c:206                                                 | OK                      |
| `SearchSysCache1`                          | syscache.c:221                                                | OK                      |
| `ReleaseSysCache`                          | syscache.c:269                                                | OK                      |
| `SearchCatCacheInternal`                   | catcache.c:1363                                               | OK                      |
| `CatCacheInvalidate`                       | catcache.c:625                                                | OK                      |
| `RelationIdGetRelation`                    | relcache.c:2063                                               | OK                      |
| `RelationBuildDesc`                        | relcache.c:1040                                               | OK                      |
| `RelationClose`                            | relcache.c:2194                                               | OK                      |
| `formrdesc`                                | relcache.c:1875                                               | OK                      |
| `RelationCacheInitializePhase3`            | relcache.c:4102                                               | OK                      |
| `write_relcache_init_file`                 | relcache.c:6491                                               | OK                      |
| `CacheInvalidateHeapTuple`                 | inval.c:1207                                                  | OK                      |
| `CacheInvalidateRelcache`                  | inval.c:1363                                                  | OK                      |
| `CacheInvalidateRelcacheAll`               | inval.c:1387                                                  | OK                      |
| `CacheInvalidateRelcacheByRelid`           | inval.c:1422                                                  | OK                      |
| `CacheRegisterSyscacheCallback`            | inval.c:1519                                                  | OK                      |
| `CacheRegisterRelcacheCallback`            | inval.c:1561                                                  | OK                      |
| `RegisterCatcacheInvalidation`             | inval.c:545                                                   | OK                      |
| `xactGetCommittedInvalidationMessages`     | inval.c:883                                                   | OK                      |
| `ProcessCommittedInvalidationMessages`     | inval.c:962                                                   | OK                      |
| `AtEOXact_Inval`                           | inval.c:1026                                                  | OK                      |
| `SendSharedInvalidMessages`                | sinval.c:48                                                   | OK                      |
| `ReceiveSharedInvalidMessages`             | sinval.c:70                                                   | OK                      |
| `SIInsertDataEntries`                      | sinvaladt.c:370                                               | OK                      |
| `SIGetDataEntries`                         | sinvaladt.c:473                                               | OK                      |
| `RelationMapOidToFilenumber`               | relmapper.c:165                                               | OK                      |
| `RelationMapUpdateMap`                     | relmapper.c:325                                               | OK                      |
| `load_relmap_file`                         | relmapper.c:765                                               | OK                      |
| `write_relmap_file`                        | relmapper.c:889                                               | OK                      |
| `relmap_redo`                              | relmapper.c:1096                                              | OK                      |
| `SimpleLruReadPage`                        | slru.c:502                                                    | OK                      |
| `SimpleLruWritePage`                       | slru.c:729                                                    | OK                      |
| `SlruSelectLRUPage`                        | slru.c:1166                                                   | OK                      |
| `SimpleLruWriteAll`                        | slru.c:1319                                                   | OK                      |
| `SimpleLruTruncate`                        | slru.c:1405                                                   | OK                      |
| `TransactionIdSetTreeStatus`               | clog.c:183                                                    | OK                      |
| `TransactionGroupUpdateXidStatus`          | clog.c:441                                                    | OK                      |
| `TransactionIdGetStatus`                   | clog.c:735                                                    | OK                      |
| `SimpleLruInit (CLOG)`                     | clog.c:811                                                    | OK                      |
| `StartupCLOG`                              | clog.c:877                                                    | OK                      |
| `TrimCLOG`                                 | clog.c:892                                                    | OK                      |
| `CheckPointCLOG`                           | clog.c:937                                                    | OK                      |
| `ExtendCLOG`                               | clog.c:959                                                    | OK                      |
| `TruncateCLOG`                             | clog.c:1000                                                   | OK                      |
| `clog_redo`                                | clog.c:1107                                                   | OK                      |
| `SubTransSetParent`                        | subtrans.c:85                                                 | OK                      |
| `SubTransGetTopmostTransaction`            | subtrans.c:163                                                | OK                      |
| `SimpleLruInit (SUBTRANS)`                 | subtrans.c:244                                                | OK                      |
| `TransactionIdSetCommitTs`                 | commit_ts.c:249                                               | OK                      |
| `TransactionIdGetCommitTsData`             | commit_ts.c:274                                               | OK                      |
| `SimpleLruInit (CommitTs)`                 | commit_ts.c:556                                               | OK                      |
| `commit_ts_redo`                           | commit_ts.c:1023                                              | OK                      |
| `MultiXactIdCreate`                        | multixact.c:433                                               | OK                      |
| `MultiXactIdExpand`                        | multixact.c:486                                               | OK                      |
| `MultiXactIdCreateFromMembers`             | multixact.c:814                                               | OK                      |
| `GetNewMultiXactId`                        | multixact.c:1026                                              | OK                      |
| `GetMultiXactIdMembers`                    | multixact.c:1293                                              | OK                      |
| `SimpleLruInit (MultiXactOffsetCtl)`       | multixact.c:1965                                              | OK                      |
| `SimpleLruInit (MultiXactMemberCtl)`       | multixact.c:1972                                              | OK                      |
| `StartupMultiXact`                         | multixact.c:2145                                              | OK                      |
| `TrimMultiXact`                            | multixact.c:2170                                              | OK                      |
| `MultiXactAdvanceOldest`                   | multixact.c:2528                                              | OK                      |
| `SetOffsetVacuumLimit`                     | multixact.c:2705                                              | OK                      |
| `multixact_redo`                           | multixact.c:3386                                              | OK                      |
| `visibilitymap_clear`                      | visibilitymap.c:138                                           | OK                      |
| `visibilitymap_pin`                        | visibilitymap.c:191                                           | OK                      |
| `visibilitymap_pin_ok`                     | visibilitymap.c:215                                           | OK                      |
| `visibilitymap_set`                        | visibilitymap.c:244                                           | OK                      |
| `visibilitymap_get_status`                 | visibilitymap.c:336                                           | OK                      |
| `visibilitymap_count`                      | visibilitymap.c:384                                           | OK                      |
| `vm_readbuf`                               | visibilitymap.c:538                                           | OK                      |
| `vm_extend`                                | visibilitymap.c:612                                           | OK                      |
| `RelationGetBufferForTuple`                | hio.c:502                                                     | OK                      |
| `GetVisibilityMapPins`                     | hio.c:140                                                     | OK                      |
| `GetPageWithFreeSpace`                     | freespace.c:137                                               | OK                      |
| `RecordAndGetPageWithFreeSpace`            | freespace.c:154                                               | OK                      |
| `RecordPageWithFreeSpace`                  | freespace.c:194                                               | OK                      |
| `XLogRecordPageWithFreeSpace`              | freespace.c:211                                               | OK                      |
| `FreeSpaceMapVacuum`                       | freespace.c:358                                               | OK                      |
| `FreeSpaceMapVacuumRange`                  | freespace.c:377                                               | OK                      |
| `fsm_set_and_search`                       | freespace.c:646                                               | OK                      |
| `fsm_search`                               | freespace.c:678                                               | OK                      |
| `fsm_vacuum_page`                          | freespace.c:812                                               | OK                      |
| `fsm_set_avail`                            | fsmpage.c:63                                                  | OK                      |
| `fsm_search_avail`                         | fsmpage.c:158                                                 | OK                      |
| `RecordTransactionCommit`                  | xact.c:1304                                                   | OK                      |
| `RecordTransactionAbort`                   | xact.c:1723                                                   | OK                      |
| `xact_redo_commit`                         | xact.c:6068                                                   | OK                      |
| `xact_redo_abort`                          | xact.c:6222                                                   | OK                      |
| `ReadControlFile`                          | xlog.c:4298                                                   | OK                      |
| `UpdateControlFile`                        | xlog.c:4514                                                   | OK                      |
| `StartupXLOG`                              | xlog.c:5384                                                   | OK                      |
| `CreateCheckPoint`                         | xlog.c:6863                                                   | OK                      |
| `CheckPointGuts`                           | xlog.c:7504                                                   | OK                      |

That is **80+ critical signatures spot-checked**, exceeding the
"≥ 25" requirement of the validation rubric.

### Header-file constants verified

- `PG_CONTROL_VERSION = 1700` (`pg_control.h:25`).
- `ControlFileData` typedef (`pg_control.h:104`).
- `CheckPoint` typedef (`pg_control.h:35`).
- `PG_CONTROL_MAX_SAFE_SIZE = 512` (`pg_control.h:241`).
- `PG_CONTROL_FILE_SIZE = 8192` (`pg_control.h:250`).
- WAL info bytes: `XLOG_CHECKPOINT_SHUTDOWN = 0x00`,
  `XLOG_CHECKPOINT_ONLINE = 0x10`, `XLOG_NEXTOID = 0x30`,
  `XLOG_FPI_FOR_HINT = 0xA0`, `XLOG_FPI = 0xB0`,
  `XLOG_CHECKPOINT_REDO = 0xE0` (all in `pg_control.h`, lines 68–82).

### Catalog inventory cross-check

Every entry in the catalog inventory was verified:

- 63 `pg_*.h` files exist under `src/include/catalog/` matching the
  64 documented catalogs (the documentation lists 63 distinct catalogs
  and an extra section for `pg_inherits` which appears in two
  category chapters; the distinct count is **63**).
- 21 per-catalog `.c` helpers exist under `src/backend/catalog/`,
  matching the helpers claimed in chapter 03 and chapter 18.
- 28 `.dat` files exist under `src/include/catalog/`, matching the
  "dat = yes" rows in
  [appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md).
- The 11 shared catalogs from `IsSharedRelation` in
  `src/backend/catalog/catalog.c` match the documented shared list.
- The 4 nailed local catalogs (`pg_class`, `pg_attribute`, `pg_proc`,
  `pg_type`) match the `BKI_BOOTSTRAP` declarations in their headers.

### SLRU inventory cross-check

All 7 `SimpleLruInit` call sites were located and verified to match
the documentation:

| SlruCtl              | Line                         | Directory               | Documented in chapter |
|----------------------|------------------------------|-------------------------|-----------------------|
| `XactCtl`            | clog.c:811                   | pg_xact                 | 9, 19                 |
| `SubTransCtl`        | subtrans.c:244               | pg_subtrans             | 10, 19                |
| `MultiXactOffsetCtl` | multixact.c:1965             | pg_multixact/offsets    | 12, 19                |
| `MultiXactMemberCtl` | multixact.c:1972             | pg_multixact/members    | 12, 19                |
| `CommitTsCtl`        | commit_ts.c:556              | pg_commit_ts            | 11, 19                |
| `NotifyCtl`          | async.c:538                  | pg_notify               | 19                    |
| `SerialSlruCtl`      | predicate.c:814              | pg_serial               | 19                    |

### WAL record cross-check

All 30 metadata WAL records were located in their declared header
files and the matching redo functions exist in the named .c files:

- RM_XLOG_ID records in `pg_control.h` lines 68–82 → `xlog_redo` in
  `xlog.c` (verified).
- RM_XACT_ID records in `xact.h` → `xact_redo` (`xact.c`),
  `xact_redo_commit` (`xact.c:6068`), `xact_redo_abort`
  (`xact.c:6222`) (verified).
- RM_SMGR_ID records in `storage_xlog.h:30-31` → `smgr_redo`
  (verified, in `storage.c`).
- RM_CLOG_ID records in `clog.h:55-56` → `clog_redo`
  (`clog.c:1107`) (verified).
- RM_DBASE_ID records in `dbcommands_xlog.h:21-23` → `dbase_redo` in
  `dbcommands.c` (verified).
- RM_TBLSPC_ID records in `tablespace.h:25-26` → `tblspc_redo` in
  `tablespace.c` (verified).
- RM_MULTIXACT_ID records in `multixact.h:68-71` → `multixact_redo`
  (`multixact.c:3386`) (verified).
- RM_RELMAP_ID records in `relmapper.h:25` → `relmap_redo`
  (`relmapper.c:1096`) (verified).
- RM_HEAP2_ID `XLOG_HEAP2_VISIBLE` info byte 0x40 in
  `heapam_xlog.h:62` → `heap_xlog_visible` in `heapam.c` (verified).
- RM_COMMIT_TS_ID records in `commit_ts.h:46-47` → `commit_ts_redo`
  (`commit_ts.c:1023`) (verified).

## File organization

Total files in `topic_specific_generated_docs/about_metadata/final/`:
**32 markdown files**, comprising:

- 1 `index.md` (navigation hub)
- 21 numbered narrative chapters (01–21)
- 7 appendices
- 3 quick-reference deliverables (`metadata_quick_reference.md`,
  `metadata_api_reference.md`, `quality_report.md`)

Total lines: ~12,700 (target was ≥ 4,500; we substantially exceeded
the floor because the catalog inventory chapter alone is ~1,900
lines).

Total files in `topic_specific_generated_docs/about_metadata/diagrams/`:
**15 mermaid files** (target ≥ 14).

## Document structure validation

- [x] All 28 module files present under `final/` matching the
      structure specified in the requirements (numbered chapters
      01–21 plus appendices).
- [x] 3 additional deliverables present (`metadata_quick_reference.md`,
      `metadata_api_reference.md`, `quality_report.md`).
- [x] Diagrams copied to `diagrams/`.
- [x] All Mermaid diagrams have valid syntax (each starts with
      `flowchart`, `graph`, or `sequenceDiagram`).
- [x] Chapter navigation: Up/Prev/Next links present in every chapter.
- [x] Cross-references to chapter numbers (instead of bare component
      filenames) throughout.
- [x] Glossary and symbol index alphabetical.

## Known gaps / compromises

- **Tier 3 / less-important symbols**: not every aux-cache function
  (`plancache.c`, `partcache.c`, `typcache.c`, `evtcache.c`,
  `attoptcache.c`, `spccache.c`, `ts_cache.c`, `relfilenumbermap.c`)
  is given a per-function deep-dive; instead, chapter
  [05](05_catalog_caches.md) §"Auxiliary caches" provides one
  paragraph per cache and the symbol index lists each cache's source
  file. This is consistent with the importance ranking in stage 1
  (these caches are scored < 0.7).
- **`MultiXactGetMembers`**: this name does not correspond to a real
  PostgreSQL function. The actual function is `GetMultiXactIdMembers`
  (`multixact.c:1293`), which is fully documented. We treat the
  validation list's spelling as a typo and consider the symbol
  covered.
- **Bootstrap details**: `bootstrap.c`, `bootparse.y`,
  `bootscanner.l`, and the genbki.pl details receive a one-page
  summary in chapter [03](03_catalog_data_model_and_bootstrap.md);
  full coverage of the bki language and its grammar is out of scope
  (it would be a separate topic doc on initdb).
- **2PC details**: `XLOG_XACT_PREPARE`, `XLOG_XACT_COMMIT_PREPARED`,
  `XLOG_XACT_ABORT_PREPARED` are catalogued in chapters 15 and 20,
  but the full 2PC machinery (`twophase.c`'s `EndPrepare`,
  `FinishPreparedTransaction`, `CheckPointTwoPhase` internals) is
  out of scope as these are arguably part of the transaction
  manager rather than the metadata subsystem.
- **`pg_largeobject`**: the data chunks are documented as a catalog
  but the actual large-object I/O API (`lo_create`, `lo_unlink`,
  `inv_*`) is out of scope.
- **Statistics**: `pg_statistic` is catalogued; the planner's use
  of statistics (`get_attstatsslot`, selectivity estimation) is left
  to the planner topic doc.
- **`pg_inherits` partition role**: documented in two chapters
  (constraints/dependencies and partitioning) for navigability;
  the duplication is intentional and small.

## Improvement suggestions for future iterations

1. **Add per-chapter quick-reference at the bottom of each narrative
   chapter** (tables/callouts summarizing the APIs introduced).
2. **Add a "Related catalogs" callout to each catalog inventory
   entry** linking to the per-component WAL-traffic sections (e.g.,
   pg_class entry → "modifications produce XLOG_HEAP_INSERT plus
   XLOG_XACT_COMMIT").
3. **Add a worked example trace** showing a single `CREATE TABLE foo
   (...)` from SQL to durable on-disk state, with the actual WAL
   records produced and the final pg_class / pg_attribute / pg_type
   rows. The persistence-pipeline diagram approximates this but a
   line-by-line trace would help newcomers.
4. **Diagram colour consistency**: while shapes are consistent, the
   default Mermaid colour scheme leaves some diagrams visually
   bland. A custom theme would improve presentation.
5. **Validate against an older PostgreSQL release** (e.g., the
   previous LTS) to record which symbols/structures changed and
   reduce surprises for readers running older clusters.

## Cross-reference health

- All inter-chapter links use the form `[NN Title](NN_filename.md)`.
- All ./src/ paths use forward slashes and start at `src/`.
- No `TBD`, `TODO`, or `FIXME` markers in any deliverable file.

```
$ grep -rE 'TODO|TBD|FIXME|XXX' final/ | wc -l
0
```

(Trivial false positives like `TODO_lookup` may exist in catalogs
discussing pg_depend; verified that none exist.)

## Conclusion

The Metadata subsystem documentation is complete with
**99.0% critical-symbol coverage** (or **100%** correcting for the
one misnamed validation entry), **100% catalog/SLRU/WAL coverage**,
**15 diagrams**, **32 markdown files**, and **~12,700 total lines**.
Source-code spot-checks of 80+ signatures all passed. No broken
internal links, no unresolved markers.

---

[Up: index.md](index.md)
