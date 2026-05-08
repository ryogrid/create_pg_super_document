# PostgreSQL Metadata Subsystem — Final Documentation

[Top: ../../README.md](../../README.md)

This is the integrated, professionally edited documentation for the
PostgreSQL Metadata subsystem. It collates the work of fifteen
component narratives, twelve catalog-inventory chapters, six SLRU
inventories, and eight WAL-record inventories into a single coherent
reading experience.

The Metadata subsystem is the **self-describing** layer of the database:
it stores, indexes, caches, persists, and replicates every piece of
information about the database itself — every relation, every column,
every type, every function, every dependency, every transaction's
commit status, every subtransaction's parent, every multi-locker tuple,
every page-level all-visible/all-frozen hint, and every page's
free-space estimate. This topic doc explains how all of those pieces
fit together and how they survive crashes, recovery, replication, and
the careful choreography of WAL, checkpoint, and pg_control.

## How to read this document

If you are new to the subsystem, read in order:

1. [01 — Executive Summary](01_executive_summary.md)
2. [02 — Architecture Overview](02_architecture_overview.md)
3. [03 — Catalog Data Model and Bootstrap](03_catalog_data_model_and_bootstrap.md)

Then pick a path:

| Goal                                       | Recommended chapters                                              |
|--------------------------------------------|-------------------------------------------------------------------|
| Understand DDL machinery                   | 04, 05, 06                                                        |
| Understand transaction durability          | 08, 09, 11, 15, 16                                                |
| Understand row visibility                  | 09, 10, 12, 13                                                    |
| Understand the bootstrap circularity       | 03, 07, 21 § "Relmapper bootstrap"                                |
| Look up a specific pg_catalog table        | 18 plus appendix_pg_catalog_quick_reference                        |
| Look up a specific WAL record              | 20 plus appendix_wal_record_quick_reference                        |
| Look up a specific SLRU                    | 19 plus appendix_slru_quick_reference                              |
| Tune a metadata-affecting GUC              | appendix_guc_parameters                                            |
| Understand $PGDATA layout                  | appendix_pgdata_layout                                             |

## Module index

### Narrative chapters

| #  | File                                                                            | Topic                                                          |
|---:|---------------------------------------------------------------------------------|----------------------------------------------------------------|
| 01 | [Executive Summary](01_executive_summary.md)                                    | one-page overview                                              |
| 02 | [Architecture Overview](02_architecture_overview.md)                            | the four data domains and the WAL/checkpoint/pg_control spine  |
| 03 | [Catalog Data Model and Bootstrap](03_catalog_data_model_and_bootstrap.md)      | genbki, .dat files, formrdesc, nailed/shared/mapped, catversion |
| 04 | [Catalog Modification APIs](04_catalog_modification_apis.md)                    | heap.c, index.c, indexing.c, dependency.c, namespace.c, storage.c |
| 05 | [Catalog Caches](05_catalog_caches.md)                                          | catcache, syscache, relcache, plus auxiliary caches            |
| 06 | [Cache Invalidation](06_cache_invalidation.md)                                  | inval, sinval, sinvaladt, commit-time message contract         |
| 07 | [Relmapper](07_relmapper.md)                                                    | pg_filenode.map, XLOG_RELMAP_UPDATE                            |
| 08 | [SLRU Framework](08_slru_framework.md)                                          | slru.c, bank locking, page-state machine                       |
| 09 | [CLOG](09_clog.md)                                                              | commit/abort status, group commit, lifecycle                   |
| 10 | [SUBTRANS](10_subtrans.md)                                                      | subtransaction parents, the not-WAL-logged exception           |
| 11 | [Commit Timestamps](11_commit_timestamps.md)                                    | pg_commit_ts, GUC, lifecycle                                   |
| 12 | [MultiXact](12_multixact.md)                                                    | two-SLRU duality, wraparound, freeze threshold                 |
| 13 | [Visibility Map](13_visibility_map.md)                                          | ALL_VISIBLE / ALL_FROZEN, set/clear protocols, IOS             |
| 14 | [Free Space Map](14_free_space_map.md)                                          | 3-level tree, page-internal heap, hio.c                        |
| 15 | [Persistence and WAL Records](15_persistence_and_wal_records.md)                | rmgrlist-driven dispatch, every metadata WAL record            |
| 16 | [Checkpoints and Recovery](16_checkpoints_and_recovery.md)                      | CreateCheckPoint dispatch, StartupXLOG, Trim* hooks            |
| 17 | [Hooks and Extensibility](17_hooks_and_extensibility.md)                        | object_access_hook, cache callbacks, custom_rmgr               |

### Detailed catalog chapters

| #  | File                                                                            | Topic                                                          |
|---:|---------------------------------------------------------------------------------|----------------------------------------------------------------|
| 18 | [Catalog Inventory](18_catalog_inventory.md)                                    | every pg_catalog table — detailed catalog (63 tables)          |
| 19 | [SLRU Users Catalog](19_slru_users_catalog.md)                                  | every SLRU instance — detailed catalog (7 SLRUs)               |
| 20 | [WAL Record Catalog](20_wal_record_catalog.md)                                  | every metadata-affecting WAL record (30 records across 9 rmgrs)|

### Deep dives

| #  | File                                                                            | Topic                                                          |
|---:|---------------------------------------------------------------------------------|----------------------------------------------------------------|
| 21 | [Deep Dives](21_deep_dives.md)                                                  | 18 cross-cutting topics that need a full essay                  |

### Appendices

| File                                                                                          | Topic                                                       |
|-----------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| [appendix_symbol_index.md](appendix_symbol_index.md)                                          | alphabetical symbol reference                               |
| [appendix_glossary.md](appendix_glossary.md)                                                  | metadata terminology                                         |
| [appendix_data_structures.md](appendix_data_structures.md)                                    | key struct definitions                                      |
| [appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md)              | one row per catalog table                                    |
| [appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)                          | one row per SLRU instance                                    |
| [appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md)              | one row per metadata WAL record                              |
| [appendix_pgdata_layout.md](appendix_pgdata_layout.md)                                        | $PGDATA on-disk file map                                     |
| [appendix_guc_parameters.md](appendix_guc_parameters.md)                                      | every metadata-relevant GUC                                  |

### Quick-reference deliverables

| File                                                                              | Audience                                                          |
|-----------------------------------------------------------------------------------|-------------------------------------------------------------------|
| [metadata_quick_reference.md](metadata_quick_reference.md)                        | a 3-page printable summary                                         |
| [metadata_api_reference.md](metadata_api_reference.md)                            | function signatures grouped by subsystem                            |
| [quality_report.md](quality_report.md)                                            | coverage metrics, known gaps                                       |

## Conventions used in this document

- File and line citations look like `src/backend/access/transam/clog.c:735`
  and refer to a specific revision of the PostgreSQL source tree under
  the project root.
- Code blocks show *abridged* signatures; the leading `extern`,
  `pg_attribute_*`, calling-convention macros, and most argument
  comments are omitted for readability. The exact signature is in the
  cited file.
- Mermaid diagrams use the same colour and shape conventions
  throughout: rounded rectangles for active code, cylinders for
  on-disk files, hexagons for cluster-wide singletons (pg_control,
  pg_filenode.map), and dashed arrows for invalidation/notification
  flows.
- Terminology is standardized to PostgreSQL implementation terms
  (see [glossary](appendix_glossary.md)). For example, this document
  always uses "Relation" (or `RelationData`) for the relcache entry
  struct, "system catalog" not "data dictionary", "SLRU" not
  "circular log", "FPI" not "full-page write" (the latter is the GUC
  name), "FSM" not "free space file", "VM" not "vis map", "redo" not
  "replay" (though "WAL replay" is acceptable when describing the
  standby path).
- "Tier 1" annotations indicate the symbol's importance score (≥ 0.85)
  in the auto-extracted ranking from stage 1.

## Diagram catalogue

All Mermaid sources live under
`topic_specific_generated_docs/about_metadata/diagrams/`. They are
embedded inline in the relevant chapters; this list lets you find them
quickly:

| File                                                | Chapter           | Topic                                                |
|-----------------------------------------------------|-------------------|------------------------------------------------------|
| `01_persistence_pipeline.mermaid`                   | 02, 15            | end-to-end metadata persistence                      |
| `02_catalog_stack.mermaid`                          | 04                | DDL → pg_*.c → indexing.c → pg_catalog               |
| `03_catalog_caches.mermaid`                         | 05                | relcache / syscache / catcache layering              |
| `04_sinval_distribution.mermaid`                    | 06                | sinval ring buffer, overflow                         |
| `05_slru_disk_layout.mermaid`                       | 08                | SLRU segment files, bank-lock partitioning           |
| `06_slru_page_state.mermaid`                        | 08                | EMPTY → READING → VALID → DIRTY → WRITING            |
| `07_clog_page_format.mermaid`                       | 09                | 2 b/XID, 4 XIDs/byte, group_lsn                      |
| `08_multixact_duality.mermaid`                      | 12                | offsets ↔ members two-SLRU split                    |
| `09_vm_page_format.mermaid`                         | 13                | VM 2 b/heap-page, ALL_VISIBLE + ALL_FROZEN           |
| `10_vm_clear_set_protocol.mermaid`                  | 13                | pin-before-lock, LSN handshake                       |
| `11_fsm_three_level_tree.mermaid`                   | 14                | FSM root, midlevel, leaf                             |
| `12_fsm_page_internal_heap.mermaid`                 | 14                | per-page binary heap                                 |
| `13_hio_fsm_vm_integration.mermaid`                 | 14                | RelationGetBufferForTuple ↔ FSM ↔ VM                 |
| `14_pg_control_checkpoint_flow.mermaid`             | 16                | CheckPointGuts dispatch + UpdateControlFile          |
| `15_recovery_sequence.mermaid`                      | 16                | ReadControlFile → StartupXLOG → Trim*                |

## Source authority

This document is a derived work; PostgreSQL source code is the
ultimate authority. When this document and the source disagree, the
source wins. The relevant source files are listed in each chapter's
"Source references" section and in
[appendix_symbol_index.md](appendix_symbol_index.md).

The text was cross-checked against the current PostgreSQL `master`
branch at the time of writing (catalog_version_no, function
signatures, struct layouts, file paths, and line numbers). See
[quality_report.md](quality_report.md) for the verification log.
