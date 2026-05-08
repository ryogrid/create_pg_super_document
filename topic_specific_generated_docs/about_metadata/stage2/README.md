# PostgreSQL Metadata Subsystem — Stage 2 Documentation

Documentation for the PostgreSQL Metadata subsystem: system catalogs, the
CLOG family, the visibility map, the free space map, and their persistence
guarantees.

## Component documents

| File                                                    | Topic                                              |
|---------------------------------------------------------|----------------------------------------------------|
| `component_catalog_data_model_and_bootstrap.md`         | genbki, .dat files, formrdesc, pg_control          |
| `component_catalog_modification_apis.md`                | heap.c, index.c, indexing.c, dependency.c          |
| `component_catalog_caches.md`                            | catcache, syscache, relcache, plancache, etc.       |
| `component_cache_invalidation.md`                        | inval.c, sinval.c, sinvaladt.c                      |
| `component_relmapper.md`                                 | pg_filenode.map, XLOG_RELMAP_UPDATE                 |
| `component_slru_framework.md`                            | slru.c, bank locking, page-state machine            |
| `component_clog.md`                                      | pg_xact, transaction commit log                     |
| `component_subtrans.md`                                  | pg_subtrans, not-WAL-logged exception               |
| `component_commit_ts.md`                                 | pg_commit_ts, commit timestamps                     |
| `component_multixact.md`                                 | offsets+members duality                             |
| `component_visibility_map.md`                            | VM ALL_VISIBLE / ALL_FROZEN, set/clear protocols    |
| `component_free_space_map.md`                            | FSM 3-level tree, hio.c integration                 |
| `component_persistence_and_wal_records.md`               | rmgrlist.h dispatch, every metadata WAL record      |
| `component_checkpoints_and_recovery.md`                  | CreateCheckPoint, CheckPointGuts, StartupXLOG       |
| `component_hooks_and_extensibility.md`                   | object_access_hook, callbacks, custom_rmgr          |

## Catalog inventory

| File                                                    | Catalogs                                            |
|---------------------------------------------------------|-----------------------------------------------------|
| `catalog_inventory/core_relations.md`                    | pg_class, pg_attribute, pg_index, pg_namespace, ...|
| `catalog_inventory/type_system.md`                       | pg_type, pg_cast, pg_range, pg_enum, ...           |
| `catalog_inventory/functions_and_operators.md`           | pg_proc, pg_aggregate, pg_operator, pg_amop, ...   |
| `catalog_inventory/constraints_and_dependencies.md`      | pg_constraint, pg_depend, pg_shdepend, ...         |
| `catalog_inventory/partitioning.md`                      | pg_partitioned_table, pg_inherits                   |
| `catalog_inventory/statistics.md`                        | pg_statistic, pg_statistic_ext, ...                 |
| `catalog_inventory/access_control.md`                    | pg_authid, pg_default_acl, pg_policy, ...           |
| `catalog_inventory/replication_and_publication.md`       | pg_publication, pg_subscription, ...                |
| `catalog_inventory/triggers_and_rewrite.md`              | pg_trigger, pg_event_trigger, pg_rewrite           |
| `catalog_inventory/extensions_and_fdw.md`                | pg_extension, pg_foreign_*, pg_user_mapping        |
| `catalog_inventory/text_search.md`                       | pg_ts_*                                             |
| `catalog_inventory/misc.md`                              | pg_largeobject, pg_description, pg_sequence, ...   |

## SLRU users catalog

| File                                                    | SLRU                                                |
|---------------------------------------------------------|-----------------------------------------------------|
| `slru_users_catalog/clog.md`                             | pg_xact (XactCtl)                                   |
| `slru_users_catalog/subtrans.md`                         | pg_subtrans (SubTransCtl)                           |
| `slru_users_catalog/multixact_offsets.md`                | pg_multixact/offsets                                |
| `slru_users_catalog/multixact_members.md`                | pg_multixact/members                                |
| `slru_users_catalog/commit_ts.md`                        | pg_commit_ts (CommitTsCtl)                          |
| `slru_users_catalog/other_slru_users.md`                 | pg_notify, pg_serial                                |

## WAL record catalog

| File                                                    | rmgr                                                |
|---------------------------------------------------------|-----------------------------------------------------|
| `wal_record_catalog/clog_records.md`                     | RM_CLOG_ID                                          |
| `wal_record_catalog/multixact_records.md`                | RM_MULTIXACT_ID                                     |
| `wal_record_catalog/relmap_records.md`                   | RM_RELMAP_ID                                        |
| `wal_record_catalog/storage_smgr_records.md`             | RM_SMGR_ID                                          |
| `wal_record_catalog/heap_visible_records.md`             | RM_HEAP2_ID (XLOG_HEAP2_VISIBLE only)               |
| `wal_record_catalog/commit_ts_records.md`                | RM_COMMIT_TS_ID                                     |
| `wal_record_catalog/nextoid_and_checkpoint_records.md`   | RM_XLOG_ID                                          |
| `wal_record_catalog/dbase_and_tblspc_records.md`         | RM_DBASE_ID, RM_TBLSPC_ID                           |

## Diagrams (mermaid)

| File                                                    | Topic                                               |
|---------------------------------------------------------|-----------------------------------------------------|
| `diagrams/01_persistence_pipeline.mermaid`               | end-to-end metadata persistence                     |
| `diagrams/02_catalog_stack.mermaid`                      | DDL → pg_*.c → indexing.c → pg_catalog              |
| `diagrams/03_catalog_caches.mermaid`                     | relcache / syscache / catcache layering             |
| `diagrams/04_sinval_distribution.mermaid`                | sinval ring buffer, overflow                        |
| `diagrams/05_slru_disk_layout.mermaid`                   | SLRU segment files, bank-lock partitioning          |
| `diagrams/06_slru_page_state.mermaid`                    | EMPTY → READING → VALID → DIRTY → WRITING            |
| `diagrams/07_clog_page_format.mermaid`                   | 2 b/XID, 4 XIDs/byte, group_lsn                     |
| `diagrams/08_multixact_duality.mermaid`                  | offsets ↔ members two-SLRU split                    |
| `diagrams/09_vm_page_format.mermaid`                     | VM 2 b/heap-page, ALL_VISIBLE + ALL_FROZEN          |
| `diagrams/10_vm_clear_set_protocol.mermaid`              | pin-before-lock, LSN handshake                      |
| `diagrams/11_fsm_three_level_tree.mermaid`               | root, midlevel, leaf                                |
| `diagrams/12_fsm_page_internal_heap.mermaid`             | per-page binary heap                                |
| `diagrams/13_hio_fsm_vm_integration.mermaid`             | RelationGetBufferForTuple ↔ FSM ↔ VM                 |
| `diagrams/14_pg_control_checkpoint_flow.mermaid`         | CheckPointGuts dispatch + UpdateControlFile         |
| `diagrams/15_recovery_sequence.mermaid`                  | ReadControlFile → StartupXLOG → Trim*               |

## Reading order

For first-time readers:

1. `component_catalog_data_model_and_bootstrap.md` — pg_control + bootstrap.
2. `component_slru_framework.md` — SLRU machinery shared by CLOG/Multi/etc.
3. One of `component_clog.md`, `component_multixact.md` — applied SLRU.
4. `component_relmapper.md` — the metadata-of-metadata.
5. `component_catalog_modification_apis.md` + `component_catalog_caches.md` +
   `component_cache_invalidation.md` — the "user-visible" catalog stack.
6. `component_visibility_map.md` + `component_free_space_map.md` — the
   per-page metadata forks.
7. `component_persistence_and_wal_records.md` + `component_checkpoints_and_recovery.md`
   — the integration story.

## Inventory totals (verified)

- 63 pg_catalog tables documented across 12 catalog_inventory files.
- 7 SLRU instances documented across 6 slru_users_catalog files
  (multixact_offsets and multixact_members are kept separate; pg_notify and
  pg_serial share other_slru_users.md).
- 30 WAL record types documented across 8 wal_record_catalog files.
- 15 mermaid diagrams (target was ≥14).
- 15 component_*.md narrative files.
