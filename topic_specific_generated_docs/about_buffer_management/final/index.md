# PostgreSQL Buffer Management -- Documentation Hub

## About This Documentation

This documentation provides a comprehensive technical reference for PostgreSQL's buffer management subsystem, covering the shared buffer pool, page layout, concurrency control, replacement policies, WAL integration, storage management, and access method interaction. All content is based on the PostgreSQL 17 source tree.

**Total scope:** 30 key symbols, 68 symbols in architecture map, 9 diagrams, 22 documentation files across 13 functional areas.

---

## Reading Guide

### For newcomers to PostgreSQL internals

Start with the [Executive Summary](01_executive_summary.md) for a one-page overview, then read the [Architecture Overview](02_architecture_overview.md) to understand the big picture. Follow the numbered chapters in order -- they progress from high-level concepts to implementation details.

### For experienced PostgreSQL developers

Jump directly to the section you need using the table of contents below, or use the [Symbol Index](appendix_symbol_index.md) to find documentation for a specific function or data structure. The [API Reference](buffer_mgmt_api_reference.md) provides a quick lookup of function signatures grouped by subsystem.

### For performance tuning

Read [Buffer Replacement Policy](07_buffer_replacement_policy.md) for clock sweep and ring buffer behavior, [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) for background writer and checkpoint tuning, and [Deep Dives](15_deep_dives.md) for performance-focused topics. The [GUC Parameters](appendix_guc_parameters.md) appendix lists all relevant configuration knobs.

---

## Table of Contents

### Core Documentation

| # | Chapter | Description |
|---|---------|-------------|
| 1 | [Executive Summary](01_executive_summary.md) | One-page overview for newcomers |
| 2 | [Architecture Overview](02_architecture_overview.md) | System-wide perspective and structural diagrams |
| 3 | [Buffer Pool Architecture](03_buffer_pool_architecture.md) | Shared memory layout, BufferDesc, initialization |
| 4 | [Buffer Lookup and Hash Table](04_buffer_lookup_and_hashtable.md) | Buffer tags, partitioned hash table, locking |
| 5 | [Buffer Access Protocol](05_buffer_access_protocol.md) | ReadBuffer, pin/unpin, content locks |
| 6 | [Page Concurrency Control](06_page_concurrency_control.md) | Lock types, ordering rules, per-operation traces |
| 7 | [Buffer Replacement Policy](07_buffer_replacement_policy.md) | Clock sweep, usage count, free list, ring buffers |
| 8 | [Page Layout and Types](08_page_layout_and_types.md) | PageHeaderData, line pointers, forks, page types |
| 9 | [Dirty Buffer and Writeback](09_dirty_buffer_and_writeback.md) | MarkBufferDirty, bgwriter, checkpoint |
| 10 | [WAL Integration](10_wal_integration.md) | WAL-before-data, LSN management, full-page writes |
| 11 | [Storage Manager](11_storage_manager.md) | smgr, md.c, VFD, relation forks |
| 12 | [Data Movement and Durability](12_data_movement_and_durability.md) | Shared buffers to OS cache to disk, fsync |
| 13 | [Local Buffers](13_local_buffers.md) | Temp tables, per-backend buffer pool |
| 14 | [Access Method Integration](14_access_method_integration.md) | Heap, B-tree, VACUUM, prefetch |
| 15 | [Deep Dives](15_deep_dives.md) | Double buffering, FPW, checksums, crash recovery |

### Appendices and References

| Document | Description |
|----------|-------------|
| [Symbol Index](appendix_symbol_index.md) | Alphabetical symbol reference with source locations |
| [Glossary](appendix_glossary.md) | Buffer management terminology |
| [Data Structures](appendix_data_structures.md) | Key struct definitions |
| [GUC Parameters](appendix_guc_parameters.md) | Configuration parameters reference |
| [Quick Reference](buffer_mgmt_quick_reference.md) | Two-page cheat sheet |
| [API Reference](buffer_mgmt_api_reference.md) | Function signatures by subsystem |
| [Quality Report](quality_report.md) | Coverage metrics and known gaps |

### Diagrams

All diagrams are in Mermaid format under `../diagrams/`:

| Diagram | Description |
|---------|-------------|
| [buffer_pool_layout.mermaid](../diagrams/buffer_pool_layout.mermaid) | Shared memory layout of the buffer pool |
| [readbuffer_flow.mermaid](../diagrams/readbuffer_flow.mermaid) | Complete ReadBuffer() call flow |
| [clock_sweep.mermaid](../diagrams/clock_sweep.mermaid) | Clock sweep victim selection algorithm |
| [pin_lock_protocol.mermaid](../diagrams/pin_lock_protocol.mermaid) | Buffer pin and lock state machine |
| [lock_hierarchy.mermaid](../diagrams/lock_hierarchy.mermaid) | Lock hierarchy and ordering rules |
| [page_layout.mermaid](../diagrams/page_layout.mermaid) | Page internal structure |
| [writeback_pipeline.mermaid](../diagrams/writeback_pipeline.mermaid) | Write-back pipeline from buffer to disk |
| [ring_buffer_strategies.mermaid](../diagrams/ring_buffer_strategies.mermaid) | Ring buffer strategy types and decisions |
| [storage_stack.mermaid](../diagrams/storage_stack.mermaid) | Full storage stack from access methods to OS |

---

## Key Source Files

| File | Lines | Role |
|------|-------|------|
| `src/backend/storage/buffer/bufmgr.c` | ~5800 | Core buffer manager |
| `src/backend/storage/buffer/freelist.c` | ~820 | Replacement policy and ring buffers |
| `src/backend/storage/buffer/buf_table.c` | ~160 | Hash table operations |
| `src/backend/storage/buffer/buf_init.c` | ~190 | Buffer pool initialization |
| `src/backend/storage/buffer/localbuf.c` | ~700 | Local (temp table) buffers |
| `src/backend/storage/page/bufpage.c` | ~1000 | Page operations |
| `src/backend/storage/smgr/smgr.c` | ~600 | Storage manager interface |
| `src/backend/storage/smgr/md.c` | ~1830 | Magnetic disk layer |
| `src/include/storage/buf_internals.h` | ~370 | Internal data structures |
| `src/include/storage/bufmgr.h` | ~380 | Public buffer manager API |
| `src/include/storage/bufpage.h` | ~510 | Page layout definitions |
| `src/include/storage/smgr.h` | ~130 | Storage manager interface |
| `src/backend/storage/buffer/README` | ~400 | Authoritative design document |
