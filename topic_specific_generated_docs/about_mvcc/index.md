# PostgreSQL MVCC Documentation

**Version:** PostgreSQL 17.6 | **Generated:** 2026-03-01

---

## Table of Contents

### Getting Started
- [Executive Summary](01_executive_summary.md) -- What MVCC is, why PostgreSQL uses it, key trade-offs
- [Architecture Overview](02_architecture_overview.md) -- System-wide component interaction, data flow, shared memory

### Core Documentation

1. [Transaction Lifecycle](03_transaction_lifecycle.md)
   - Transaction states, XID allocation, commit/abort paths, subtransactions
   - Key symbols: `StartTransaction`, `CommitTransaction`, `AbortTransaction`, `GetNewTransactionId`

2. [Tuple Versioning](04_tuple_versioning.md)
   - HeapTupleHeaderData, version chains, HOT updates, infomask flags, ComboCID
   - Key symbols: `heap_insert`, `heap_update`, `heap_delete`, `HeapTupleHeaderData`

3. [Visibility Rules](05_visibility_rules.md)
   - HeapTupleSatisfiesMVCC algorithm, hint bits, race condition prevention
   - Key symbols: `HeapTupleSatisfiesMVCC`, `SetHintBits`, `HeapTupleSatisfiesUpdate`

4. [Snapshot Management](06_snapshot_management.md)
   - Snapshot construction, isolation levels, active snapshot stack, reuse optimization
   - Key symbols: `GetSnapshotData`, `GetTransactionSnapshot`, `SnapshotData`, `XidInMVCCSnapshot`

5. [Concurrency Infrastructure](07_concurrency_infrastructure.md)
   - PGPROC, ProcArray, dense arrays, group clearing, SSI overview
   - Key symbols: `PGPROC`, `ProcArrayEndTransaction`, `TransactionIdIsInProgress`

6. [CLOG and Transaction Status](08_clog_transaction_status.md)
   - Two-bit status encoding, SLRU infrastructure, SUB_COMMITTED state, CLOG truncation
   - Key symbols: `TransactionIdSetTreeStatus`, `TransactionIdDidCommit`, `TransactionIdGetStatus`

7. [VACUUM and Freezing](09_vacuum_and_freezing.md)
   - Two-pass strategy, cutoff computation, pruning, freezing, visibility map
   - Key symbols: `vacuum_get_cutoffs`, `heap_page_prune_and_freeze`, `heap_prepare_freeze_tuple`

### Advanced Topics

8. [Deep Dives](10_deep_dives.md)
   - Serializable Snapshot Isolation (SSI) and rw-conflict detection
   - HOT update chains and index implications
   - Freeze map and visibility map optimization
   - MVCC and WAL interaction (crash recovery implications)
   - MultiXact handling

### Appendices and References

- [Symbol Index](appendix_symbol_index.md) -- Alphabetical list of all 74 symbols with source locations
- [Glossary](appendix_glossary.md) -- 40+ MVCC terminology definitions
- [Data Structures](appendix_data_structures.md) -- Key struct definitions with field descriptions
- [Quick Reference Card](mvcc_quick_reference.md) -- 2-page summary for developers
- [API Reference](mvcc_api_reference.md) -- Function signatures grouped by subsystem

### Diagrams

All diagrams in `diagrams/` directory (Mermaid format):

| Diagram | Description |
|---------|-------------|
| [transaction_lifecycle.mermaid](diagrams/transaction_lifecycle.mermaid) | TransState and TBlockState state machines |
| [tuple_version_chain.mermaid](diagrams/tuple_version_chain.mermaid) | INSERT/UPDATE/DELETE version chain with HOT |
| [mvcc_visibility_flowchart.mermaid](diagrams/mvcc_visibility_flowchart.mermaid) | HeapTupleSatisfiesMVCC decision tree |
| [snapshot_acquisition.mermaid](diagrams/snapshot_acquisition.mermaid) | GetSnapshotData ProcArray scanning flow |
| [shared_memory_layout.mermaid](diagrams/shared_memory_layout.mermaid) | PGPROC, ProcGlobal, dense arrays layout |
| [isolation_level_comparison.mermaid](diagrams/isolation_level_comparison.mermaid) | READ COMMITTED vs REPEATABLE READ vs SERIALIZABLE |
| [clog_status_transitions.mermaid](diagrams/clog_status_transitions.mermaid) | CLOG status state machine with SUB_COMMITTED |
| [vacuum_cleanup_flow.mermaid](diagrams/vacuum_cleanup_flow.mermaid) | VACUUM two-pass processing pipeline |

---

## Reading Guide

### By Audience

**New to PostgreSQL internals:**
1. Start with the [Executive Summary](01_executive_summary.md)
2. Read the [Architecture Overview](02_architecture_overview.md)
3. Focus on [Visibility Rules](05_visibility_rules.md) -- the heart of MVCC
4. Review the [Glossary](appendix_glossary.md) as needed

**Investigating a performance issue:**
- ProcArray contention: [Concurrency Infrastructure](07_concurrency_infrastructure.md) and [Snapshot Management](06_snapshot_management.md)
- Table bloat: [VACUUM and Freezing](09_vacuum_and_freezing.md)
- XID wraparound warnings: [Transaction Lifecycle](03_transaction_lifecycle.md) and [VACUUM](09_vacuum_and_freezing.md)
- Serialization failures: [Deep Dives: SSI](10_deep_dives.md)

**Contributing to PostgreSQL MVCC code:**
1. Read chapters 03-09 in order for complete coverage
2. Study the [Deep Dives](10_deep_dives.md) for cross-cutting concerns
3. Use the [API Reference](mvcc_api_reference.md) and [Symbol Index](appendix_symbol_index.md) for lookup
4. Refer to [Data Structures](appendix_data_structures.md) for struct layouts

### Prerequisites

This documentation assumes familiarity with:
- Basic relational database concepts (transactions, isolation, concurrency)
- C programming (for reading code examples)
- PostgreSQL user-level operations (SQL, VACUUM, configuration)

No prior knowledge of PostgreSQL internals is required -- all internal concepts are explained from first principles.

---

## Document Statistics

| Metric | Value |
|--------|-------|
| Core chapters | 10 |
| Appendices and references | 5 |
| Total output files | 16 |
| Symbols documented | 74 (30 in depth) |
| Mermaid diagrams | 8 |
| Source files covered | 17 |
| PostgreSQL version | 17.6 |

---

## Quality Report

See [quality_report.md](quality_report.md) for coverage metrics, validation results, and known gaps.
