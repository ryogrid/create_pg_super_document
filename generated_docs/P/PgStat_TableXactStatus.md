# PgStat_TableXactStatus

## Location
src/include/pgstat.h: 210 - 227

## Overview
PgStat_TableXactStatus tracks per-table statistics within the context of a specific subtransaction, maintaining counters for tuple operations and managing the hierarchical relationship of nested transactions.

## Definition


## Detailed Description
PgStat_TableXactStatus is a crucial data structure in PostgreSQL's statistics subsystem that maintains per-table, per-subtransaction statistics. It forms part of a hierarchical system that tracks database activity at different transaction nesting levels. The structure maintains counters for tuple operations (insert, update, delete) and handles special cases like table truncation or dropping. It supports PostgreSQL's nested transaction (savepoint) functionality by maintaining links to parent transactions and sibling transactions at the same nesting level.

## Parameters / Member Variables
- : Counter tracking the number of tuples inserted within the current subtransaction
- : Counter tracking the number of tuples updated within the current subtransaction  
- : Counter tracking the number of tuples deleted within the current subtransaction
- : Boolean flag indicating whether the relation was truncated or dropped in this subtransaction
- : Counter preserving insert statistics from before a truncate/drop operation
- : Counter preserving update statistics from before a truncate/drop operation
- : Counter preserving delete statistics from before a truncate/drop operation
- : Integer indicating the subtransaction nesting depth
- : Pointer to the PgStat_TableXactStatus structure of the parent (higher-level) subtransaction
- : Pointer to the overall PgStat_TableStatus structure for this table
- : Pointer to the next PgStat_TableXactStatus structure at the same subtransaction level

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_Counter
  - PgStat_TableStatus
- Called from (representative examples):
  - pgstat_report_analyze
  - find_tabstat_entry
  - AtEOXact_PgStat_Relations
  - AtEOSubXact_PgStat_Relations
  - add_tabstat_xact_level
  - save_truncdrop_counters
  - restore_truncdrop_counters

## Notes and Other Information
This structure is essential for PostgreSQL's MVCC (Multi-Version Concurrency Control) and nested transaction support. The pre-truncate/drop counters are particularly important for maintaining accurate statistics when DDL operations occur within transactions. The linked list structure (via 'next' pointer) allows multiple tables to be tracked at the same subtransaction level, while the 'upper' pointer maintains the transaction hierarchy for proper rollback and commit handling.