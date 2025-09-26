# PgStat_SubXactStatus

## Location
src/include/utils/pgstat_internal.h: 171 - 195

## Overview
PgStat_SubXactStatus maintains transactional context for statistics operations, tracking pending changes in subtransactions that must be committed or rolled back atomically.

## Definition


## Detailed Description
PgStat_SubXactStatus forms part of PostgreSQL's transactional statistics system, managing statistics changes that must be handled atomically with transaction commits and rollbacks. This structure maintains a stack-based hierarchy corresponding to the subtransaction nesting levels, ensuring that statistics operations can be properly rolled back if a subtransaction aborts.

The structure serves two main purposes: tracking statistics drops for objects that are deleted within transactions, and managing table-level transaction statistics (insertions, deletions, updates) that cannot be immediately applied to the global statistics until the transaction outcome is known. This design ensures that statistics remain consistent even in complex nested transaction scenarios.

The pending_drops list contains statistics entries that should be deleted if the transaction commits, while the first field points to a linked list of per-table transaction statistics. The stack-like organization (via prev pointer) mirrors PostgreSQL's subtransaction stack, allowing proper cleanup and rollback behavior at any nesting level.

## Parameters / Member Variables
- : Integer indicating the subtransaction nesting depth (0 for main transaction, higher for nested)
- : Pointer to the PgStat_SubXactStatus of the parent subtransaction, forming a stack
- : Double-ended circular list of statistics entries to be dropped upon transaction commit
- : Head pointer to a linked list of PgStat_TableXactStatus entries for table-level transactional statistics

## Dependencies
- Functions called/Symbols referenced:
  - dclist_head (double-ended circular list header)
  - PgStat_TableXactStatus (table transaction statistics structure)
- Called from (representative examples):
  - AtEOXact_PgStat (end-of-transaction statistics processing)
  - AtEOSubXact_PgStat (end-of-subtransaction processing)
  - AtPrepare_PgStat (prepared transaction processing)
  - PostPrepare_PgStat (post-prepare transaction handling)
  - pgstat_get_xact_stack_level (transaction level retrieval)
  - create_drop_transactional_internal (transactional drop creation)
  - add_tabstat_xact_level (table statistics transaction level addition)

## Notes and Other Information
- Forms a stack data structure that mirrors PostgreSQL's subtransaction stack for proper nested transaction handling
- Essential for maintaining ACID properties in PostgreSQL's statistics subsystem
- Pending drops are included in commit/abort WAL records to handle replication and crash recovery scenarios
- The structure lives in TopTransactionContext to ensure proper memory management across transaction boundaries
- Statistics changes are deferred until transaction commit to maintain consistency with the underlying data changes
- Supports complex nested subtransaction scenarios where inner transactions may abort while outer transactions commit
- Part of PostgreSQL's broader transactional infrastructure that ensures statistics accuracy even during complex transaction scenarios