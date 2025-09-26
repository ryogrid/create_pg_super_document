# PgStat_SubXactStatus

## Location
[src/include/utils/pgstat_internal.h:171-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pgstat_internal.h#L171-L195)

## Overview
PgStat_SubXactStatus maintains transactional context for statistics operations, tracking pending changes in subtransactions that must be committed or rolled back atomically.

## Definition

```c
typedef struct PgStat_SubXactStatus
{
	int			nest_level;		/* subtransaction nest level */

	struct PgStat_SubXactStatus *prev;	/* higher-level subxact if any */

	/*
	 * Statistics for transactionally dropped objects need to be
	 * transactionally dropped as well. Collect the stats dropped in the
	 * current (sub-)transaction and only execute the stats drop when we know
	 * if the transaction commits/aborts. To handle replicas and crashes,
	 * stats drops are included in commit / abort records.
	 */
	dclist_head pending_drops;

	/*
	 * Tuple insertion/deletion counts for an open transaction can't be
	 * propagated into PgStat_TableStatus counters until we know if it is
	 * going to commit or abort.  Hence, we keep these counts in per-subxact
	 * structs that live in TopTransactionContext.  This data structure is
	 * designed on the assumption that subxacts won't usually modify very many
	 * tables.
	 */
	PgStat_TableXactStatus *first;	/* head of list for this subxact */
} PgStat_SubXactStatus;
```
## Detailed Description
PgStat_SubXactStatus forms part of PostgreSQL's transactional statistics system, managing statistics changes that must be handled atomically with transaction commits and rollbacks. This structure maintains a stack-based hierarchy corresponding to the subtransaction nesting levels, ensuring that statistics operations can be properly rolled back if a subtransaction aborts.

The structure serves two main purposes: tracking statistics drops for objects that are deleted within transactions, and managing table-level transaction statistics (insertions, deletions, updates) that cannot be immediately applied to the global statistics until the transaction outcome is known. This design ensures that statistics remain consistent even in complex nested transaction scenarios.

The pending_drops list contains statistics entries that should be deleted if the transaction commits, while the first field points to a linked list of per-table transaction statistics. The stack-like organization (via prev pointer) mirrors PostgreSQL's subtransaction stack, allowing proper cleanup and rollback behavior at any nesting level.

## Parameters / Member Variables
- `nest_level`: Integer indicating the subtransaction nesting depth (0 for main transaction, higher for nested)
- `*prev`: Pointer to the PgStat_SubXactStatus of the parent subtransaction, forming a stack
- `pending_drops`: Double-ended circular list of statistics entries to be dropped upon transaction commit
- `*first`: Head pointer to a linked list of PgStat_TableXactStatus entries for table-level transactional statistics
## Dependencies
- Functions called/Symbols referenced:
  - [dclist_head](../d/dclist_head.md) (double-ended circular list header)
  - [PgStat_TableXactStatus](PgStat_TableXactStatus.md) (table transaction statistics structure)
- Called from (representative examples):
  - [AtEOXact_PgStat](../A/AtEOXact_PgStat.md) (end-of-transaction statistics processing)
  - [AtEOSubXact_PgStat](../A/AtEOSubXact_PgStat.md) (end-of-subtransaction processing)
  - [AtPrepare_PgStat](../A/AtPrepare_PgStat.md) (prepared transaction processing)
  - [PostPrepare_PgStat](PostPrepare_PgStat.md) (post-prepare transaction handling)
  - [pgstat_get_xact_stack_level](../p/pgstat_get_xact_stack_level.md) (transaction level retrieval)
  - [create_drop_transactional_internal](../c/create_drop_transactional_internal.md) (transactional drop creation)
  - [add_tabstat_xact_level](../a/add_tabstat_xact_level.md) (table statistics transaction level addition)

## Notes and Other Information
- Forms a stack data structure that mirrors PostgreSQL's subtransaction stack for proper nested transaction handling
- Essential for maintaining ACID properties in PostgreSQL's statistics subsystem
- Pending drops are included in commit/abort WAL records to handle replication and crash recovery scenarios
- The structure lives in TopTransactionContext to ensure proper memory management across transaction boundaries
- Statistics changes are deferred until transaction commit to maintain consistency with the underlying data changes
- Supports complex nested subtransaction scenarios where inner transactions may abort while outer transactions commit
- Part of PostgreSQL's broader transactional infrastructure that ensures statistics accuracy even during complex transaction scenarios