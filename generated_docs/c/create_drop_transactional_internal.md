# create_drop_transactional_internal

## Location
[src/backend/utils/activity/pgstat_xact.c:332-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L332-L356)

## Overview
Internal helper function that creates pending statistics drop entries for transactional management of database object statistics creation and deletion.

## Definition
```c
static void create_drop_transactional_internal(PgStat_Kind kind, Oid dboid, Oid objoid, bool is_create)
```

## Detailed Description
This static internal function handles the common logic for both transactional statistics creation and deletion by adding entries to the pending drops list. The function serves as the core implementation for deferred statistics operations that need to be resolved based on transaction outcomes.

The function operates by:
1. Determining the current transaction nesting level using GetCurrentTransactionNestLevel()
2. Ensuring a statistics transaction stack entry exists for this nesting level
3. Allocating a new PgStat_PendingDroppedStatsItem in TopTransactionContext
4. Populating the pending item with the provided statistics information
5. Adding the item to the pending_drops list for the current transaction level

The is_create flag determines the behavior during transaction resolution:
- **is_create = true**: Entry represents a newly created object that should be cleaned up on abort
- **is_create = false**: Entry represents a dropped object whose stats should be removed on commit

This design enables proper transactional semantics for statistics operations, ensuring they are atomic with the underlying database operations.

## Parameters / Member Variables
- `kind`: The type of PostgreSQL statistics object (PgStat_Kind enum)
- `dboid`: Database OID containing the object
- `objoid`: Object OID for the specific database object
- `is_create`: Boolean flag indicating if this is for object creation (true) or deletion (false)

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_Kind](../P/PgStat_Kind.md) (enum type)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (structure type) 
  - [PgStat_PendingDroppedStatsItem](../P/PgStat_PendingDroppedStatsItem.md) (structure type)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [pgstat_get_xact_stack_level](../p/pgstat_get_xact_stack_level.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [dclist_push_tail](../d/dclist_push_tail.md)
  - TopTransactionContext (global memory context)

- Called from (representative examples):
  - [pgstat_create_transactional](../p/pgstat_create_transactional.md) (src/backend/utils/activity/pgstat_xact.c:368)
  - [pgstat_drop_transactional](../p/pgstat_drop_transactional.md) (src/backend/utils/activity/pgstat_xact.c:381)

## Notes and Other Information
- Static function, only accessible within pgstat_xact.c
- Memory allocation occurs in TopTransactionContext to ensure proper transaction-scoped cleanup
- The pending_drops list is processed during transaction commit/abort to execute actual statistics operations
- Essential building block for PostgreSQL's transactional statistics system
- Supports nested transactions through proper nesting level management
- The dual-purpose design (create/drop) with the is_create flag reduces code duplication
- Items added to pending_drops are later processed by pgstat_get_transactional_drops() and pgstat_execute_transactional_drops()