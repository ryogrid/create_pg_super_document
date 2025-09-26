# pgstat_get_xact_stack_level

## Location
src/backend/utils/activity/pgstat_xact.c: 236 - 269

## Overview
Ensures that a statistics transaction stack entry exists for the specified transaction nesting level, creating and initializing it if necessary.

## Definition
```c
PgStat_SubXactStatus *pgstat_get_xact_stack_level(int nest_level)
```

## Detailed Description
This function manages the transaction stack for PostgreSQL statistics by ensuring that a PgStat_SubXactStatus entry exists for the given transaction nesting level. The function operates as follows:

1. Checks if the current stack top (pgStatXactStack) matches the requested nest_level
2. If no match or stack is empty, allocates a new PgStat_SubXactStatus structure in TopTransactionContext
3. Initializes the new entry with:
   - Empty pending_drops list using dclist_init()
   - The specified nest_level
   - Links to the previous stack entry (forming a stack)
   - NULL first pointer for table transaction status
4. Updates the global pgStatXactStack to point to the new entry
5. Returns the appropriate PgStat_SubXactStatus structure

The function implements a lazy allocation strategy, only creating stack entries when needed, and maintains proper stack ordering for nested transactions.

## Parameters / Member Variables
- `nest_level`: The transaction nesting level for which a stack entry is needed (higher numbers indicate deeper nesting)

## Dependencies
- Functions called/Symbols referenced:
  - PgStat_SubXactStatus (structure type)
  - MemoryContextAlloc
  - dclist_init
  - TopTransactionContext (global memory context)
  - pgStatXactStack (global variable)

- Called from (representative examples):
  - AtEOSubXact_PgStat_Relations (src/backend/utils/activity/pgstat_relation.c:643)
  - add_tabstat_xact_level (src/backend/utils/activity/pgstat_relation.c:926)
  - AtEOSubXact_PgStat_DroppedStats (src/backend/utils/activity/pgstat_xact.c:145)
  - create_drop_transactional_internal (src/backend/utils/activity/pgstat_xact.c:339)

## Notes and Other Information
- Memory allocation occurs in TopTransactionContext to ensure proper cleanup at transaction end
- The function maintains a stack structure where newer entries point to older ones via the prev pointer
- Stack entries are created on-demand rather than pre-allocated for all possible nesting levels
- Essential for proper handling of nested transactions and savepoints in PostgreSQL's statistics system
- The pending_drops list is initialized empty and populated as transactional drops occur