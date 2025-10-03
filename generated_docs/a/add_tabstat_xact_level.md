# add_tabstat_xact_level

## Location
[src/backend/utils/activity/pgstat_relation.c:917-943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L917-L943)

## Overview
Creates a new transaction state record for tracking table statistics at a specific transaction nesting level, establishing the necessary data structures to track table modifications within savepoints and subtransactions.

## Definition

```c
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
```
## Detailed Description
This function creates and initializes a new  structure to track table statistics changes at a specific transaction nesting level. It manages the hierarchical transaction state stack by:

1. Ensuring the transaction stack level exists by calling 
2. Allocating a new  structure in the top transaction context
3. Linking the new transaction state into both the per-table transaction chain and the global transaction stack
4. Setting up proper parent-child relationships for nested transaction rollback support

The function is essential for PostgreSQL's statistics tracking system to properly handle savepoints and subtransactions, allowing statistics changes to be rolled back when subtransactions abort.

## Parameters / Member Variables
- `*pgstat_info`: Pointer to the table's statistics status structure that needs transaction-level tracking
- `nest_level`: The transaction nesting level (0 for main transaction, higher values for savepoints/subtransactions)
## Dependencies
- Functions called/Symbols referenced:
  - : Ensures the transaction stack level exists
  - : Allocates zeroed memory in TopTransactionContext
  - : Transaction-specific table statistics structure
  - : Per-subtransaction state structure
  - : Main table statistics tracking structure
- Called from (representative examples):
  - : The primary caller that determines when new transaction levels are needed

## Notes and Other Information
- The function is static and only used internally within the statistics relation module
- Memory allocation uses TopTransactionContext to ensure proper cleanup when transactions end
- The linking strategy maintains both forward (next) and backward (upper) chains for efficient traversal
- This is part of PostgreSQL's sophisticated statistics tracking system that must handle complex transaction scenarios including nested savepoints

## Simplified Source

```c
static void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level) {
    // Ensure the transaction stack level exists for this nesting level
    PgStat_SubXactStatus *xact_state = pgstat_get_xact_stack_level(nest_level);

    // Create a new per-table transaction state record
    PgStat_TableXactStatus *trans = (PgStat_TableXactStatus *)
        MemoryContextAllocZero(TopTransactionContext, sizeof(PgStat_TableXactStatus));

    // Set up the transaction state linkages
    trans->nest_level = nest_level;
    trans->upper = pgstat_info->trans;        // Link to previous level
    trans->parent = pgstat_info;              // Link back to table info
    trans->next = xact_state->first;          // Link to transaction stack

    // Update the chain pointers
    xact_state->first = trans;
    pgstat_info->trans = trans;
}
```