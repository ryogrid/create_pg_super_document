# PostPrepare_PgStat

## Location
[src/backend/utils/activity/pgstat_xact.c:209-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L209-L235)

## Overview
Performs cleanup operations for PostgreSQL statistics after a successful PREPARE statement in a two-phase commit protocol, ensuring proper handling of transactional statistics state.

## Definition
```c
void PostPrepare_PgStat(void)
```

## Detailed Description
This function is called after a successful PREPARE statement to clean up the statistics transaction state. Unlike normal transaction commit/abort, PREPARE requires special handling because the transaction outcome is deferred. The function:

1. Accesses the current transaction statistics state from pgStatXactStack
2. Validates that we're at the top-level transaction (nest_level == 1)
3. Delegates relation-specific cleanup to PostPrepare_PgStat_Relations()
4. Clears the transaction stack by setting pgStatXactStack to NULL
5. Throws away any existing statistics snapshot to ensure fresh data

The function does not free transactional memory since it resides in TopTransactionContext and will be automatically cleaned up. Note that AtEOXact_PgStat is not called during PREPARE, making this function essential for proper statistics handling in two-phase commits.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_SubXactStatus](PgStat_SubXactStatus.md) (structure type)
  - [PostPrepare_PgStat_Relations](PostPrepare_PgStat_Relations.md)
  - [pgstat_clear_snapshot](../p/pgstat_clear_snapshot.md)
  - pgStatXactStack (global variable)

- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md) (src/backend/access/transam/xact.c:2675)

## Notes and Other Information
- This function is specifically designed for two-phase commit scenarios
- Memory cleanup is deferred to PostgreSQL's memory context management
- The function assumes single-level transaction nesting during PREPARE
- Statistics snapshots are invalidated to prevent stale data usage
- Part of PostgreSQL's transactional statistics subsystem

## Simplified Source

```c
void PostPrepare_PgStat(void)
{
    PgStat_SubXactStatus *xact_state;

    // Access current transaction statistics state
    xact_state = pgStatXactStack;
    if (xact_state != NULL)
    {
        // Validate we're at top-level transaction
        Assert(xact_state->nest_level == 1);
        Assert(xact_state->prev == NULL);

        // Handle relation-specific statistics cleanup
        PostPrepare_PgStat_Relations(xact_state);
    }

    // Clear the transaction stack
    pgStatXactStack = NULL;

    // Throw away any existing stats snapshot
    pgstat_clear_snapshot();
}
```