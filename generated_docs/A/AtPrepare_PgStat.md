# AtPrepare_PgStat

## Location
[src/backend/utils/activity/pgstat_xact.c:189-208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L189-L208)

## Overview
Saves the transactional statistics state during two-phase commit (2PC) transaction preparation, preserving statistics information for later commit or abort operations.

## Definition
```c
void AtPrepare_PgStat(void)
```

## Detailed Description
AtPrepare_PgStat is part of PostgreSQL's two-phase commit protocol implementation for the statistics subsystem. When a transaction is prepared for two-phase commit, this function ensures that the accumulated statistics state is properly preserved so that it can be later processed when the transaction is either committed or aborted through COMMIT PREPARED or ROLLBACK PREPARED commands. The function validates that it's being called at the top-level transaction (nest_level == 1) and delegates the actual statistics preservation to specialized relation handling functions.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [AtPrepare_PgStat_Relations](AtPrepare_PgStat_Relations.md)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (struct type)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md) (src/backend/access/transam/xact.c:2617)

## Notes and Other Information
- This function is specifically designed for two-phase commit (2PC) protocol support
- It operates on the global pgStatXactStack which maintains transaction statistics state
- The function validates that it's being called at the correct transaction nesting level (top-level only)
- Unlike regular transaction completion, prepare operations must preserve the statistics state rather than finalizing it
- The function only handles relation statistics preparation; other statistics types may not require special preparation handling
- This is part of PostgreSQL's distributed transaction support, allowing transactions to be prepared on multiple nodes before final commit

## Simplified Source

```c
void AtPrepare_PgStat(void)
{
    PgStat_SubXactStatus *xact_state;

    // Get the current transaction statistics state
    xact_state = pgStatXactStack;
    if (xact_state != NULL)
    {
        // Verify we're at the top-level transaction
        Assert(xact_state->nest_level == 1);
        Assert(xact_state->prev == NULL);

        // Save relation statistics for the prepared transaction
        AtPrepare_PgStat_Relations(xact_state);
    }
}
```