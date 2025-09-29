# AtPrepare_RelationMap

## Location
[src/backend/utils/cache/relmapper.c:588-610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L588-L610)

## Overview
Handles relation mapping during PREPARE TRANSACTION by rejecting any transaction that has modified relation mappings.

## Definition

```c
void
AtPrepare_RelationMap(void)
```
## Detailed Description
The AtPrepare_RelationMap function is called during the PREPARE phase of two-phase commit processing. Currently, PostgreSQL does not support preparing transactions that have made changes to relation mappings.

The function checks all four relation mapping update structures:
- active_shared_updates: Active shared catalog mapping changes
- active_local_updates: Active local catalog mapping changes  
- pending_shared_updates: Pending shared catalog mapping changes
- pending_local_updates: Pending local catalog mapping changes

If any of these structures contains mapping updates (num_mappings != 0), the function raises an ERROR with code ERRCODE_FEATURE_NOT_SUPPORTED, preventing the transaction from being prepared.

This restriction exists because relation mapping changes are complex to handle in the context of two-phase commit. The mapping files need to be updated atomically and consistently across all participating processes, which would require significant additional infrastructure to support properly in the two-phase commit protocol.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - [errcode](../e/errcode.md) (for error code specification)
  - [errmsg](../e/errmsg.md) (for error message formatting)
- Global variables accessed:
  - active_shared_updates (static RelMapFile structure)
  - active_local_updates (static RelMapFile structure)
  - pending_shared_updates (static RelMapFile structure)
  - pending_local_updates (static RelMapFile structure)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md) (in src/backend/access/transam/xact.c)

## Notes and Other Information
- This function implements a deliberate limitation in PostgreSQL's two-phase commit support
- The restriction helps maintain the integrity of the relation mapping system by avoiding the complexity of coordinating mapping changes across multiple phases
- Transactions that need to modify relation mappings (such as VACUUM FULL or CLUSTER on system catalogs) cannot be prepared for two-phase commit
- The error is raised with ERRCODE_FEATURE_NOT_SUPPORTED to clearly indicate this is an intentional limitation rather than a bug
- Future PostgreSQL versions could potentially lift this restriction by implementing proper two-phase commit support for relation mapping changes

## Simplified Source

```c
void AtPrepare_RelationMap(void)
{
    // Check if any relation mapping updates are pending or active
    if (active_shared_updates.num_mappings != 0 ||
        active_local_updates.num_mappings != 0 ||
        pending_shared_updates.num_mappings != 0 ||
        pending_local_updates.num_mappings != 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot PREPARE a transaction that modified relation mapping")));
}
```