# ResetLogicalStreamingState

## Location
[src/backend/replication/logical/logical.c:1969-1978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1969-L1978)

## Overview
Clears logical streaming state variables during transaction or subtransaction abort to reset the system to a clean state.

## Definition
void ResetLogicalStreamingState(void)

## Detailed Description
This function is responsible for resetting global state variables used during logical replication streaming when a transaction or subtransaction aborts. It ensures that any partially processed logical replication state is properly cleared to prevent inconsistencies or stale data from affecting subsequent operations.

The function resets two critical global variables:
1. CheckXidAlive - tracks transaction IDs that need to be validated as still active
2. bsysscan - indicates whether a bootstrap system scan is in progress

This cleanup is essential during abort scenarios to ensure that the logical replication subsystem returns to a consistent state and doesn't carry over partially processed transaction information.

## Parameters / Member Variables
None - this is a void function with no parameters

## Dependencies
- Functions called/Symbols referenced:
  - InvalidTransactionId (constant)
  - Global variables: CheckXidAlive, bsysscan
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AbortSubTransaction](../A/AbortSubTransaction.md)

## Notes and Other Information
- Simple cleanup function with minimal overhead
- Critical for maintaining logical replication consistency during error conditions
- Part of the transaction abort cleanup protocol
- The CheckXidAlive variable is declared in src/backend/access/transam/xact.c
- The bsysscan variable is also declared in src/backend/access/transam/xact.c and used by the index scanning subsystem

## Simplified Source

```c
// Simplified version of ResetLogicalStreamingState
void ResetLogicalStreamingState(void) {
    // Clear transaction ID tracking for logical replication
    CheckXidAlive = InvalidTransactionId;

    // Reset bootstrap system scan flag
    bsysscan = false;
}
```

Key simplifications made:
- Added descriptive comments explaining the purpose of each state reset
- Maintained the complete original logic as the function is already minimal
- Emphasized the two-step cleanup process: transaction tracking and scan state reset