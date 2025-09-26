# AtEOXact_ComboCid

## Location
[src/backend/utils/time/combocid.c:182-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/combocid.c#L182-L203)

## Overview
Cleans up combo command ID data structures at the end of a transaction, resetting all combo CID tracking state.

## Definition
```c
void AtEOXact_ComboCid(void)
```

## Detailed Description
This function performs end-of-transaction cleanup for the combo command ID subsystem. Combo command IDs are only relevant within the transaction that created them, so all associated data structures can be safely discarded when the transaction ends.

The function resets four key state variables:
- comboHash: Hash table for combo CID lookups
- comboCids: Array storing combo CID mappings
- usedComboCids: Count of used combo CIDs  
- sizeComboCids: Allocated size of combo CID array

The function deliberately avoids calling pfree() on the data structures because they are allocated in TopTransactionContext, which will be automatically destroyed at transaction end anyway.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - No function calls (only resets global variables)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md): During transaction commit cleanup
  - [PrepareTransaction](../P/PrepareTransaction.md): During two-phase commit preparation
  - [AbortTransaction](AbortTransaction.md): During transaction abort cleanup

## Notes and Other Information
- Part of PostgreSQL's transaction cleanup infrastructure
- Avoids explicit memory deallocation since TopTransactionContext handles cleanup
- Essential for preventing combo CID state from leaking between transactions
- Combo CIDs are transaction-local and have no meaning outside their creating transaction
- Called during all transaction termination scenarios (commit, prepare, abort)
- Located in src/backend/utils/time/combocid.c:182-203