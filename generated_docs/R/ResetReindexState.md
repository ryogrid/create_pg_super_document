# ResetReindexState

## Location
[src/backend/catalog/index.c:4152-4180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L4152-L4180)

## Overview
Clears all global reindexing state during transaction or subtransaction abort, resetting the reindex processing flags and pending index lists when the transaction nesting level matches or exceeds the reindex operation level.

## Definition

```c
void
ResetReindexState(int nestLevel)
```
## Detailed Description
ResetReindexState is a public function called during transaction abort scenarios to clean up reindexing state. It compares the provided nest level against the stored reindexingNestLevel to determine if cleanup is needed. When the reindexing nest level is greater than or equal to the abort nest level, it resets all reindexing state including currently reindexed heap/index OIDs, the pending reindexed indexes list, and the nesting level itself. The function is designed to handle subtransaction failures within REINDEX operations without affecting outer-level state.

## Parameters / Member Variables
- : The transaction nesting level at which the abort is occurring

## Dependencies
- Functions called/Symbols referenced:
  - None (only uses global variables and constants)
- Called from (representative examples):
  - [AbortTransaction](../A/AbortTransaction.md)
  - [AbortSubTransaction](../A/AbortSubTransaction.md)
  - REINDEX_REL_FORCE_INDEXES_PERMANENT

## Notes and Other Information
- This is a public function (not static) and can be called from outside the index.c module
- Uses transaction nesting level comparison to determine whether to reset state
- Does not explicitly free pendingReindexedIndexes memory as it should be in transaction-lifespan context
- Handles both transaction and subtransaction abort scenarios
- The function assumes reindexing is not re-entrant, so it only needs to track one level of nesting
- Resets all global reindexing state variables when cleanup is triggered