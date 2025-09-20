# StartPrepare

## Location
[src/backend/access/transam/twophase.c:1049-1141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1049-L1141)

## Overview
StartPrepare initializes the two-phase commit state file preparation process by creating data structures and inserting the 2PC file header record.

## Definition

```c
void
StartPrepare(GlobalTransaction gxact)
```
## Detailed Description
StartPrepare begins the process of preparing a two-phase commit state file for a given global transaction. It initializes the linked list data structure used to accumulate state data, creates and populates the two-phase file header with transaction metadata, and saves initial state information including subtransactions, file deletions, statistics, and cache invalidation messages. The function sets up the foundation for the state file that will be completed by EndPrepare.

## Parameters / Member Variables
- : GlobalTransaction structure containing the transaction to be prepared, including transaction ID, GID, owner, and timing information

## Dependencies
- Functions called/Symbols referenced:
  - GetPGProcByNumber
  - [xactGetCommittedChildren](../x/xactGetCommittedChildren.md)
  - [smgrGetPendingDeletes](../s/smgrGetPendingDeletes.md)
  - pgstat_get_transactional_drops
  - [xactGetCommittedInvalidationMessages](../x/xactGetCommittedInvalidationMessages.md)
  - [save_state_data](../s/save_state_data.md)
  - [GXactLoadSubxactData](../G/GXactLoadSubxactData.md)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Initializes the global  data structure used throughout the 2PC preparation process
- Creates TwoPhaseFileHeader with magic number TWOPHASE_MAGIC for file format identification
- Handles various transaction artifacts: subtransactions, file deletions (commit/abort), statistics drops, and cache invalidation messages
- The total_len field in the header is filled in later by EndPrepare
- Memory allocation uses palloc/palloc0 for PostgreSQL memory management
- Part of the two-phase commit protocol implementation in PostgreSQL