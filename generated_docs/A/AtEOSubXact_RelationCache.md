# AtEOSubXact_RelationCache

## Location
src/backend/utils/cache/relcache.c: 3389 - 3443

## Overview
Cleans up the relation cache at subtransaction commit or abort, handling subtransaction-specific cleanup operations while preserving the eoxact_list for subsequent main transaction processing.

## Definition


## Detailed Description
This function is the subtransaction counterpart to AtEOXact_RelationCache, handling cleanup operations when a subtransaction commits or aborts. It performs similar scanning and cleanup operations but with subtransaction-specific logic:

1. **In-Progress List Management**: Clears the in_progress_list, which is relevant when aborting due to errors during RelationBuildDesc(). The function asserts that the list should only be non-empty during abort cases since subtransactions don't commit during RelationBuildDesc().

2. **Efficient Scanning**: Uses the same optimization strategy as the main transaction cleanup - either processes only the relations listed in eoxact_list[] or falls back to scanning the entire RelationIdCache hash table if the list overflowed.

3. **Subtransaction-Specific Cleanup**: Calls AtEOSubXact_cleanup() for each relation, passing the specific subtransaction IDs to handle subtransaction state transitions properly.

4. **List Preservation**: Unlike AtEOXact_RelationCache, this function does not reset the eoxact_list because additional cleanup may be needed later during main transaction termination.

The function must be called before processing invalidation messages, similar to its main transaction counterpart.

## Parameters / Member Variables
- : Boolean indicating whether this is a subtransaction commit (true) or abort (false)
- : The SubTransactionId of the subtransaction being terminated
- : The SubTransactionId of the parent subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_search](../h/hash_search.md)
  - [AtEOSubXact_cleanup](AtEOSubXact_cleanup.md)
- Data structures used:
  - HASH_SEQ_STATUS
  - RelIdCacheEnt
  - SubTransactionId
  - HASH_FIND
- Global variables accessed:
  - in_progress_list_len
  - eoxact_list_overflowed
  - eoxact_list
  - eoxact_list_len
  - RelationIdCache
- Called from:
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (in xact.c)
  - [AbortSubTransaction](AbortSubTransaction.md) (in xact.c)

## Notes and Other Information
- This function must be called before processing invalidation messages during subtransaction termination
- Uses the same eoxact_list optimization as the main transaction cleanup for efficiency
- The in_progress_list_len should only be non-zero during subtransaction abort scenarios
- Unlike AtEOXact_RelationCache, this function deliberately does not reset the eoxact_list since more cleanup may be needed later
- The function handles both subtransaction commit and abort cases with appropriate subtransaction ID management
- Subtransactions don't commit during RelationBuildDesc(), which is why the assertion checks that in_progress_list_len is only non-zero during abort