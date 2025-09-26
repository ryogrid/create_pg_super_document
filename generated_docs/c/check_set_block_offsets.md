# check_set_block_offsets

## Location
[src/test/modules/test_tidstore/test_tidstore.c:220-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_tidstore/test_tidstore.c#L220-L313)

## Overview
A comprehensive verification function that validates the correctness of TID (tuple identifier) storage operations by comparing stored TIDs against verification arrays using multiple access methods.

## Definition

```c
Datum
check_set_block_offsets(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs thorough validation of TidStore functionality by implementing a multi-stage verification process. It checks TID storage integrity through three different approaches: direct member lookup, comprehensive block scanning, and iteration-based retrieval. The function ensures that all three methods return consistent results and match the original verification data.

The verification process includes: (1) checking that all inserted TIDs are present using TidStoreIsMember, (2) performing exhaustive lookups across all possible offsets for each block to build a lookup array, (3) iterating through the TidStore to collect all stored TIDs, and (4) comparing all three datasets after sorting to ensure consistency.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [check_tidstore_available](check_tidstore_available.md) - Validates tidstore availability
  - [TidStoreIsMember](../T/TidStoreIsMember.md) - Checks if a TID exists in the store
  - [TidStoreLockShare](../T/TidStoreLockShare.md)/TidStoreUnlock - Shared locking for thread-safe read operations
  - [TidStoreBeginIterate](../T/TidStoreBeginIterate.md)/TidStoreIterateNext/TidStoreEndIterate - Iterator interface for TidStore
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)/ItemPointerGetOffsetNumber - Extract components from TIDs
  - [ItemPointerSet](../I/ItemPointerSet.md) - Construct TID values
  - qsort - [Sort](../S/Sort.md) arrays for comparison
  - [itemptr_cmp](../i/itemptr_cmp.md) - Custom comparison function for ItemPointer sorting
- Called from (representative examples):
  - No direct references found (likely called via SQL interface in tests)

## Notes and Other Information
- Located in src/test/modules/test_tidstore/test_tidstore.c:220-313
- Implements a rigorous three-way verification strategy to ensure TidStore correctness
- Uses shared locking during read operations to allow concurrent access while maintaining consistency
- Performs exhaustive scanning of all possible offsets (FirstOffsetNumber to MaxOffsetNumber) for thorough validation
- Validates that iteration results match both direct lookups and comprehensive scanning
- Raises detailed ERROR messages with specific TID information when mismatches are detected
- Essential component of the PostgreSQL TidStore testing framework, ensuring reliability of the storage mechanism
- Returns void as verification is the primary purpose rather than data retrieval