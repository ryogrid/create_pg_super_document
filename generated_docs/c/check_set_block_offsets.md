# check_set_block_offsets

## Location
src/test/modules/test_tidstore/test_tidstore.c: 220 - 313

## Overview
A comprehensive verification function that validates the correctness of TID (tuple identifier) storage operations by comparing stored TIDs against verification arrays using multiple access methods.

## Definition


## Detailed Description
This function performs thorough validation of TidStore functionality by implementing a multi-stage verification process. It checks TID storage integrity through three different approaches: direct member lookup, comprehensive block scanning, and iteration-based retrieval. The function ensures that all three methods return consistent results and match the original verification data.

The verification process includes: (1) checking that all inserted TIDs are present using TidStoreIsMember, (2) performing exhaustive lookups across all possible offsets for each block to build a lookup array, (3) iterating through the TidStore to collect all stored TIDs, and (4) comparing all three datasets after sorting to ensure consistency.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Operates on global  structure containing verification arrays
- Uses global  for TidStore operations

## Dependencies
- Functions called/Symbols referenced:
  - check_tidstore_available - Validates tidstore availability
  - TidStoreIsMember - Checks if a TID exists in the store
  - TidStoreLockShare/TidStoreUnlock - Shared locking for thread-safe read operations
  - TidStoreBeginIterate/TidStoreIterateNext/TidStoreEndIterate - Iterator interface for TidStore
  - ItemPointerGetBlockNumber/ItemPointerGetOffsetNumber - Extract components from TIDs
  - ItemPointerSet - Construct TID values
  - qsort - Sort arrays for comparison
  - itemptr_cmp - Custom comparison function for ItemPointer sorting
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