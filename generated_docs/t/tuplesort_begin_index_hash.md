# tuplesort_begin_index_hash

## Location
src/backend/utils/sort/tuplesortvariants.c: 437 - 489

## Overview
Initializes a Tuplesortstate for sorting hash index tuples during hash index creation, organizing tuples by hash codes for efficient bucket distribution.

## Definition


## Detailed Description
This function creates a specialized tuplesort state for hash index creation operations. Unlike other index types that sort by key values, hash indexes sort tuples by their hash codes to distribute them efficiently across hash buckets. The function configures a single-key sort operation where the sort key is the computed hash code rather than the original data values. It sets up hash-specific comparison functions and stores the bucket configuration parameters needed for proper hash index organization.

## Parameters / Member Variables
- : The heap relation being indexed
- : The hash index relation being created
- : Bitmask for high-order hash bucket calculation
- : Bitmask for low-order hash bucket calculation  
- : Maximum number of hash buckets allowed
- : Amount of memory (in KB) available for sorting operations
- : Coordination structure for parallel sorting operations
- : Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_begin_common
  - TuplesortstateGetPublic
  - removeabbrev_index
  - comparetup_index_hash
  - comparetup_index_hash_tiebreak
  - writetup_index
  - readtup_index
- Called from (representative examples):
  - _h_spoolinit (hashsort.c:83)

## Notes and Other Information
- Uses only one sort key (nKeys = 1) since sorting is based on hash codes rather than multiple data attributes
- Creates a TuplesortIndexHashArg structure to store hash-specific parameters including bucket masks
- Enables datum1 optimization since there's only one sort key
- The hash masks and max_buckets parameters control the hash bucket distribution strategy
- Used specifically during CREATE INDEX operations for hash indexes
- Does not require complex sort support setup since comparison is based on simple hash code values
- The sorting organizes tuples for efficient insertion into hash buckets during index build