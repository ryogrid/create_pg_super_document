# find_hash_columns

## Location
src/backend/executor/nodeAgg.c: 1563 - 1693

## Overview
Computes and configures which columns need to be stored in hash table entries for aggregation, optimizing storage by eliminating unnecessary columns while preserving all required data for grouping and aggregation operations.

## Definition


## Detailed Description
The  function performs crucial optimization for hashed aggregation by determining the minimal set of columns that must be stored in hash table entries. Input tuples from child plan nodes typically contain grouping columns, columns referenced in target lists and qualifications, columns needed for aggregate function computation, and potentially unused columns. This function identifies and retains only the first two types, significantly reducing hash table entry size and improving performance.

The function operates in several phases: First, it calls  to identify which columns are referenced in target lists and qualifications, categorizing them as aggregated or unaggregated. Then, for each grouping set requiring hashing, it builds a comprehensive column mapping that includes both directly-hashed grouping columns and additional needed columns like functionally dependent columns and row-mark ctids.

Special handling is provided for grouping sets scenarios where certain variables might be referenced in target lists for other grouping sets but not needed for the current hash table. The function uses prepare_projection_slot logic to determine which columns can be safely omitted. Column mappings are built to translate between input tuple positions and hash table positions, ensuring efficient access during hash value computation and tuple comparison operations.

## Parameters / Member Variables
- : The AggState execution node containing aggregation configuration, grouping set information, and hash table data structures

## Dependencies
- Functions called/Symbols referenced:
  - find_cols
  - outerPlanState
  - bms_union
  - bms_copy
  - bms_is_member
  - bms_del_member
  - bms_add_member
  - bms_next_member
  - bms_num_members
  - bms_free
  - list_nth
  - list_free
  - lfirst_int
  - ExecTypeFromTL
  - execTuplesHashPrepare
  - ExecAllocTableSlot
- Types referenced:
  - AggState
  - AggStatePerHash
  - Bitmapset
  - TupleDesc
  - AttrNumber
  - EState
- Called from (representative examples):
  - ExecInitAgg

## Notes and Other Information
- Critical for hash aggregation performance as it minimizes hash table entry size by eliminating unused columns
- Handles complex grouping sets scenarios by determining column requirements per grouping set
- Builds column mapping arrays (hashGrpColIdxInput and hashGrpColIdxHash) for efficient translation between input and hash table column positions
- Places grouping columns first in hash table layout for optimal access during hash computation and tuple comparison
- Uses ExecTypeFromTL to create appropriate tuple descriptors for hash table entries
- Supports edge cases like duplicate columns in grpColIdx arrays that can occur with semijoins and DISTINCT operations
- Memory allocation is done in per-query context since the structures persist across ExecReScanAgg calls
- Sets up hash and equality functions through execTuplesHashPrepare for proper tuple comparison
- Creates optimized tuple slots using TTSOpsMinimalTuple for efficient hash table storage