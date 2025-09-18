# calculate_toast_table_size

## Location
src/backend/utils/adt/dbsize.c: 378 - 423

## Overview
A static utility function that calculates the total disk space consumed by a TOAST relation, including both the TOAST table itself and all its associated indexes.

## Definition
```c
static int64 calculate_toast_table_size(Oid toastrelid)
```

## Detailed Description
This function computes the comprehensive disk space usage of a TOAST (The Oversized-Attribute Storage Technique) relation. TOAST is PostgreSQL's mechanism for storing large field values that would otherwise make tuples too large to fit in database pages. The function calculates sizes for all components of a TOAST relation:

1. The main TOAST heap, including all fork types (main data, free space map, visibility map, etc.)
2. All indexes associated with the TOAST table, also including all their fork types

The function iterates through all possible fork numbers (0 to MAX_FORKNUM) for both the TOAST table and its indexes, ensuring complete coverage of all storage components. It uses `RelationGetIndexList` to discover all indexes associated with the TOAST relation and processes each one systematically.

**Important**: This function must only be applied to TOAST relations, as noted in the function comments.

## Parameters / Member Variables
- `toastrelid`: OID of the TOAST relation whose total size is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [relation_open](../r/relation_open.md)
  - [calculate_relation_size](calculate_relation_size.md)
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [relation_close](../r/relation_close.md)
  - [list_free](../l/list_free.md)
  - lfirst_oid
  - MAX_FORKNUM (constant)
- Called from (representative examples):
  - [calculate_table_size](calculate_table_size.md)

## Notes and Other Information
- This is a static function, only accessible within the same compilation unit (dbsize.c)
- Must only be used with TOAST relations - applying it to regular relations would be incorrect
- Calculates comprehensive size including all fork types (main, FSM, VM, init if present)
- Processes all indexes associated with the TOAST relation systematically
- Uses AccessShareLock for safe concurrent access to relations and indexes
- The function is defined in src/backend/utils/adt/dbsize.c:378-423
- Essential component of PostgreSQL's table size calculation system when TOAST storage is involved
- Provides complete accounting of TOAST-related storage overhead, which can be significant for tables with large attributes
- Memory management includes proper cleanup of the index list using list_free()