# datumIsEqual

## Location
src/backend/utils/adt/datum.c: 223 - 265

## Overview
Compares two datums for equality using byte-wise comparison, handling both pass-by-value and pass-by-reference types with appropriate size checking.

## Definition


## Detailed Description
The  function performs equality comparison between two datums using a straightforward byte-by-byte comparison approach. The function's behavior depends on the datum storage type:

1. **Pass-by-value types**: Performs direct comparison using the equality operator (), relying on the assumption that each datatype consistently fills any extraneous bits in the Datum representation.

2. **Pass-by-reference types**: 
   - First determines the size of both datums using 
   - If sizes differ, immediately returns false
   - If sizes match, performs byte-wise comparison using 

**Important limitations:**
- The function may return false for different representations of the same logical value
- It will not handle "toasted" (compressed/out-of-line) datums correctly
- The comparison is intentionally kept simple to work safely in aborted transaction contexts

## Parameters / Member Variables
- : First datum to compare
- : Second datum to compare  
- : Boolean indicating whether the type is passed by value (true) or by reference (false)
- : The declared type length (-1 for varlena, positive for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - datumGetSize (called twice for pass-by-reference types)
  - DatumGetPointer
  - memcmp
- Called from (representative examples):
  - equalTupleDescs
  - heap_attr_equals
  - _equalConst
  - find_compatible_trans
  - coerce_type
  - partition_bounds_equal

## Notes and Other Information
- This is a low-level comparison function that does not invoke type-specific equality operators
- The function assumes that datatypes consistently handle padding bits in Datum representations
- For pass-by-value types, alignment within the Datum is not explicitly handled, relying on type consistency
- The simple byte-wise approach is intentional to ensure the function works in error recovery contexts
- TOAST handling is explicitly avoided to prevent issues in aborted transactions
- This function is primarily used for system-level comparisons rather than user-visible equality operations
- Different logical representations of the same value (like different encodings) will be considered unequal