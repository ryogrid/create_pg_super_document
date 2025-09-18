# union_tuples

## Location
src/backend/access/brin/brin.c: 2022 - 2162

## Overview
Merges two BRIN tuples by adjusting the first deformed tuple to be consistent with the summary values in both tuples, effectively creating a union of their summarized data ranges.

## Definition
static void union_tuples(BrinDesc *bdesc, BrinMemTuple *a, BrinTuple *b)

## Detailed Description
This function performs a sophisticated merge operation between two BRIN tuples, combining their summary information to create a unified summary that encompasses both data ranges. The function modifies the first tuple (a) in-place to contain the union of both summaries.

The merge process handles several important cases:
1. **Empty range optimization**: If either tuple represents an empty range, the function can skip expensive per-key union operations
2. **Null handling**: Properly manages null values and tracks whether nulls are present in the combined summary
3. **Data copying**: When one tuple is empty and the other is not, it efficiently copies data instead of performing union operations
4. **Per-key merging**: For non-empty ranges, calls index-specific union functions for each key to properly merge summary values

The function uses a temporary memory context to avoid memory leaks during the deformation of tuple b and subsequent operations.

## Parameters / Member Variables
- : BRIN descriptor containing index metadata and operator information for each indexed column
- : Target BrinMemTuple (in-memory deformed tuple) that will be modified to contain the union result
- : Source BrinTuple (on-disk format) whose summary values will be merged into tuple a

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates temporary memory context for safe memory management
  - brin_deform_tuple: Converts on-disk tuple b to in-memory format for processing
  - datumCopy: Creates copies of datum values with proper memory management
  - index_getprocinfo: Retrieves index-specific union function for each key
  - FunctionCall3Coll: Calls the union function with collation support
  - MemoryContextDelete: Cleans up temporary memory context
  - BrinDesc, BrinMemTuple, BrinTuple: Core BRIN data structures
  - BrinValues, BrinOpcInfo: Per-column summary and operator information structures

- Called from (representative examples):
  - summarize_range: During BRIN index maintenance and summarization
  - _brin_parallel_merge: During parallel BRIN index construction merge phase

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- The function is optimized for common cases where one or both ranges are empty, avoiding unnecessary computation
- Uses a temporary memory context ("brin union") to ensure proper cleanup and avoid memory fragmentation
- Handles both regular null semantics and special BRIN null handling based on operator class configuration
- The union operation is performed per-key using index access method specific union functions
- Critical for BRIN index maintenance operations like vacuuming and parallel index construction
- The first parameter (a) is modified in-place, making this function destructive to its first argument
- Properly handles type-specific copying using typbyval and typlen information from the type cache