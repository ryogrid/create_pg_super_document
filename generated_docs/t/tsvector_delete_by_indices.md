# tsvector_delete_by_indices

## Location
src/backend/utils/adt/tsvector_op.c: 464 - 553

## Overview
An internal static function that creates a new TSVector by removing lexemes at specified indices from an existing TSVector.

## Definition


## Detailed Description
The  function performs selective deletion of lexemes from a TSVector based on an array of indices. It creates a new TSVector containing all lexemes except those at the specified positions, preserving the alphabetical ordering and maintaining position/weight information for retained lexemes.

The function operates in several phases:
1. **Index Preprocessing**: Sorts the indices_to_delete array and removes duplicates using qsort and qunique
2. **Memory Allocation**: Allocates memory for the output TSVector (initially overestimating size)
3. **Selective Copying**: Iterates through the source TSVector, copying only lexemes not marked for deletion
4. **Data Preservation**: For each retained lexeme, copies both the lexeme text and any associated position/weight data
5. **Memory Alignment**: Ensures proper alignment of position data using SHORTALIGN
6. **Size Correction**: Sets the final size of the output TSVector based on actual data copied

## Parameters / Member Variables
- : Source TSVector from which to delete lexemes
- : Array of lexeme indices to remove (gets modified by sorting/deduplication)
- : Number of elements in the indices array

## Dependencies
- Functions called/Symbols referenced:
  - qsort (sorts indices array using compare_int)
  - [qunique](../q/qunique.md) (removes duplicate indices using compare_int)  
  - [compare_int](../c/compare_int.md) (comparator function for integer sorting)
  - [palloc0](../p/palloc0.md) (PostgreSQL memory allocation)
  - memcpy (memory copy operations)
  - ARRPTR/STRPTR (TSVector access macros)
  - POSDATALEN (position data length calculation)
  - SHORTALIGN (memory alignment for position data)
  - SET_VARSIZE/CALCDATASIZE (TSVector size management)
- Called from:
  - [tsvector_delete_str](tsvector_delete_str.md) (for single lexeme deletion)
  - [tsvector_delete_arr](tsvector_delete_arr.md) (for multiple lexeme deletion by text array)

## Notes and Other Information
- The indices_to_delete array is modified during execution (sorted and deduplicated)
- Includes bounds checking via Assert to ensure all specified indices are valid
- Preserves position and weight information for all retained lexemes
- Uses memory alignment requirements for position data storage
- Returns a completely new TSVector rather than modifying the input
- Critical for implementing various TSVector deletion operations in PostgreSQL's text search functionality
- Efficiently handles both single and multiple lexeme deletions with O(n) complexity after initial sorting