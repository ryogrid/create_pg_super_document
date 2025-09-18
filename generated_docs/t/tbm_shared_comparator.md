# tbm_shared_comparator

## Location
src/backend/nodes/tidbitmap.c: 1438 - 1460

## Overview
A comparator function used for sorting PagetableEntry indices in shared TID bitmaps based on block numbers.

## Definition


## Detailed Description
This function serves as a comparison callback for sorting operations on shared TID bitmaps. Unlike direct PagetableEntry comparisons, this function works with indices into a PagetableEntry array. It takes two integer indices, retrieves the corresponding PagetableEntry structures from the base array provided in the arg parameter, and compares their block numbers. This indirection is necessary when working with shared memory structures where direct pointer comparisons are not feasible.

## Parameters / Member Variables
- `left`: Pointer to the first integer index to compare
- `right`: Pointer to the second integer index to compare  
- `arg`: Pointer to the base PagetableEntry array used for index resolution

## Dependencies
- Functions called/Symbols referenced:
  - [PagetableEntry](../P/PagetableEntry.md) (struct type)
- Called from (representative examples):
  - [tbm_prepare_shared_iterate](tbm_prepare_shared_iterate.md)
  - [TBMSharedIterator](../T/TBMSharedIterator.md) (context)

## Notes and Other Information
- This is a static function used internally within the tidbitmap module
- The function follows the standard C qsort comparator interface, returning -1, 0, or 1
- Essential for maintaining sorted order in shared bitmap iterations across parallel workers
- Works with indices rather than direct pointers to support shared memory architecture