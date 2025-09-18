# list_copy_tail

## Location
src/backend/nodes/list.c: 1613 - 1638

## Overview
Creates a shallow copy of a PostgreSQL list excluding the first N elements, effectively returning the 'tail' portion of the list.

## Definition


## Detailed Description
The  function creates a shallow copy of a PostgreSQL List structure starting from the (nskip+1)th element, effectively skipping the first 'nskip' elements. This is the complement operation to . The function performs a shallow copy where only the list structure and element pointers are duplicated.

The function includes robust error handling: negative nskip values are treated as 0 rather than causing an error. If the number of elements to skip equals or exceeds the list length, or if the input list is NIL, the function returns NIL.

The implementation uses pointer arithmetic to start copying from the correct offset in the source list's elements array, making it efficient for extracting tail portions of lists.

## Parameters / Member Variables
- : The source List from which to copy the tail elements. Can be NIL.
- : The number of elements to skip from the beginning. If negative, treated as 0. If greater than or equal to list length, results in NIL return.

## Dependencies
- Functions called/Symbols referenced:
  - [new_list](../n/new_list.md)
  - [check_list_invariants](../c/check_list_invariants.md)
- Called from (representative examples):
  - [get_object_address_opcf](../g/get_object_address_opcf.md)
  - [transformAggregateCall](../t/transformAggregateCall.md)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md)
  - [addRangeTableEntryForTableFunc](../a/addRangeTableEntryForTableFunc.md)
  - [addRangeTableEntryForJoin](../a/addRangeTableEntryForJoin.md)
  - [expandRTE](../e/expandRTE.md)
  - [get_name_for_var_field](../g/get_name_for_var_field.md)

## Notes and Other Information
- This is a shallow copy operation - only the list structure is duplicated, not the data elements
- Negative nskip values are normalized to 0 (the comment suggests this behavior could potentially be changed to elog)
- Safe to call with NIL input
- Uses pointer arithmetic for efficient copying from the correct starting position
- Commonly used in parsing and query processing to remove processed elements from parameter lists
- The copied list maintains the same type as the original
- Returns NIL if nskip >= oldlist->length