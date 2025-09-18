# JsonValueListInitIterator

## Location
src/backend/utils/adt/jsonpath_exec.c: 3554 - 3579

## Overview
Initializes a JsonValueListIterator structure for sequential iteration over all JSON values in a JsonValueList, handling singleton, list, and empty cases.

## Definition
static void JsonValueListInitIterator(const JsonValueList *jvl, JsonValueListIterator *it)

## Detailed Description
This function prepares a JsonValueListIterator structure for iterating through all JSON values contained in a JsonValueList. It handles three distinct cases:

1. **Singleton case**: When the JsonValueList contains a single value, it sets the iterator's value to the singleton, clears the list pointer, and sets next to NULL.

2. **List case**: When the JsonValueList contains multiple values in a list, it sets the current value to the first list element, stores the list reference, and sets next to point to the second cell for subsequent iteration.

3. **Empty case**: When the JsonValueList is empty (no singleton and list is NIL), it initializes all fields to NULL/NIL.

The function provides the foundation for sequential access to all values in a JsonValueList regardless of its internal representation.

## Parameters / Member Variables
- jvl: Pointer to a const JsonValueList structure to iterate over
- it: Pointer to a JsonValueListIterator structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - JsonValueList (structure type)
  - JsonValueListIterator (structure type)
  - linitial (PostgreSQL list utility function)
  - list_second_cell (PostgreSQL list utility function)
  - NIL (PostgreSQL constant for empty list)
- Called from (representative examples):
  - executeItemOptUnwrapResult
  - executePredicate
  - executeUnaryArithmExpr
  - wrapItemsInArray
  - JsonTableResetRowPattern

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- The iterator must be used with corresponding iteration functions to traverse all values
- Handles the dual representation of JsonValueList efficiently
- Part of the JSON path expression evaluation system in PostgreSQL
- The iterator provides a uniform interface for sequential access regardless of internal storage
- Used extensively in operations that need to process all values in a result set
- The iterator state tracks both the current position and the underlying data structure