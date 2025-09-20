# bttidcmp

## Location
[src/backend/utils/adt/tid.c:230-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L230-L238)

## Overview
A PostgreSQL function that compares two tuple identifiers (TIDs) for B-tree indexing operations, providing the ordering comparison needed for B-tree internal nodes.

## Definition

```c
Datum
bttidcmp(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that performs comparison between two ItemPointer values (TIDs) for use in B-tree indexes. It extracts two ItemPointer arguments from the function call arguments and delegates the actual comparison logic to the  function. This function is essential for B-tree operations on TID columns, enabling proper ordering and searching within B-tree indexes. The function follows PostgreSQL's standard function calling convention using the  macro and returns an integer result indicating the comparison outcome.

## Parameters / Member Variables
- Function uses  macro to access arguments:
  - First argument (): ItemPointer - the first TID to compare
  - Second argument (): ItemPointer - the second TID to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ITEMPOINTER (macro for extracting ItemPointer arguments)
  - [ItemPointerCompare](../I/ItemPointerCompare.md) (performs the actual TID comparison)
  - PG_RETURN_INT32 (macro for returning integer result)
- Called from:
  - No direct references found (likely referenced through function pointer tables or system catalogs for B-tree operations)

## Notes and Other Information
- This function is part of PostgreSQL's TID (tuple identifier) data type support system
- The function name prefix 'bt' indicates it's specifically designed for B-tree index operations
- Returns standard comparison result: negative for less than, 0 for equal, positive for greater than
- Located in src/backend/utils/adt/tid.c:230-238