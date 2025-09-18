# tidout

## Location
[src/backend/utils/adt/tid.c:119-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L119-L138)

## Overview
The `tidout` function is the output conversion function for PostgreSQL's TID (tuple identifier) data type, converting an internal ItemPointer structure to its string representation.

## Definition
```c
Datum tidout(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tidout` function converts an internal ItemPointer structure to a human-readable string representation in the format "(block,offset)". This function is the inverse of `tidin` and is used whenever PostgreSQL needs to display TID values to users, such as in query results or debugging output. The function extracts the block number and offset number from the ItemPointer and formats them into a standardized string format.

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_ITEMPOINTER(0)`: ItemPointer structure to convert to string
- Internal variables:
  - `blockNumber`: Block number extracted from ItemPointer
  - `offsetNumber`: Offset number extracted from ItemPointer
  - `buf[32]`: Character buffer to hold the formatted string (sized to accommodate maximum possible TID values)

## Dependencies
- Functions called/Symbols referenced:
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md): Extracts block number from ItemPointer without validation
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md): Extracts offset number from ItemPointer without validation
  - `snprintf`: Standard library function for formatted string output
  - [pstrdup](../p/pstrdup.md): PostgreSQL string duplication function for memory management
  - `PG_RETURN_CSTRING`: PostgreSQL macro to return C string as datum
- Called from (representative examples):
  - PostgreSQL type system when converting TID values to string for display
  - [Query](../Q/Query.md) result formatting when TID columns are selected
  - Debugging and logging functions that need to display TID values

## Notes and Other Information
- The function uses "NoCheck" variants of ItemPointer accessor functions, assuming the input ItemPointer is valid
- Output format is always "(block,offset)" with parentheses and comma as delimiters
- Buffer size of 32 characters is sufficient for maximum possible TID values on all supported platforms
- The function comment suggests that future versions might output TID as a record type instead of a string
- Memory for the returned string is allocated using `pstrdup` to ensure proper PostgreSQL memory context handling