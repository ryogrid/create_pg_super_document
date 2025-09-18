# tidin

## Location
[src/backend/utils/adt/tid.c:52-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L52-L118)

## Overview
The `tidin` function is the input conversion function for PostgreSQL's TID (tuple identifier) data type, converting a string representation of a TID into its internal ItemPointer format.

## Definition
```c
Datum tidin(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tidin` function parses a string representation of a TID in the format "(block,offset)" and converts it to an internal ItemPointer structure. The function performs extensive validation to ensure the input string follows the correct syntax and that both block and offset numbers are within valid ranges. It uses PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS) and supports error context for better error reporting.

The function expects input in the format "(block_number,offset_number)" where:
- block_number is a valid BlockNumber (typically 32-bit unsigned integer)
- offset_number is a valid OffsetNumber (16-bit unsigned integer, max USHRT_MAX)
- The parentheses and comma are required delimiters

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_CSTRING(0)`: String representation of TID to parse
- `escontext`: Error context from function call info for enhanced error reporting
- Internal variables:
  - `coord[NTIDARGS]`: Array to store pointers to coordinate substrings (block and offset)
  - `blockNumber`: Parsed block number as BlockNumber type
  - `offsetNumber`: Parsed offset number as OffsetNumber type
  - `result`: Allocated ItemPointer to return

## Dependencies
- Functions called/Symbols referenced:
  - `strtoul`: Standard library function for string to unsigned long conversion
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [ItemPointerSet](../I/ItemPointerSet.md): Sets block and offset in ItemPointer structure
  - `PG_RETURN_ITEMPOINTER`: PostgreSQL macro to return ItemPointer datum
  - `ereturn`: Error return function with context support
- Constants used:
  - `NTIDARGS` (value: 2): Number of expected TID arguments
  - `LDELIM` (value: '('): Left delimiter character
  - `RDELIM` (value: ')'): Right delimiter character  
  - `DELIM` (value: ','): Coordinate delimiter character
- Called from (representative examples):
  - PostgreSQL type system when converting string literals to TID values
  - SQL parsing and execution when TID constants are encountered

## Notes and Other Information
- The function includes special handling for platforms where `unsigned long` is wider than `BlockNumber` to prevent overflow issues
- Comprehensive input validation ensures malformed TID strings result in appropriate error messages
- The function allocates memory for the result ItemPointer using `palloc`
- Error messages follow PostgreSQL standards and include the invalid input for debugging
- The parsing logic is noted to be largely derived from the `boxin()` function implementation