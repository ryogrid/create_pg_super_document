# pushValue_internal

## Location
[src/backend/utils/adt/tsquery.c:547-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L547-L579)

## Overview
Internal function that creates and pushes a QueryOperand structure onto the parser state's polish notation stack with validation for size limits and proper field initialization.

## Definition
```c
static void
pushValue_internal(TSQueryParserState state, pg_crc32 valcrc, int distance, int lenval, int weight, bool prefix)
```

## Detailed Description
This static function serves as the core implementation for pushing value operands onto the tsquery parser's polish notation stack. It performs critical validation checks on the input parameters to ensure they don't exceed PostgreSQL's internal limits (MAXSTRPOS and MAXSTRLEN), creates a properly initialized QueryOperand structure, and adds it to the polstr list.

The function includes comprehensive error handling with appropriate error codes and messages when limits are exceeded. It uses the PostgreSQL error context system (ereturn with escontext) to provide proper error reporting that can be handled gracefully by callers. The CRC value provided is used for efficient comparison operations during query processing.

## Parameters / Member Variables
- `state`: TSQueryParserState containing the current parsing context and error handling context
- `valcrc`: CRC32 checksum of the operand value for efficient comparison
- `distance`: Distance parameter for proximity operations
- `lenval`: Length of the operand value
- `weight`: Weight flags for the operand (bit mask indicating which weight classes apply)
- `prefix`: Boolean flag indicating whether this is a prefix match operand

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](palloc0.md) (PostgreSQL memory allocation function)
  - [lcons](../l/lcons.md) (list construction function)
  - ereturn (PostgreSQL error return macro)
  - QueryOperand (query operand structure type)
  - QI_VAL (query item type constant for values)
  - MAXSTRPOS, MAXSTRLEN (PostgreSQL size limit constants)
  - pg_crc32 (PostgreSQL CRC32 type)
- Called from (representative examples):
  - [pushValue](pushValue.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside the tsquery.c module
- The function enforces PostgreSQL's internal limits on operand size and distance values
- Error handling uses the modern PostgreSQL error context system for proper error propagation
- The CRC value enables efficient operand comparison without string operations during query evaluation
- Weight parameter uses a bitmask format to indicate which text search weight classes (A, B, C, D) apply to the operand
- The distance parameter is used for phrase queries and proximity operations
- Memory allocation uses palloc0 to ensure proper zero-initialization of the structure