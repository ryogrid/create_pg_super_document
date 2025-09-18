# pg_snapshot_in

## Location
[src/backend/utils/adt/xid8funcs.c:420-435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L420-L435)

## Overview
Input function for the pg_snapshot data type that converts a string representation of a snapshot into the internal pg_snapshot structure.

## Definition
```c
Datum pg_snapshot_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The pg_snapshot_in function serves as the input conversion function for PostgreSQL's pg_snapshot data type. It takes a C-string parameter containing the textual representation of a snapshot and converts it into the internal pg_snapshot binary format. This function is part of PostgreSQL's type system infrastructure and is automatically called when converting string literals or text values to pg_snapshot type.

The function delegates the actual parsing work to parse_snapshot(), which handles the complex logic of interpreting the string format and constructing the appropriate pg_snapshot structure.

## Parameters / Member Variables
- `str`: A C-string containing the textual representation of the snapshot to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - [parse_snapshot](parse_snapshot.md)
  - PG_GETARG_CSTRING
  - PG_RETURN_POINTER
- Called from (representative examples):
  - No direct references found in the analyzed codebase (typically called by PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's type system infrastructure for pg_snapshot
- It's typically called automatically by the PostgreSQL engine when type conversion is needed
- The actual parsing logic is implemented in the parse_snapshot helper function
- Uses the standard PostgreSQL function calling convention with PG_FUNCTION_ARGS
- Memory allocation and error handling are managed by the parse_snapshot function
- Located in src/backend/utils/adt/xid8funcs.c:420-435