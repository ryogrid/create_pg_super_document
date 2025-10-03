# regprocedurein

## Location
[src/backend/utils/adt/regproc.c:224-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L224-L277)

## Overview
Converts a procedure name with arguments or numeric OID string to a regprocedure OID type, providing input conversion for the regprocedure data type.

## Definition

```c
Datum
regprocedurein(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function serves as the input conversion function for PostgreSQL's  data type. It accepts either:

1. A procedure name with argument types in the format "proname(args)"
2. A numeric OID as a string
3. A dash ("-") to signify unknown/invalid procedure (OID 0)

The function performs namespace resolution to find matching procedures in the current search path and validates that the argument types exactly match an existing procedure in the pg_proc catalog. If multiple procedures exist with the same name, it uses the argument types to disambiguate and find the exact match.

The function operates in two phases:
1. Parse the input to determine if it's a numeric OID, dash, or procedure signature
2. For procedure signatures, parse the name and arguments, then search the function catalog for exact matches

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Input string containing either a procedure signature "name(arg_types)" or numeric OID
## Dependencies
- Functions called/Symbols referenced:
  - : Handles parsing of "-" or numeric OID inputs
  - : Parses procedure name and argument type list
  - : Retrieves candidate functions matching the name
  - : Checks if system is in bootstrap mode
  - : Error return with context support
- Called from (representative examples):
  - : ACL function name conversion
  - : Type conversion function

## Notes and Other Information
- In bootstrap mode, only numeric OIDs are accepted (no name resolution)
- Supports error contexts for better error reporting in newer PostgreSQL versions
- Uses exact argument type matching - no implicit type conversion during lookup
- The function enforces that exactly one procedure must match the given signature
- Maximum function arguments is limited by FUNC_MAX_ARGS constant
- Part of PostgreSQL's object identifier (OID) type system for referencing catalog objects