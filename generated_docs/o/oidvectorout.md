# oidvectorout

## Location
[src/backend/utils/adt/oid.c:158-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oid.c#L158-L183)

## Overview
Output function that converts PostgreSQL's internal oidvector format into a string representation of space-separated OIDs.

## Definition

```c
Datum
oidvectorout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a type output function that converts an oidvector data structure from PostgreSQL's internal binary format into its external string representation. This function is the counterpart to  and is part of PostgreSQL's type system infrastructure.

The function iterates through all OID values in the oidvector, formatting each as an unsigned integer and separating them with spaces. It pre-allocates memory based on the assumption that each OID will take at most 12 characters (including sign, 10 digits, and space separator), ensuring sufficient buffer space for the output string.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides:
  - : Input oidvector pointer containing the OID values to convert

## Dependencies
- Functions called/Symbols referenced:
  - oidvector (data type)
  - [palloc](../p/palloc.md) (memory allocation)
  - sprintf (string formatting)
  - PG_RETURN_CSTRING (macro to return C-string result)
- Called from (representative examples):
  - PostgreSQL type system during output conversion
  - SQL query result formatting
  - Data export operations

## Notes and Other Information
- Allocates memory assuming maximum 12 characters per OID (sign + 10 digits + space)
- Uses sprintf for integer-to-string conversion with "%u" format
- Properly handles space separation between OID values (no leading space, spaces between values)
- The resulting string is null-terminated
- Memory allocation follows PostgreSQL's palloc pattern for automatic cleanup
- Efficient single-pass algorithm that builds the output string incrementally