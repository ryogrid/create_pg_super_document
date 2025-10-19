# pg_lsn_out

## Location
[src/backend/utils/adt/pg_lsn.c:80-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L80-L91)

## Overview
A PostgreSQL output function that converts the internal pg_lsn data type representation into a human-readable string format.

## Definition
```c
Datum pg_lsn_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the standard output conversion function for the pg_lsn data type in PostgreSQL's type system. It takes an internal XLogRecPtr value and formats it as a string in the canonical "XXXXXXXX/XXXXXXXX" hexadecimal format. The function handles the conversion of the 64-bit LSN value into two 32-bit components, formatting them as uppercase hexadecimal numbers separated by a forward slash.

The function allocates memory for the result string using pstrdup to ensure the returned string persists beyond the function call, which is required by PostgreSQL's memory management conventions for output functions.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - `lsn`: XLogRecPtr value to convert (accessed via PG_GETARG_LSN(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (extracts LSN argument from function args)
  - MAXPG_LSNLEN (constant defining maximum length of LSN string)
  - LSN_FORMAT_ARGS (macro to extract high/low 32-bit components)
  - snprintf (C library function for formatted string output)
  - [pstrdup](pstrdup.md) (PostgreSQL memory allocation function)
  - PG_RETURN_CSTRING (returns string as Datum)

- Called from (representative examples):
  - No direct references found (typically called by PostgreSQL's type system)

## Notes and Other Information
- This is the official output function registered in PostgreSQL's type system for pg_lsn
- Follows PostgreSQL's function calling convention for type input/output functions
- Uses uppercase hexadecimal format for consistency with PostgreSQL conventions
- The resulting string is allocated in the current memory context and will be freed automatically
- Essential for displaying LSN values in SQL query results, logs, and user interfaces
- Buffer size is carefully controlled by MAXPG_LSNLEN to prevent overflow

## Simplified Source

```c
Datum pg_lsn_out(PG_FUNCTION_ARGS) {
    // Extract the LSN value from function arguments
    XLogRecPtr lsn = PG_GETARG_LSN(0);
    char buf[MAXPG_LSNLEN + 1];

    // Format as "XXXXXXXX/XXXXXXXX" hexadecimal string
    snprintf(buf, sizeof buf, "%X/%X", LSN_FORMAT_ARGS(lsn));

    // Allocate persistent memory for result and return
    char *result = pstrdup(buf);
    PG_RETURN_CSTRING(result);
}
```