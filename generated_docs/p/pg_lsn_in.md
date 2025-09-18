# pg_lsn_in

## Location
[src/backend/utils/adt/pg_lsn.c:63-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L63-L79)

## Overview
A PostgreSQL input function that converts a string representation of a Log Sequence Number (LSN) into the internal pg_lsn data type format.

## Definition
```c
Datum pg_lsn_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the standard input conversion function for the pg_lsn data type in PostgreSQL's type system. It acts as a wrapper around pg_lsn_in_internal, providing proper error handling that integrates with PostgreSQL's error reporting system. When parsing fails, it generates appropriate error messages using the PostgreSQL error framework rather than returning error codes.

The function follows PostgreSQL's standard input function convention by taking PG_FUNCTION_ARGS and returning a Datum. It extracts the input string from the function arguments, delegates the actual parsing to pg_lsn_in_internal, and either returns the converted LSN value or throws an error with a descriptive message.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - `str`: Input string containing LSN in "XXXXXXXX/XXXXXXXX" format (accessed via PG_GETARG_CSTRING(0))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_lsn_in_internal](pg_lsn_in_internal.md) (performs the actual parsing)
  - PG_GETARG_CSTRING (extracts string argument)
  - ereturn (PostgreSQL error return mechanism)
  - PG_RETURN_LSN (returns LSN value as Datum)
  - [errcode](../e/errcode.md), errmsg (PostgreSQL error reporting functions)

- Called from (representative examples):
  - [parse_subscription_options](parse_subscription_options.md)
  - [libpqrcv_create_slot](../l/libpqrcv_create_slot.md)

## Notes and Other Information
- This is the official input function registered in PostgreSQL's type system for pg_lsn
- Follows PostgreSQL's function calling convention for type input/output functions
- Generates ERRCODE_INVALID_TEXT_REPRESENTATION errors for invalid input
- Used internally by SQL parsing when LSN literals are encountered
- Essential for subscription and replication functionality where LSN values are specified as strings