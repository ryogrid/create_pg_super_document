# int4in

## Location
[src/backend/utils/adt/int.c:287-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L287-L297)

## Overview
Converts a string representation of an integer into PostgreSQL's internal int4 (32-bit signed integer) data type.

## Definition
```c
Datum int4in(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the text input handler for PostgreSQL's int4 data type, converting string representations of integers into the internal 32-bit signed integer format. It delegates the actual parsing and validation to pg_strtoint32_safe, which provides comprehensive error checking for invalid syntax, overflow conditions, and other parsing errors. The function supports soft error handling through the error context mechanism.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `num`: Input C-string containing the integer representation to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strtoint32_safe](../p/pg_strtoint32_safe.md) (safe string to int32 conversion with error context)
  - `PG_RETURN_INT32` (return int32 macro)
- Called from (representative examples):
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (in jsonpath_exec.c:1349, 1577)
  - [inet_client_port](inet_client_port.md) (in network.c:1780)
  - [inet_server_port](inet_server_port.md) (in network.c:1852)
  - [pg_stat_get_backend_client_port](../p/pg_stat_get_backend_client_port.md) (in pgstatfuncs.c:964)

## Notes and Other Information
- Part of PostgreSQL's type input/output system for int4 data type
- Supports the full range of 32-bit signed integers (-2,147,483,648 to 2,147,483,647)
- Uses pg_strtoint32_safe for robust parsing with comprehensive error handling
- Commonly used throughout PostgreSQL for converting text to integer values
- Essential function for SQL parsing, data import, and user input processing