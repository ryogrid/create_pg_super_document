# pg_ls_tmpdir_1arg

## Location
[src/backend/utils/adt/genfile.c:678-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L678-L686)

## Overview
A SQL-callable function that lists files in the temporary directory of a user-specified tablespace.

## Definition
```c
Datum pg_ls_tmpdir_1arg(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around the generic pg_ls_tmpdir function, designed to list temporary files in a specific tablespace's pgsql_tmp directory. Unlike pg_ls_tmpdir_noargs, this function accepts a tablespace OID as an argument, allowing users to examine temporary files in any tablespace rather than just the default one. It extracts the tablespace OID from the function arguments and passes it to the core pg_ls_tmpdir function.

## Parameters / Member Variables
- Takes one argument via PG_FUNCTION_ARGS: tablespace OID (extracted using PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_tmpdir](pg_ls_tmpdir.md) (core listing functionality)
  - PG_GETARG_OID (macro to extract OID argument)
- Called from (representative examples):
  - SQL queries via function call interface

## Notes and Other Information
- This function is exported (not static) and callable from SQL
- Provides flexibility to examine temporary files in any valid tablespace
- More versatile than pg_ls_tmpdir_noargs but requires knowledge of tablespace OIDs
- Part of PostgreSQL's administrative function suite for comprehensive temporary file monitoring
- Useful for database administrators managing multiple tablespaces who need to monitor temporary file usage across different storage locations