# pg_ls_tmpdir_noargs

## Location
[src/backend/utils/adt/genfile.c:668-677](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L668-L677)

## Overview
A SQL-callable function that lists files in the temporary directory of the default tablespace without requiring any arguments.

## Definition
```c
Datum pg_ls_tmpdir_noargs(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around the generic pg_ls_tmpdir function, specifically designed to list temporary files in the default tablespace's pgsql_tmp directory. It provides a convenient no-argument interface for SQL users who want to examine temporary files in the standard location without needing to specify a tablespace OID. The function automatically uses DEFAULTTABLESPACE_OID as the tablespace parameter.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_tmpdir](pg_ls_tmpdir.md) (core listing functionality)
  - DEFAULTTABLESPACE_OID (constant for default tablespace)
- Called from (representative examples):
  - SQL queries via function call interface

## Notes and Other Information
- This function is exported (not static) and callable from SQL
- Specifically targets the default tablespace, making it the most commonly used variant
- Part of PostgreSQL's administrative function suite for monitoring temporary file usage
- Useful for database administrators to monitor temporary file accumulation in the default location