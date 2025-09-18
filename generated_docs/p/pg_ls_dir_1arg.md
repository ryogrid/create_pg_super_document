# pg_ls_dir_1arg

## Location
[src/backend/utils/adt/genfile.c:558-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L558-L569)

## Overview
A single-argument wrapper function for pg_ls_dir that provides compatibility with PostgreSQL's built-in function argument validation system.

## Definition


## Detailed Description
The  function is a simple wrapper around the main  function that accepts only one argument (the directory path). This wrapper exists specifically to satisfy PostgreSQL's opr_sanity checks, which require that all built-in SQL functions sharing the same implementing C function must take the same number of arguments.

This function directly delegates to pg_ls_dir by passing through the entire fcinfo structure, which allows pg_ls_dir to handle the actual directory listing logic while providing a separate entry point for single-argument calls.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing the single directory path argument

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ls_dir](pg_ls_dir.md) (the main implementation function that performs the actual directory listing)
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This is a compliance wrapper required by PostgreSQL's system catalog sanity checks
- The wrapper pattern allows the same C function to support both single-argument and multi-argument variants
- Provides backward compatibility for SQL code that only needs basic directory listing without optional parameters
- The function passes through all function call information unchanged, allowing pg_ls_dir to determine the actual argument count