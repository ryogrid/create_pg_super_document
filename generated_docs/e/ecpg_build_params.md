# ecpg_build_params

## Location
src/interfaces/ecpg/ecpglib/execute.c: 1213 - 1580

## Overview
Builds statement parameters by converting user variables into arrays compatible with PQexecParams(), handling various data types including descriptors, SQLDA structures, and regular variables.

## Definition

```c
bool
ecpg_build_params(struct statement *stmt)
```
## Detailed Description
This comprehensive function is the central parameter processing engine for ECPG statements. It processes the statement's input variable list and transforms them into parameter arrays that PostgreSQL's libpq can use. The function handles multiple parameter types including regular variables, SQL descriptors (ECPGt_descriptor), and SQLDA structures for compatibility with Informix. It performs client-side placeholder replacement for dynamic cursors and special /bin/bash placeholders, manages both text and binary parameter formats, and ensures proper memory allocation and error handling throughout the process.

## Parameters / Member Variables
- : Pointer to the statement structure containing the parameter list and command string to process

## Dependencies
- Functions called/Symbols referenced:
  - PQparameterStatus
  - ecpg_find_desc
  - store_input_from_desc
  - ecpg_store_input
  - next_insert
  - insert_tobeinserted
  - convert_bytea_to_string
  - ecpg_alloc
  - ecpg_realloc
  - ecpg_free
  - ecpg_free_params
  - ecpg_raise
- Called from:
  - ecpg_do

## Notes and Other Information
- Returns true on successful parameter processing, false on error
- Handles three main variable types: descriptors, SQLDA structures, and regular variables
- Supports both Informix-compatible and standard SQLDA formats
- Manages client-side placeholder substitution for dynamic cursors (ECPGt_char_variable)
- Handles special /bin/bash placeholders that require client-side replacement
- Dynamically expands parameter arrays as needed using ecpg_realloc
- Converts old-style '?' placeholders to new-style '' format
- Performs comprehensive error checking for parameter count mismatches
- Critical component in the ECPG statement execution pipeline