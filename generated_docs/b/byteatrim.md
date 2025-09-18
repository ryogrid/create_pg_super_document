# byteatrim

## Location
src/backend/utils/adt/oracle_compat.c: 617 - 643

## Overview
The byteatrim function removes specified bytes from both the front and back of a bytea (binary data) value based on a set of bytes to be removed.

## Definition
```c
Datum byteatrim(PG_FUNCTION_ARGS)
```

## Detailed Description
byteatrim is a PostgreSQL built-in function that performs bidirectional trimming of binary data (bytea type). It takes two bytea arguments: a source binary string and a set of bytes to remove. The function removes bytes from both the beginning and end of the binary data, continuing until it encounters the first byte that is not present in the removal set. This function is the binary data equivalent of the text btrim function and is part of PostgreSQL's Oracle compatibility layer.

## Parameters / Member Variables
- `string` (bytea): The input binary data to be trimmed
- `set` (bytea): The set of bytes to remove from the front and back of the binary data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (PostgreSQL macro for getting bytea arguments)
  - [dobyteatrim](../d/dobyteatrim.md) (core binary trimming logic function)
  - PG_RETURN_BYTEA_P (PostgreSQL macro for returning bytea values)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's Oracle compatibility layer for binary data
- Located in src/backend/utils/adt/oracle_compat.c:617-643
- Cloned from btrim and modified to work with binary data (bytea type)
- Uses the dobyteatrim helper function with both front and back trimming enabled (true, true parameters)
- Operates on raw binary data without character encoding considerations
- Follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS and related macros