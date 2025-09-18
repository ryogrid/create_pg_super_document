# byteacat

## Location
[src/backend/utils/adt/varlena.c:2938-2952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2938-L2952)

## Overview
byteacat is a PostgreSQL internal function that concatenates two bytea values and returns the result as a new bytea value.

## Definition


## Detailed Description
This function provides bytea concatenation functionality by taking two bytea arguments and returning their concatenation as a new bytea value. It serves as a wrapper around the internal bytea_catenate function, handling the PostgreSQL function calling convention and argument extraction. The function was cloned from textcat and modified for bytea data types.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: First bytea value to concatenate
  - Argument 1: Second bytea value to concatenate

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (to extract bytea arguments)
  - [bytea_catenate](bytea_catenate.md) (performs the actual concatenation)
  - PG_RETURN_BYTEA_P (returns the result bytea)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is a thin wrapper around bytea_catenate, handling the PostgreSQL function interface
- Uses PG_GETARG_BYTEA_PP for potentially packed bytea arguments for memory efficiency
- The function is cloned from textcat but adapted for binary data (bytea) instead of text
- Located in src/backend/utils/adt/varlena.c:2938-2952