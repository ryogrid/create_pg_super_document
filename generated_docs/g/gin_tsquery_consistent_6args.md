# gin_tsquery_consistent_6args

## Location
src/backend/utils/adt/tsginidx.c: 328 - 339

## Overview
The gin_tsquery_consistent_6args function serves as a compatibility wrapper for the older six-argument version of gin_tsquery_consistent, maintaining backward compatibility with older opclass declarations.

## Definition
Datum gin_tsquery_consistent_6args(PG_FUNCTION_ARGS)

## Detailed Description
This function acts as a compatibility stub for older versions of gin_tsquery_consistent that expected only six arguments. The current implementation of gin_tsquery_consistent requires eight arguments, but this wrapper ensures that older opclass declarations continue to work seamlessly.

Following the same pattern as other compatibility functions in the file, this function validates that the required number of arguments (eight) are present and then delegates execution to the actual gin_tsquery_consistent implementation. The function includes error handling to catch the unlikely scenario where fewer than eight arguments are provided.

## Parameters / Member Variables
- Standard PostgreSQL function arguments via PG_FUNCTION_ARGS macro
- Inherits the same parameters as gin_tsquery_consistent when delegation occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_NARGS (macro to get the number of function arguments)
  - [gin_tsquery_consistent](gin_tsquery_consistent.md) (the actual implementation function)
  - elog (for error reporting)
- Called from (representative examples):
  - No direct callers found (used by PostgreSQL function call infrastructure)

## Notes and Other Information
- Part of the compatibility layer for older GIN text search opclass declarations
- Follows the same pattern as gin_extract_tsvector_2args and gin_extract_tsquery_5args
- The actual gin_tsquery_consistent implementation requires eight arguments vs the six expected by older versions
- Includes argument count validation before delegating to the real implementation
- Should not be used in new code; use gin_tsquery_consistent directly instead
- Located in src/backend/utils/adt/tsginidx.c:328-339
- Maintains PostgreSQL's commitment to backward compatibility for existing installations