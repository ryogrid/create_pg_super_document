# gin_extract_tsquery_5args

## Location
src/backend/utils/adt/tsginidx.c: 316 - 327

## Overview
The gin_extract_tsquery_5args function serves as a compatibility wrapper for the older five-argument version of gin_extract_tsquery, maintaining backward compatibility with older opclass declarations.

## Definition  
Datum gin_extract_tsquery_5args(PG_FUNCTION_ARGS)

## Detailed Description
This function exists as a compatibility stub for older versions of gin_extract_tsquery that expected only five arguments. The current implementation of gin_extract_tsquery requires seven arguments, but this wrapper allows older opclass declarations to continue functioning without modification.

Similar to gin_extract_tsvector_2args, this function performs argument count validation and then delegates execution to the full gin_extract_tsquery implementation. It includes a safety check to ensure the required seven arguments are present, though this should always be the case during normal PostgreSQL operation.

## Parameters / Member Variables
- Standard PostgreSQL function arguments via PG_FUNCTION_ARGS macro  
- Inherits the same parameters as gin_extract_tsquery when delegation occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_NARGS (macro to get the number of function arguments)
  - gin_extract_tsquery (the actual implementation function)
  - elog (for error reporting)
- Called from (representative examples):
  - No direct callers found (used by PostgreSQL function call infrastructure)

## Notes and Other Information
- This is a compatibility stub function similar to gin_extract_tsvector_2args
- Maintains backward compatibility with older opclass declarations expecting five arguments
- The actual gin_extract_tsquery implementation requires seven arguments  
- Includes validation to ensure proper argument count before delegation
- Should not be used in new code; use gin_extract_tsquery directly instead
- Located in src/backend/utils/adt/tsginidx.c:316-327
- Part of PostgreSQL's backward compatibility infrastructure for GIN text search indexes