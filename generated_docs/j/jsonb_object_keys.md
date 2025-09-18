# jsonb_object_keys

## Location
[src/backend/utils/adt/jsonfuncs.c:566-638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L566-L638)

## Overview
This SQL function extracts and returns the set of top-level keys from a JSONB object as a set-returning function (SRF) operating in value-per-call mode.

## Definition
Datum jsonb_object_keys(PG_FUNCTION_ARGS)

## Detailed Description
jsonb_object_keys implements a set-returning function that processes JSONB objects to extract their top-level keys. The function operates in value-per-call mode, processing the entire JSONB object during the first call and caching all keys in an array for subsequent calls. It uses PostgreSQL's JsonbIterator to traverse the JSONB structure efficiently, collecting only WJB_KEY tokens while skipping nested structures. The function validates input by rejecting scalar values and arrays, ensuring it only operates on JSONB objects. Memory management is handled through PostgreSQL's SRF framework, with all allocations occurring in the multi-call memory context to persist across function calls.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro to access arguments:
  - Argument 0: JSONB object from which to extract keys

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_SCALAR
  - JB_ROOT_IS_ARRAY
  - SRF_FIRSTCALL_INIT
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - WJB_DONE
  - WJB_KEY
- Called from:
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
This function is designed for efficient key extraction from potentially large JSONB objects by processing all keys in a single iteration and storing them in memory. The implementation limits keys to NAMEDATALEN size and assumes reasonable key counts to avoid excessive memory usage. The function demonstrates PostgreSQL's SRF pattern for returning multiple values from a single function call, making it suitable for use in SELECT clauses and other contexts requiring set-valued returns. Error handling includes appropriate type checking to ensure the function only operates on JSONB objects.