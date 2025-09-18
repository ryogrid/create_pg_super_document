# gtsvector_union

## Location
src/backend/utils/adt/tsgistidx.c: 402 - 428

## Overview
The gtsvector_union function implements the GiST union operation for TSVector data types, combining multiple signature keys into a single unified signature for index operations.

## Definition


## Detailed Description
This function is a PostgreSQL GiST index support function that creates a union of multiple TSVector signatures. It iterates through all entries in the input vector, calling unionkey() to merge each signature with the base signature. If any individual signature indicates an all-true state, the entire result is marked as all-true and processing stops early. The function allocates a new signature result and returns it as a PostgreSQL Datum.

## Parameters / Member Variables
- Function uses PostgreSQL's PG_FUNCTION_ARGS macro which provides:
  - : GistEntryVector containing array of signature entries to union
  - : Pointer to integer where result size will be stored
- Internal variables:
  - : Length of signature in bytes (retrieved via GET_SIGLEN())
  - : Newly allocated SignTSVector for the union result
  - : Bit vector pointer to the result signature

## Dependencies
- Functions called/Symbols referenced:
  - GET_SIGLEN (macro to get signature length)
  - gtsvector_alloc (allocates new TSVector signature)
  - GETSIGN (macro to get signature bit vector)
  - memset (standard C function to initialize memory)
  - GETENTRY (macro to get entry from vector)
  - unionkey (performs actual union operation)
  - SET_VARSIZE (macro to set variable size)
  - CALCGTSIZE (macro to calculate signature size)
  - VARSIZE (macro to get variable size)
- Called from:
  - GiST index infrastructure (registered as union support function)

## Notes and Other Information
This is a PostgreSQL extension function following the PG_FUNCTION_ARGS/PG_RETURN_POINTER convention. It's specifically designed to be registered as a GiST index support function for TSVector data types. The function handles the ALLISTRUE optimization where if any signature represents all possible values, the entire union becomes all-true, allowing for more efficient index operations.