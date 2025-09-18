# gtsvector_same

## Location
src/backend/utils/adt/tsgistidx.c: 429 - 489

## Overview
The gtsvector_same function implements the GiST same operation for TSVector data types, determining whether two signature keys are identical for index optimization purposes.

## Definition


## Detailed Description
This function compares two TSVector signatures to determine if they are identical. It handles two different signature formats: signature keys (bit vectors) and array-based representations. For signature keys, it first checks if both signatures have the same ALLTRUE status, then performs byte-by-byte comparison of the bit vectors if needed. For array-based signatures, it compares the array lengths first, then performs element-by-element comparison. The function is used by the GiST index infrastructure to optimize index operations by identifying duplicate signatures.

## Parameters / Member Variables
- Function uses PostgreSQL's PG_FUNCTION_ARGS macro which provides:
  - : First SignTSVector signature to compare
  - : Second SignTSVector signature to compare  
  - : Pointer to boolean where comparison result will be stored
- Internal variables:
  - : Length of signature in bytes (retrieved via GET_SIGLEN())
  - , : Bit vector pointers for signature comparison
  - , : Array lengths for array-based signatures
  - , : Array pointers for element comparison

## Dependencies
- Functions called/Symbols referenced:
  - GET_SIGLEN (macro to get signature length)
  - ISSIGNKEY (macro to check if signature is a key type)
  - ISALLTRUE (macro to check if signature represents all-true state)
  - GETSIGN (macro to get signature bit vector)
  - GETSIGLEN (macro to get signature length)
  - LOOPBYTE (macro for byte-wise iteration)
  - ARRNELEM (macro to get number of array elements)
  - GETARR (macro to get array representation)
- Called from:
  - GiST index infrastructure (registered as same support function)

## Notes and Other Information
This is a PostgreSQL extension function following the PG_FUNCTION_ARGS/PG_RETURN_POINTER convention. It's specifically designed to be registered as a GiST index support function for TSVector data types. The function handles special cases efficiently: if one signature is ALLTRUE and the other is not, they are immediately considered different. The comparison is optimized to break early when differences are found, making it efficient for large signatures.