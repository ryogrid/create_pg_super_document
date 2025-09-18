# jsonb_hash

## Location
src/backend/utils/adt/jsonb_op.c: 253 - 294

## Overview
Computes a hash value for a JSONB value, enabling its use in hash-based data structures like hash indexes and hash joins.

## Definition
Datum jsonb_hash(PG_FUNCTION_ARGS)

## Detailed Description
The jsonb_hash function generates a 32-bit hash value for JSONB data by iterating through all components of the JSONB structure using JsonbIterator. It processes different JSONB elements (arrays, objects, keys, values, elements) and combines their hash contributions using XOR operations and scalar value hashing.

The function handles empty JSONB values by returning 0 immediately. For non-empty values, it iterates through the entire JSONB structure, applying different hashing strategies based on the element type:
- Array and object beginnings contribute type-specific flags (JB_FARRAY, JB_FOBJECT)
- Scalar values (keys, values, elements) are hashed using JsonbHashScalarValue
- End markers for arrays and objects are ignored in hash calculation

The hashing ensures that structurally and semantically identical JSONB values produce the same hash, supporting hash-based operations and indexing.

## Parameters / Member Variables
- : JSONB value to hash (jb)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_COUNT
  - JsonbIteratorInit
  - JsonbIteratorNext
  - JsonbHashScalarValue (src/backend/utils/adt/jsonb_util.c:1322-1364)
  - PG_FREE_IF_COPY
  - PG_RETURN_INT32
  - elog
- Data types used:
  - Jsonb
  - JsonbIterator
  - JsonbValue
  - JsonbIteratorToken
- Constants used:
  - WJB_DONE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, WJB_ELEM, WJB_END_ARRAY, WJB_END_OBJECT
  - JB_FARRAY, JB_FOBJECT

## Notes and Other Information
- Location: src/backend/utils/adt/jsonb_op.c:253-294
- Essential for hash-based indexing of JSONB columns (hash indexes, hash joins)
- Returns 0 for empty JSONB values as an optimization
- Uses XOR operations to combine hash contributions from different structural elements
- The iteration-based approach ensures all nested elements contribute to the final hash
- Hash consistency is maintained across identical JSONB structures regardless of key ordering in objects
- Error handling includes detection of invalid JsonbIteratorNext return codes