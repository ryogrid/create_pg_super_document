# jsonb_contains

## Location
[src/backend/utils/adt/jsonb_op.c:112-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_op.c#L112-L129)

## Overview
Tests whether a JSONB value contains another JSONB value as a subset, performing deep containment checking.

## Definition


## Detailed Description
The jsonb_contains function implements the PostgreSQL '@>' operator for JSONB values. It performs deep containment checking to determine if the first JSONB value contains the second JSONB value as a subset. This is a recursive operation that checks nested structures.

For containment to be true:
- Objects must contain all key-value pairs from the template object (but can have additional pairs)
- Arrays must contain all elements from the template array (but can have additional elements)
- The root types (object vs array) must match between the two JSONB values

## Parameters / Member Variables
-  (Jsonb *): The JSONB value that potentially contains the template
-  (Jsonb *): The template JSONB value to search for within the first value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - JB_ROOT_IS_OBJECT
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbDeepContains](../J/JsonbDeepContains.md)
  - PG_RETURN_BOOL
- Types used:
  - Jsonb
  - JsonbIterator

## Notes and Other Information
- Returns false immediately if root types don't match (object vs array)
- Performs recursive deep containment checking using JsonbDeepContains
- Corresponds to the '@>' operator in PostgreSQL JSONB operations
- More complex than simple key existence - checks actual containment of structures
- Uses iterators to efficiently traverse both JSONB structures simultaneously
- Containment is asymmetric: A contains B does not imply B contains A