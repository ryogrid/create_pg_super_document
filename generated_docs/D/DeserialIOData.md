# DeserialIOData

## Location
src/backend/utils/adt/array_userfuncs.c: 39 - 43

## Overview
DeserialIOData is a structure used for caching element-type data during array aggregation deserialization to optimize performance by avoiding repeated catalog lookups.

## Definition
typedef struct DeserialIOData
{
    FmgrInfo    typreceive;
    Oid         typioparam;
} DeserialIOData;

## Detailed Description
DeserialIOData is a helper structure specifically designed for the array_agg_deserialize function in PostgreSQL's array aggregation implementation. It serves as a cache to store the element type's binary input function information and type I/O parameter, preventing the need for repeated catalog lookups during the deserialization process of array aggregation states.

The structure is allocated in the function's memory context and stored in fcinfo->flinfo->fn_extra for persistence across multiple calls to the same function instance. This caching mechanism significantly improves performance when deserializing arrays with by-reference element types, as the type's receive function information only needs to be looked up once.

## Parameters / Member Variables
- `typreceive`: FmgrInfo structure containing the cached binary input function information for the array element type, obtained via getTypeBinaryInputInfo() and initialized with fmgr_info_cxt()
- `typioparam`: OID of the type I/O parameter required by the element type's receive function, also obtained from getTypeBinaryInputInfo()

## Dependencies
- Functions called/Symbols referenced:
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager info structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [array_agg_deserialize](../a/array_agg_deserialize.md) (src/backend/utils/adt/array_userfuncs.c:764, 767, 772, 774)

## Notes and Other Information
- This structure is only used for by-reference element types; by-value types don't require the receive function during deserialization
- The structure is allocated in the function's memory context to ensure it persists for the lifetime of the function call
- Part of PostgreSQL's array aggregation serialization/deserialization infrastructure for parallel query processing
- The cached information is used with ReceiveFunctionCall() to deserialize individual array elements
- The typioparam member is essential for certain data types that require additional parameters during input processing