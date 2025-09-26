# array_recv

## Location
[src/backend/utils/adt/arrayfuncs.c:1271-1453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1271-L1453)

## Overview
Converts PostgreSQL arrays from external binary format to their internal ArrayType representation, handling deserialization with comprehensive validation and type checking.

## Definition

```c
struct_empty_array(element_type));
```
## Detailed Description
array_recv is the binary receive function for PostgreSQL arrays, responsible for deserializing binary array data from network or storage formats into internal ArrayType structures. The function performs extensive validation of the binary input, including dimension bounds checking, element type verification, and format validation. It uses a caching mechanism (ArrayMetaState) to optimize repeated operations with the same element type.

The function processes the binary stream by reading array metadata (dimensions, bounds, flags), validates the element type against expected types (with special handling for built-in vs. user-defined types), reads individual array elements using ReadArrayBinary, and constructs the final ArrayType structure with proper null bitmap handling. Security considerations include robust validation to prevent malformed binary data from causing system issues.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro with three arguments:
  - buf (StringInfo): Binary data buffer containing the serialized array
  - spec_element_type (Oid): Expected element type OID
  - typmod (int32): Type modifier for array elements

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [ArrayCheckBounds](../A/ArrayCheckBounds.md)
  - [get_type_io_data](../g/get_type_io_data.md)
  - IOFunc_receive
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [ReadArrayBinary](../R/ReadArrayBinary.md)
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - SET_VARSIZE
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - ARR_DIMS
  - ARR_LBOUND
  - PG_RETURN_ARRAYTYPE_P
- Called from (representative examples):
  - [int2vectorrecv](../i/int2vectorrecv.md)
  - [oidvectorrecv](../o/oidvectorrecv.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
The function implements security-conscious type checking, only complaining about type mismatches for built-in types (OIDs less than FirstGenbkiObjectId) since user-defined type OIDs are not stable across systems. It handles empty arrays as a special case, returning construct_empty_array() after validating the element type. The ArrayMetaState cache structure stores element type information to avoid repeated lookups. The function supports arrays with up to MAXDIM dimensions and performs comprehensive bounds checking to prevent integer overflow in array size calculations.