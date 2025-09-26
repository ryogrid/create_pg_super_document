# store_att_byval

## Location
[src/include/access/tupmacs.h:183-207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tupmacs.h#L183-L207)

## Overview
Stores a Datum value into tuple data area at a specified address, handling only by-value attributes with appropriate type conversion based on attribute length.

## Definition
static inline void store_att_byval(void *T, Datum newdatum, int attlen)

## Detailed Description
The store_att_byval function is a partial inverse of fetch_att that stores a given Datum value into tuple storage. It specifically handles only by-value attributes, as the caller typically needs to distinguish between by-value and by-reference cases anyway. The function uses a switch statement to handle different attribute lengths (1, 2, 4, or 8 bytes) and performs the appropriate type conversion using DatumGet functions before storing the value at the specified memory location.

## Parameters / Member Variables
- `T`: Pointer to the destination memory location in tuple storage where the value will be stored
- `newdatum`: The Datum value to be stored
- `attlen`: Length of the attribute in bytes, determining the storage format

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetChar](../D/DatumGetChar.md) (for 1-byte values)
  - [DatumGetInt16](../D/DatumGetInt16.md) (for 2-byte values) 
  - [DatumGetInt32](../D/DatumGetInt32.md) (for 4-byte values)
  - SIZEOF_DATUM (compile-time constant for 8-byte values)
  - elog (for error reporting)
- Called from (representative examples):
  - [brin_range_serialize](../b/brin_range_serialize.md)
  - [fill_val](../f/fill_val.md)
  - [statext_mcv_serialize](statext_mcv_serialize.md)
  - [ArrayCastAndSet](../A/ArrayCastAndSet.md)
  - [datum_write](../d/datum_write.md)

## Notes and Other Information
- This is an inline function for performance optimization during tuple construction and modification
- Only handles by-value attributes; by-reference attributes require separate handling by the caller
- The function includes platform-specific handling through SIZEOF_DATUM conditional compilation
- Throws an ERROR for unsupported attribute lengths to prevent data corruption
- Commonly used in serialization operations, tuple construction, and array processing
- Works as a complement to fetch_att for writing attribute data to tuple storage