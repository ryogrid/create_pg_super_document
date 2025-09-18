# convertJsonbArray

## Location
[src/backend/utils/adt/jsonb_util.c:1621-1704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1621-L1704)

## Overview
Converts a JsonbValue array structure into its binary JSONB representation by serializing the array elements and constructing the appropriate JSONB container format with headers and metadata.

## Definition


## Detailed Description
The  function is responsible for converting a JsonbValue array into the binary JSONB format. It constructs a JSONB array container by:

1. Creating a container header with element count and array flags
2. Reserving space for JEntry metadata for each array element  
3. Recursively converting each array element using 
4. Managing offset-based indexing for efficient element access
5. Enforcing size limits to prevent data overflow
6. Handling special cases like scalar arrays (single-element arrays at the top level)

The function uses a sophisticated offset management system where every JB_OFFSET_STRIDE'th element stores an absolute offset rather than a length, allowing for efficient random access to array elements.

## Parameters / Member Variables
- : StringInfo buffer where the serialized JSONB data will be appended
- : Pointer to JEntry that will be filled with metadata about this array container
- : JsonbValue containing the array data to be converted
- : Current nesting depth (used for recursive processing)

## Dependencies
- Functions called/Symbols referenced:
  - [padBufferToInt](../p/padBufferToInt.md) (aligns buffer to 4-byte boundary)
  - [appendToBuffer](../a/appendToBuffer.md) (appends data to buffer)
  - [reserveFromBuffer](../r/reserveFromBuffer.md) (reserves space in buffer)
  - [convertJsonbValue](convertJsonbValue.md) (recursively converts array elements)
  - [copyToBuffer](copyToBuffer.md) (copies data to specific buffer offset)
  - JBE_OFFLENFLD (extracts offset/length from JEntry)
- Constants used:
  - JB_FARRAY (array flag)
  - JB_FSCALAR (scalar flag)
  - JB_OFFSET_STRIDE (offset stride for indexing)
  - JENTRY_OFFLENMASK (maximum allowed size)
  - JENTRY_TYPEMASK (type mask)
  - JENTRY_HAS_OFF (offset flag)
  - JENTRY_ISCONTAINER (container flag)
- Called from:
  - [convertJsonbValue](convertJsonbValue.md) (main conversion dispatcher)

## Notes and Other Information
- Enforces a maximum total size limit of JENTRY_OFFLENMASK bytes for array contents
- Supports special 'rawScalar' arrays (single-element arrays that represent scalar values at the top level)
- Uses an offset-based indexing scheme where every JB_OFFSET_STRIDE'th element stores an absolute offset for efficient random access
- Performs buffer alignment to 4-byte boundaries for optimal memory access
- Includes comprehensive error checking for size limits to prevent integer overflow and data corruption
- The function is static and only used internally within the JSONB conversion system