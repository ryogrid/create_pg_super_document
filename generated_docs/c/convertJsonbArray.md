# convertJsonbArray

## Location
[src/backend/utils/adt/jsonb_util.c:1621-1704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1621-L1704)

## Overview
Converts a JsonbValue array structure into its binary JSONB representation by serializing the array elements and constructing the appropriate JSONB container format with headers and metadata.

## Definition

```c
struct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerhead = nElems | JB_FARRAY;
```
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

## Simplified Source

```c
static void
convertJsonbArray(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
    int base_offset = buffer->len;
    int nElems = val->val.array.nElems;

    // Align buffer to 4-byte boundary
    padBufferToInt(buffer);

    // Create container header with element count and array flags
    uint32 containerhead = nElems | JB_FARRAY;
    if (val->val.array.rawScalar) {
        containerhead |= JB_FSCALAR;  // Single scalar at top level
    }

    // Write container header
    appendToBuffer(buffer, (char *) &containerhead, sizeof(uint32));

    // Reserve space for element metadata
    int jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nElems);

    // Convert each array element
    int totallen = 0;
    for (int i = 0; i < nElems; i++) {
        JsonbValue *elem = &val->val.array.elems[i];
        JEntry meta;

        // Recursively convert element
        convertJsonbValue(buffer, &meta, elem, level + 1);

        int len = JBE_OFFLENFLD(meta);
        totallen += len;

        // Check size limit to prevent overflow
        if (totallen > JENTRY_OFFLENMASK) {
            ereport(ERROR, "jsonb array elements exceed maximum size");
        }

        // Store offset every JB_OFFSET_STRIDE elements for efficient access
        if ((i % JB_OFFSET_STRIDE) == 0) {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        // Write element metadata
        copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
        jentry_offset += sizeof(JEntry);
    }

    // Set final header with total size
    *header = JENTRY_ISCONTAINER | (buffer->len - base_offset);
}
```