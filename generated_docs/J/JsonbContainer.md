# JsonbContainer

## Location
src/include/utils/jsonb.h: 190 - 197

## Overview
JsonbContainer represents a JSONB array or object node within a PostgreSQL Jsonb Datum, serving as the on-disk storage format for composite JSON structures.

## Definition
```c
typedef struct JsonbContainer
{
    uint32      header;         /* number of elements or key/value pairs, and flags */
    JEntry      children[FLEXIBLE_ARRAY_MEMBER];
    
    /* the data for each child node follows. */
} JsonbContainer;
```

## Detailed Description
JsonbContainer is the fundamental structure for storing JSONB arrays and objects in PostgreSQL's on-disk format. It uses a compact layout where the header contains both count information and type flags, followed by an array of JEntry headers for child nodes, and then the variable-length data for each child.

For arrays, each element is stored as a child in array order. For objects, the structure uses a specialized layout: all keys appear first in sorted order, followed by their corresponding values in matching order. This arrangement optimizes memory locality and makes key searches more cache-friendly.

The structure supports the flags JB_FSCALAR, JB_FOBJECT, and JB_FARRAY in the header to indicate the container type, with count information stored in the lower 28 bits.

## Parameters / Member Variables
- `header`: 32-bit field containing count (lower 28 bits) and type flags (upper 4 bits)
  - Count: Number of elements (arrays) or key/value pairs (objects)
  - Flags: JB_FSCALAR, JB_FOBJECT, JB_FARRAY indicate container type
- `children[]`: Flexible array of JEntry headers, one for each child node
  - For arrays: one JEntry per element
  - For objects: JEntries for keys first, then values, maintaining key sort order

## Dependencies
- Functions called/Symbols referenced:
  - JEntry (entry header type)
  - FLEXIBLE_ARRAY_MEMBER (macro for variable-length arrays)
  - uint32 (standard type)
- Called from (representative examples):
  - [JsonbToCString](JsonbToCString.md)
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md)
  - [getIthJsonbValueFromContainer](../g/getIthJsonbValueFromContainer.md)
  - [JsonbIteratorInit](JsonbIteratorInit.md)
  - [compareJsonbContainers](../c/compareJsonbContainers.md)

## Notes and Other Information
Access macros are provided for convenient header manipulation: JsonContainerSize(), JsonContainerIsScalar(), JsonContainerIsObject(), and JsonContainerIsArray(). The object key/value layout (keys first, then values) is crucial for efficient key searches and maintains cache locality. The FLEXIBLE_ARRAY_MEMBER allows the structure to accommodate variable numbers of children without additional memory allocation.