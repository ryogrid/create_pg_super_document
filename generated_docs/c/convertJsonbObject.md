# convertJsonbObject

## Location
[src/backend/utils/adt/jsonb_util.c:1705-1820](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1705-L1820)

## Overview
Converts a JsonbValue object structure into its binary JSONB representation by serializing object key-value pairs and constructing the appropriate JSONB container format with proper ordering and metadata.

## Definition

```c
struct the header Jentry and store it in the beginning of the
	 * variable-length payload.
	 */
	containerheader = nPairs | JB_FOBJECT;
```
## Detailed Description
The  function converts a JsonbValue object into the binary JSONB format. It creates a JSONB object container with a specific layout:

1. Creates a container header with pair count and object flags
2. Reserves space for JEntry metadata for both keys and values (2 * nPairs entries)
3. Processes keys first, then values, to maintain the required on-disk ordering
4. Uses  for keys and  for values (allowing nested objects/arrays)
5. Implements offset-based indexing for efficient key-value access
6. Enforces comprehensive size limits to prevent overflow

The two-phase processing (keys first, then values) ensures that JSONB objects have predictable binary layout for efficient searching and indexing operations.

## Parameters / Member Variables
- : StringInfo buffer where the serialized JSONB data will be appended
- : Pointer to JEntry that will be filled with metadata about this object container
- : JsonbValue containing the object data to be converted
- : Current nesting depth (used for recursive processing of values)

## Dependencies
- Functions called/Symbols referenced:
  - [padBufferToInt](../p/padBufferToInt.md) (aligns buffer to 4-byte boundary)
  - [appendToBuffer](../a/appendToBuffer.md) (appends data to buffer)
  - [reserveFromBuffer](../r/reserveFromBuffer.md) (reserves space in buffer)
  - [convertJsonbScalar](convertJsonbScalar.md) (converts object keys, which must be strings)
  - [convertJsonbValue](convertJsonbValue.md) (recursively converts object values)
  - [copyToBuffer](copyToBuffer.md) (copies data to specific buffer offset)
  - JBE_OFFLENFLD (extracts offset/length from JEntry)
- Constants used:
  - JB_FOBJECT (object flag)
  - JB_OFFSET_STRIDE (offset stride for indexing)
  - JENTRY_OFFLENMASK (maximum allowed size)
  - JENTRY_TYPEMASK (type mask)
  - JENTRY_HAS_OFF (offset flag)
  - JENTRY_ISCONTAINER (container flag)
- Called from:
  - [convertJsonbValue](convertJsonbValue.md) (main conversion dispatcher)

## Notes and Other Information
- Processes object pairs in two phases: first all keys, then all values, to maintain required binary layout
- Keys are always converted using  since JSON object keys must be strings
- Values are converted using  to support nested objects, arrays, and scalars
- Implements offset-based indexing where every JB_OFFSET_STRIDE'th entry stores absolute offsets
- For values, the offset calculation accounts for the key entries: 
- Enforces strict size limits with multiple checks to prevent integer overflow
- Reserves space for exactly  JEntry records (one for each key and value)
- The function is static and only used internally within the JSONB conversion system
- Essential for maintaining JSONB's efficient binary search capabilities on object keys

## Simplified Source

```c
static void
convertJsonbObject(StringInfo buffer, JEntry *header, JsonbValue *val, int level)
{
    int base_offset = buffer->len;
    int nPairs = val->val.object.nPairs;

    // Align buffer to 4-byte boundary
    padBufferToInt(buffer);

    // Create container header with pair count and object flag
    uint32 containerheader = nPairs | JB_FOBJECT;
    appendToBuffer(buffer, (char *) &containerheader, sizeof(uint32));

    // Reserve space for metadata (2 entries per pair: key + value)
    int jentry_offset = reserveFromBuffer(buffer, sizeof(JEntry) * nPairs * 2);

    int totallen = 0;

    // Process all keys first (required JSONB layout)
    for (int i = 0; i < nPairs; i++) {
        JsonbPair *pair = &val->val.object.pairs[i];
        JEntry meta;

        // Convert key (always a string scalar)
        convertJsonbScalar(buffer, &meta, &pair->key);

        int len = JBE_OFFLENFLD(meta);
        totallen += len;

        // Check size limit
        if (totallen > JENTRY_OFFLENMASK) {
            ereport(ERROR, "jsonb object elements exceed maximum size");
        }

        // Store offset every JB_OFFSET_STRIDE elements
        if ((i % JB_OFFSET_STRIDE) == 0) {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
        jentry_offset += sizeof(JEntry);
    }

    // Process all values second
    for (int i = 0; i < nPairs; i++) {
        JsonbPair *pair = &val->val.object.pairs[i];
        JEntry meta;

        // Convert value (can be any JSON type)
        convertJsonbValue(buffer, &meta, &pair->value, level + 1);

        int len = JBE_OFFLENFLD(meta);
        totallen += len;

        // Check size limit
        if (totallen > JENTRY_OFFLENMASK) {
            ereport(ERROR, "jsonb object elements exceed maximum size");
        }

        // Store offset accounting for key entries already processed
        if (((i + nPairs) % JB_OFFSET_STRIDE) == 0) {
            meta = (meta & JENTRY_TYPEMASK) | totallen | JENTRY_HAS_OFF;
        }

        copyToBuffer(buffer, jentry_offset, (char *) &meta, sizeof(JEntry));
        jentry_offset += sizeof(JEntry);
    }

    // Set final header with total size
    *header = JENTRY_ISCONTAINER | (buffer->len - base_offset);
}
```