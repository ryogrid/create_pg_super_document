# statext_ndistinct_serialize

## Location
[src/backend/statistics/mvdistinct.c:179-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L179-L249)

## Overview
Serializes an MVNDistinct structure to a binary bytea format suitable for storage in the PostgreSQL system catalog.

## Definition

```c
struct, plus one base struct
	 * for each item, including number of items for each.
	 */
	len = VARHDRSZ + SizeOfHeader;
```
## Detailed Description
This function converts an in-memory MVNDistinct structure into a compact binary representation that can be stored in the pg_statistic_ext_data system catalog. The serialization process carefully packs all data including the header information (magic number, type, number of items) and each ndistinct item with its associated attribute numbers and computed ndistinct value.

The function performs precise memory calculations to determine the exact size needed for the serialized data, then systematically copies each component to the output buffer. The serialized format is designed to be platform-independent and includes validation checks to ensure data integrity.

Key aspects of the serialization:
- Stores header fields (magic, type, nitems) first
- For each item: stores ndistinct value, number of attributes, then attribute numbers
- Uses fixed-size data types for cross-platform compatibility
- Includes assertions to prevent buffer overflows and validate completeness

## Parameters / Member Variables
- : Pointer to MVNDistinct structure to be serialized, containing computed ndistinct statistics for various attribute combinations

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfHeader: Macro calculating size of MVNDistinct header
  - SizeOfItem: Macro calculating size of MVNDistinctItem with given number of attributes  
  - SET_VARSIZE: Sets the total size of a variable-length PostgreSQL type
  - VARDATA: Gets pointer to data portion of variable-length type
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
  - memcpy: Memory copy for binary data transfer
- Called from (representative examples):
  - [statext_store](statext_store.md): Stores serialized statistics in system catalog

## Notes and Other Information
- Validates magic number (STATS_NDISTINCT_MAGIC) and type (STATS_NDISTINCT_TYPE_BASIC) before serialization
- Requires all attribute combinations to have at least 2 attributes (nmembers >= 2)
- Uses bytea PostgreSQL type for variable-length binary data storage
- Includes comprehensive assertions to prevent buffer overflows and validate exact space usage
- The serialized format must match exactly with what statext_ndistinct_deserialize expects
- Part of PostgreSQL's persistent storage mechanism for multivariate statistics
- Ensures platform-independent storage by using explicit data type sizes

## Simplified Source

```c
bytea *statext_ndistinct_serialize(MVNDistinct *ndistinct) {
    bytea *output;
    char *tmp;
    Size len;
    int i;

    // Validate input structure
    Assert(ndistinct->magic == STATS_NDISTINCT_MAGIC);
    Assert(ndistinct->type == STATS_NDISTINCT_TYPE_BASIC);

    // Calculate total space needed: header + all items
    len = VARHDRSZ + SizeOfHeader;
    for (i = 0; i < ndistinct->nitems; i++) {
        int nmembers = ndistinct->items[i].nattributes;
        len += SizeOfItem(nmembers);
    }

    // Allocate output buffer
    output = (bytea *) palloc(len);
    SET_VARSIZE(output, len);
    tmp = VARDATA(output);

    // Store header: magic, type, number of items
    memcpy(tmp, &ndistinct->magic, sizeof(uint32));
    tmp += sizeof(uint32);
    memcpy(tmp, &ndistinct->type, sizeof(uint32));
    tmp += sizeof(uint32);
    memcpy(tmp, &ndistinct->nitems, sizeof(uint32));
    tmp += sizeof(uint32);

    // Store each ndistinct item: value, attribute count, attributes
    for (i = 0; i < ndistinct->nitems; i++) {
        MVNDistinctItem item = ndistinct->items[i];
        int nmembers = item.nattributes;

        memcpy(tmp, &item.ndistinct, sizeof(double));
        tmp += sizeof(double);
        memcpy(tmp, &nmembers, sizeof(int));
        tmp += sizeof(int);
        memcpy(tmp, item.attributes, sizeof(AttrNumber) * nmembers);
        tmp += nmembers * sizeof(AttrNumber);
    }

    return output;
}
```