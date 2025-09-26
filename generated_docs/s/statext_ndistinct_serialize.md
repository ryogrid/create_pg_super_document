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
  - palloc: PostgreSQL memory allocation
  - memcpy: Memory copy for binary data transfer
- Called from (representative examples):
  - statext_store: Stores serialized statistics in system catalog

## Notes and Other Information
- Validates magic number (STATS_NDISTINCT_MAGIC) and type (STATS_NDISTINCT_TYPE_BASIC) before serialization
- Requires all attribute combinations to have at least 2 attributes (nmembers >= 2)
- Uses bytea PostgreSQL type for variable-length binary data storage
- Includes comprehensive assertions to prevent buffer overflows and validate exact space usage
- The serialized format must match exactly with what statext_ndistinct_deserialize expects
- Part of PostgreSQL's persistent storage mechanism for multivariate statistics
- Ensures platform-independent storage by using explicit data type sizes