# statext_ndistinct_deserialize

## Location
[src/backend/statistics/mvdistinct.c:250-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mvdistinct.c#L250-L338)

## Overview
Deserializes binary bytea data back into an in-memory MVNDistinct structure, performing comprehensive validation and error checking.

## Definition

```c
struct */
	if (VARSIZE_ANY_EXHDR(data) < SizeOfHeader)
		elog(ERROR, "invalid MVNDistinct size %zu (expected at least %zu)",
			 VARSIZE_ANY_EXHDR(data), SizeOfHeader);
```
## Detailed Description
This function converts binary data stored in PostgreSQL's bytea format back into a fully functional MVNDistinct structure. It performs extensive validation of the input data including magic number verification, type checking, size validation, and structural integrity checks to ensure the data is valid and uncorrupted.

The deserialization process:
1. Validates minimum data size and basic structure
2. Reads and validates header fields (magic, type, nitems)
3. Checks that the total data size matches expectations
4. Allocates memory for the MVNDistinct structure and items
5. Deserializes each MVNDistinctItem with its attributes
6. Validates that all data is consumed exactly

The function includes robust error handling with detailed error messages for various corruption scenarios, making it suitable for reading data that may have been stored across different PostgreSQL versions or platforms.

## Parameters / Member Variables
- : Pointer to bytea containing serialized MVNDistinct data, or NULL (which returns NULL)

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfHeader: Macro for calculating MVNDistinct header size
  - MinSizeOfItems: Macro for calculating minimum size for given number of items
  - VARSIZE_ANY_EXHDR: Gets size of variable-length data excluding header
  - VARDATA_ANY: Gets pointer to data portion of variable-length type
  - VARSIZE_ANY: Gets total size of variable-length data
  - [palloc0](../p/palloc0.md): Zero-initialized PostgreSQL memory allocation
  - [palloc](../p/palloc.md): PostgreSQL memory allocation
  - memcpy: Memory copy for binary data
  - MAXALIGN: Aligns memory to platform requirements
- Called from (representative examples):
  - [statext_ndistinct_load](statext_ndistinct_load.md): Loads statistics from system catalog
  - [pg_ndistinct_out](../p/pg_ndistinct_out.md): Output function for pg_ndistinct type

## Notes and Other Information
- Returns NULL if input data is NULL (graceful handling)
- Validates magic number (STATS_NDISTINCT_MAGIC) and type (STATS_NDISTINCT_TYPE_BASIC)
- Ensures each item has between 2 and STATS_MAX_DIMENSIONS attributes
- Uses MAXALIGN for proper memory alignment on all platforms
- Performs exact size checking to detect truncated or corrupted data
- Allocates separate memory for each item's attribute array
- Includes assertions to prevent buffer overruns during deserialization
- Must exactly match the format produced by statext_ndistinct_serialize
- Part of PostgreSQL's type system for multivariate statistics persistence