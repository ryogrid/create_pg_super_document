# brin_minmax_multi_opcinfo

## Location
[src/backend/access/brin/brin_minmax_multi.c:1859-1882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1859-L1882)

## Overview
This function initializes and returns operator class information structure for the BRIN minmax-multi index type, setting up the necessary metadata for index operations.

## Definition

```c
Datum
brin_minmax_multi_opcinfo(PG_FUNCTION_ARGS)
```
## Detailed Description
This is a PostgreSQL function that serves as the opcinfo support function for the BRIN minmax-multi operator class. It creates and initializes a BrinOpcInfo structure that contains essential metadata about how the BRIN minmax-multi index should operate.

The function allocates memory for both the BrinOpcInfo structure and an associated MinmaxMultiOpaque structure. The allocation uses MAXALIGN to ensure proper memory alignment. The opaque data structure is positioned immediately after the BrinOpcInfo structure in the same memory block.

Key initialization includes:
- Setting oi_nstored to 1, indicating this operator class stores one summary value per indexed attribute
- Enabling oi_regular_nulls to use standard NULL handling
- Setting up the opaque pointer to reference the MinmaxMultiOpaque structure
- Initializing the type cache for the summary data type (PG_BRIN_MINMAX_MULTI_SUMMARYOID)

The strategy_procinfos in the opaque structure is noted to be lazily initialized (set to InvalidOid by palloc0), meaning the actual operator procedures will be looked up when first needed.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - MAXALIGN
  - SizeofBrinOpcInfo
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - PG_RETURN_POINTER
- Called from:
  - (No direct references found - likely called through PostgreSQL's operator class system)

## Notes and Other Information
- Returns a Datum containing pointer to the initialized BrinOpcInfo structure
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Uses lazy initialization strategy for operator procedure information to improve startup performance
- Memory layout places the opaque structure immediately after the main BrinOpcInfo structure for efficiency
- Critical infrastructure function that enables the BRIN minmax-multi operator class to integrate with PostgreSQL's index access method framework
- The oi_nstored value of 1 indicates that each page range summary consists of a single complex value (the ranges structure)
- Standard NULL handling (oi_regular_nulls = true) means the index can handle NULL values using PostgreSQL's built-in mechanisms