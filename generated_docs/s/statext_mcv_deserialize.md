# statext_mcv_deserialize

## Location
[src/backend/statistics/mcv.c:996-1337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L996-L1337)

## Overview
Deserializes a binary representation of an MCV (Most Common Values) statistics list stored as a bytea into an in-memory MCVList structure, performing comprehensive validation and memory layout optimization.

## Definition

```c
struct alignment.
	 */
	if (VARSIZE_ANY(data) < MinSizeOfMCVList)
		elog(ERROR, "invalid MCV size %zu (expected at least %zu)",
			 VARSIZE_ANY(data), MinSizeOfMCVList);
```
## Detailed Description
This function reconstructs an MCVList structure from its serialized binary form. The deserialization process involves:

1. **Header validation**: Verifies magic number, type, dimensions count, and items count
2. **Size validation**: Ensures the input data matches expected size calculations
3. **Memory layout optimization**: Allocates all required memory as a single contiguous chunk for efficient access and cleanup
4. **Value reconstruction**: Rebuilds individual MCV items with proper value mappings and null flags
5. **Type-specific handling**: Handles by-value types, fixed-length by-reference types, varlena types, and cstring types differently

The function creates a mapping array to translate serialized value indexes back to actual Datum values, ensuring proper alignment and memory management throughout the process.

## Parameters / Member Variables
- : Input bytea containing the serialized MCV list data. If NULL, the function returns NULL immediately.

## Dependencies
- Functions called/Symbols referenced:
  -  - Get size of variable-length data
  -  - Get data portion of variable-length structure  
  - // - Memory allocation functions
  -  - Attribute fetching utility
  - / - Varlena manipulation macros
  -  - Convert pointer to Datum
  - Constants: , , , 

- Called from (representative examples):
  -  - Loads MCV statistics from system catalogs
  -  - SQL function to expose MCV list contents

## Notes and Other Information
- Allocates all memory as a single chunk for efficient cleanup with a single pfree()
- Performs extensive validation to prevent corruption from malformed input data
- Handles different PostgreSQL data types (by-value, fixed-length by-reference, varlena, cstring) with type-specific deserialization logic
- Uses alignment-aware memory layout to ensure proper data structure alignment
- The deserialized structure maintains the same logical organization as the original MCV list but with optimized memory layout for runtime usage
- Critical for extended statistics functionality in PostgreSQL's query planner

## Simplified Source

```c
MCVList *
statext_mcv_deserialize(bytea *data)
{
    int dim, i;
    Size expected_size;
    MCVList *mcvlist;
    char *raw, *ptr, *endptr;
    int ndims, nitems;
    DimensionInfo *info = NULL;
    Datum **map = NULL;  // Mapping arrays for value reconstruction

    if (data == NULL)
        return NULL;

    // Basic size validation
    if (VARSIZE_ANY(data) < MinSizeOfMCVList)
        elog(ERROR, "invalid MCV size %zu (expected at least %zu)",
             VARSIZE_ANY(data), MinSizeOfMCVList);

    // Allocate base structure and setup data pointers
    mcvlist = (MCVList *) palloc0(offsetof(MCVList, items));
    ptr = VARDATA_ANY(data);
    endptr = (char *) data + VARSIZE_ANY(data);

    // Read and validate header fields
    memcpy(&mcvlist->magic, ptr, sizeof(uint32));
    ptr += sizeof(uint32);
    memcpy(&mcvlist->type, ptr, sizeof(uint32));
    ptr += sizeof(uint32);
    memcpy(&mcvlist->nitems, ptr, sizeof(uint32));
    ptr += sizeof(uint32);
    memcpy(&mcvlist->ndimensions, ptr, sizeof(AttrNumber));
    ptr += sizeof(AttrNumber);

    // Validate header values
    if (mcvlist->magic != STATS_MCV_MAGIC)
        elog(ERROR, "invalid MCV magic %u (expected %u)",
             mcvlist->magic, STATS_MCV_MAGIC);
    if (mcvlist->type != STATS_MCV_TYPE_BASIC)
        elog(ERROR, "invalid MCV type %u (expected %u)",
             mcvlist->type, STATS_MCV_TYPE_BASIC);

    nitems = mcvlist->nitems;
    ndims = mcvlist->ndimensions;

    // Validate dimensions and item counts
    if (ndims == 0 || ndims > STATS_MAX_DIMENSIONS || ndims < 0)
        elog(ERROR, "invalid dimension count in MCVList");
    if (nitems == 0 || nitems > STATS_MCVLIST_MAX_ITEMS)
        elog(ERROR, "invalid item count in MCVList");

    // Read type information and dimension info
    memcpy(mcvlist->types, ptr, sizeof(Oid) * ndims);
    ptr += (sizeof(Oid) * ndims);

    info = palloc(ndims * sizeof(DimensionInfo));
    memcpy(info, ptr, ndims * sizeof(DimensionInfo));
    ptr += (ndims * sizeof(DimensionInfo));

    // Calculate expected total size including all data
    expected_size = SizeOfMCVList(ndims, nitems);
    for (dim = 0; dim < ndims; dim++)
        expected_size += info[dim].nbytes;

    if (VARSIZE_ANY(data) != expected_size)
        elog(ERROR, "invalid MCV size %zu (expected %zu)",
             VARSIZE_ANY(data), expected_size);

    // Build value mapping arrays for each dimension
    map = (Datum **) palloc(ndims * sizeof(Datum *));
    Size datalen = 0;
    for (dim = 0; dim < ndims; dim++) {
        map[dim] = (Datum *) palloc(sizeof(Datum) * info[dim].nvalues);
        datalen += info[dim].nbytes_aligned;
    }

    // Reallocate MCVList with space for all items and data
    Size mcvlen = MAXALIGN(offsetof(MCVList, items) + (sizeof(MCVItem) * nitems));
    mcvlen += nitems * MAXALIGN(sizeof(Datum) * ndims);  // values arrays
    mcvlen += nitems * MAXALIGN(sizeof(bool) * ndims);   // isnull arrays
    mcvlen += MAXALIGN(datalen);                         // actual data

    mcvlist = repalloc(mcvlist, mcvlen);

    // Setup pointers for values, nulls, and data within the allocated space
    char *valuesptr = (char *) mcvlist + MAXALIGN(offsetof(MCVList, items) + (sizeof(MCVItem) * nitems));
    char *isnullptr = valuesptr + (nitems * MAXALIGN(sizeof(Datum) * ndims));
    char *dataptr = isnullptr + (nitems * MAXALIGN(sizeof(bool) * ndims));

    // Deserialize values for each dimension and build mapping
    for (dim = 0; dim < ndims; dim++) {
        for (i = 0; i < info[dim].nvalues; i++) {
            if (info[dim].typbyval) {
                // By-value types: direct copy
                Datum v = 0;
                memcpy(&v, ptr, info[dim].typlen);
                ptr += info[dim].typlen;
                map[dim][i] = fetch_att(&v, true, info[dim].typlen);
            } else {
                // By-reference types: copy data and create pointer
                // Handle fixed-length, varlena (-1), and cstring (-2) types
                memcpy(dataptr, ptr, /* appropriate length */);
                map[dim][i] = PointerGetDatum(dataptr);
                // Advance pointers appropriately
            }
        }
    }

    // Deserialize MCV items using the value mappings
    for (i = 0; i < nitems; i++) {
        MCVItem *item = &mcvlist->items[i];

        // Setup arrays for this item
        item->values = (Datum *) valuesptr;
        valuesptr += MAXALIGN(sizeof(Datum) * ndims);
        item->isnull = (bool *) isnullptr;
        isnullptr += MAXALIGN(sizeof(bool) * ndims);

        // Read item data
        memcpy(item->isnull, ptr, sizeof(bool) * ndims);
        ptr += sizeof(bool) * ndims;
        memcpy(&item->frequency, ptr, sizeof(double));
        ptr += sizeof(double);
        memcpy(&item->base_frequency, ptr, sizeof(double));
        ptr += sizeof(double);

        // Translate value indexes to actual Datum values
        for (dim = 0; dim < ndims; dim++) {
            uint16 index;
            memcpy(&index, ptr, sizeof(uint16));
            ptr += sizeof(uint16);

            if (!item->isnull[dim])
                item->values[dim] = map[dim][index];
        }
    }

    // Cleanup temporary mapping arrays
    for (dim = 0; dim < ndims; dim++)
        pfree(map[dim]);
    pfree(map);

    return mcvlist;
}
```