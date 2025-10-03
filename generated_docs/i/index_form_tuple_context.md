# index_form_tuple_context

## Location
[src/backend/access/common/indextuple.c:65-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/indextuple.c#L65-L240)

## Overview
The `index_form_tuple_context` function creates an IndexTuple from arrays of attribute values and null indicators, with explicit memory context specification and comprehensive handling of variable-length attributes.

## Definition
```c
IndexTuple index_form_tuple_context(TupleDesc tupleDescriptor, const Datum *values, const bool *isnull, MemoryContext context)
```

## Detailed Description
This function constructs an IndexTuple by combining attribute values and null indicators according to the provided tuple descriptor. It is the core implementation for index tuple formation in PostgreSQL, handling complex scenarios including:

- TOAST (The Oversized-Attribute Storage Technique) processing when compiled with TOAST_INDEX_HACK
- External attribute detoasting to ensure tuple self-containment
- Compression of large values that exceed TOAST_INDEX_TARGET size
- Variable-length attribute handling and masking
- Proper memory allocation in the specified memory context
- Size validation to ensure tuples fit within INDEX_SIZE_MASK limits

The function is designed to be memory-leak safe and avoid external table access when possible, making it suitable for performance-critical operations like tuple sorting.

## Parameters
- `tupleDescriptor`: TupleDesc describing the structure and attributes of the tuple to be formed
- `values`: Array of Datum values for each attribute in the tuple
- `isnull`: Array of boolean flags indicating which attributes are NULL
- `context`: MemoryContext in which to allocate the returned tuple

## Dependencies
- Functions called/Symbols referenced:
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [heap_compute_data_size](../h/heap_compute_data_size.md)
  - [heap_fill_tuple](../h/heap_fill_tuple.md)
  - [toast_compress_datum](../t/toast_compress_datum.md)
  - [IndexInfoFindDataOffset](../I/IndexInfoFindDataOffset.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
- Constants used:
  - INDEX_MAX_KEYS
  - INDEX_NULL_MASK
  - INDEX_VAR_MASK
  - INDEX_SIZE_MASK
  - TOAST_INDEX_TARGET
  - TOAST_INDEX_HACK (compilation flag)
- Called from:
  - [index_form_tuple](index_form_tuple.md) (src/backend/access/common/indextuple.c:48)
  - [tuplesort_putindextuplevalues](../t/tuplesort_putindextuplevalues.md) (src/backend/utils/sort/tuplesortvariants.c:762)

## Notes and Other Information
- Located in src/backend/access/common/indextuple.c:65-240
- Enforces INDEX_MAX_KEYS limit on the number of attributes
- When TOAST_INDEX_HACK is enabled, performs comprehensive TOAST processing including external attribute fetching and compression
- Uses heap_fill_tuple internally but converts heap tuple masks to index tuple info masks
- Ensures the final tuple size fits within INDEX_SIZE_MASK constraints
- Memory allocation is performed with MemoryContextAllocZero to ensure clean initialization
- Critical for performance in sorting operations where memory management must be precise

## Simplified Source

```c
IndexTuple index_form_tuple_context(TupleDesc tupleDescriptor, const Datum *values,
                                   const bool *isnull, MemoryContext context) {
    char *tp;
    IndexTuple tuple;
    Size size, data_size, hoff;
    int i;
    unsigned short infomask = 0;
    bool hasnull = false;
    uint16 tupmask = 0;
    int numberOfAttributes = tupleDescriptor->natts;

    // Validate attribute count
    if (numberOfAttributes > INDEX_MAX_KEYS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
                       errmsg("number of index columns (%d) exceeds limit (%d)",
                              numberOfAttributes, INDEX_MAX_KEYS)));

#ifdef TOAST_INDEX_HACK
    Datum untoasted_values[INDEX_MAX_KEYS];
    bool untoasted_free[INDEX_MAX_KEYS];

    // Handle TOAST: detoast external attributes and compress large values
    for (i = 0; i < numberOfAttributes; i++) {
        Form_pg_attribute att = TupleDescAttr(tupleDescriptor, i);
        untoasted_values[i] = values[i];
        untoasted_free[i] = false;

        if (isnull[i] || att->attlen != -1) continue;

        // Fetch external attributes
        if (VARATT_IS_EXTERNAL(DatumGetPointer(values[i]))) {
            untoasted_values[i] = PointerGetDatum(
                detoast_external_attr((struct varlena *) DatumGetPointer(values[i])));
            untoasted_free[i] = true;
        }

        // Compress large values
        if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
            VARSIZE(DatumGetPointer(untoasted_values[i])) > TOAST_INDEX_TARGET &&
            (att->attstorage == TYPSTORAGE_EXTENDED || att->attstorage == TYPSTORAGE_MAIN)) {

            Datum cvalue = toast_compress_datum(untoasted_values[i], att->attcompression);
            if (DatumGetPointer(cvalue) != NULL) {
                if (untoasted_free[i]) pfree(DatumGetPointer(untoasted_values[i]));
                untoasted_values[i] = cvalue;
                untoasted_free[i] = true;
            }
        }
    }
#endif

    // Check for null values
    for (i = 0; i < numberOfAttributes; i++) {
        if (isnull[i]) {
            hasnull = true;
            break;
        }
    }

    if (hasnull) infomask |= INDEX_NULL_MASK;

    // Calculate sizes and allocate tuple
    hoff = IndexInfoFindDataOffset(infomask);
#ifdef TOAST_INDEX_HACK
    data_size = heap_compute_data_size(tupleDescriptor, untoasted_values, isnull);
#else
    data_size = heap_compute_data_size(tupleDescriptor, values, isnull);
#endif
    size = MAXALIGN(hoff + data_size);

    tp = (char *) MemoryContextAllocZero(context, size);
    tuple = (IndexTuple) tp;

    // Fill tuple data
    heap_fill_tuple(tupleDescriptor,
#ifdef TOAST_INDEX_HACK
                   untoasted_values,
#else
                   values,
#endif
                   isnull, (char *) tp + hoff, data_size, &tupmask,
                   (hasnull ? (bits8 *) tp + sizeof(IndexTupleData) : NULL));

#ifdef TOAST_INDEX_HACK
    // Cleanup temporary TOAST data
    for (i = 0; i < numberOfAttributes; i++) {
        if (untoasted_free[i]) pfree(DatumGetPointer(untoasted_values[i]));
    }
#endif

    // Set variable width mask
    if (tupmask & HEAP_HASVARWIDTH) infomask |= INDEX_VAR_MASK;

    // Validate final size
    if ((size & INDEX_SIZE_MASK) != size)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("index row requires %zu bytes, maximum size is %zu",
                              size, (Size) INDEX_SIZE_MASK)));

    tuple->t_info = infomask | size;
    return tuple;
}
```