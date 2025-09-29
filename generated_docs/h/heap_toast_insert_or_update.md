# heap_toast_insert_or_update

## Location
[src/backend/access/heap/heaptoast.c:96-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heaptoast.c#L96-L349)

## Overview
Handles TOAST processing for INSERT or UPDATE operations by compressing and/or externalizing large attributes to make the tuple fit within size constraints.

## Definition
```c
HeapTuple heap_toast_insert_or_update(Relation rel, HeapTuple newtup, HeapTuple oldtup, int options)
```

## Detailed Description
The `heap_toast_insert_or_update` function implements PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) processing for new or updated tuples. It follows a multi-phase strategy to reduce tuple size:

1. **Phase 1**: Inline compress attributes with EXTENDED storage, and externalize very large EXTENDED/EXTERNAL attributes immediately
2. **Phase 2**: Externalize remaining EXTENDED/EXTERNAL attributes that are still inline  
3. **Phase 3**: Inline compress attributes with MAIN storage
4. **Phase 4**: Externalize MAIN attributes (with higher size threshold)

The function preserves the original input tuples and returns either the original tuple (if no toasting needed) or a new palloc'd tuple with modified values. It handles both INSERT (oldtup=NULL) and UPDATE scenarios, cleaning up old toast entries that are no longer referenced.

## Parameters / Member Variables
- `rel`: The relation being inserted into or updated
- `newtup`: The candidate new tuple to be inserted/updated  
- `oldtup`: The old row version for UPDATE operations, or NULL for INSERT
- `options`: Options to be passed to heap_insert() for toast rows

## Dependencies
- Functions called/Symbols referenced:
  - [heap_deform_tuple](heap_deform_tuple.md)
  - [toast_tuple_init](../t/toast_tuple_init.md)
  - [toast_tuple_find_biggest_attribute](../t/toast_tuple_find_biggest_attribute.md)  
  - [toast_tuple_try_compression](../t/toast_tuple_try_compression.md)
  - [toast_tuple_externalize](../t/toast_tuple_externalize.md)
  - [toast_tuple_cleanup](../t/toast_tuple_cleanup.md)
  - [heap_compute_data_size](heap_compute_data_size.md)
  - [heap_fill_tuple](heap_fill_tuple.md)
  - RelationGetToastTupleTarget
- Called from (representative examples):
  - [heap_prepare_insert](heap_prepare_insert.md)
  - [heap_update](heap_update.md)
  - [raw_heap_insert](../r/raw_heap_insert.md)

## Notes and Other Information
- Only operates on plain relations (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW)
- Uses a four-phase approach with progressively more aggressive strategies to reduce tuple size
- Implements different size thresholds for MAIN vs EXTENDED/EXTERNAL storage types
- Handles speculative insertions by filtering out HEAP_INSERT_SPECULATIVE option
- The algorithm is designed to avoid unnecessary work by externalizing very large values early
- Returns original tuple unchanged if no toasting is required, otherwise returns a new tuple

## Simplified Source
```c
HeapTuple heap_toast_insert_or_update(Relation rel, HeapTuple newtup, HeapTuple oldtup, int options) {
    HeapTuple result_tuple;
    TupleDesc tupleDesc = rel->rd_att;
    int numAttrs = tupleDesc->natts;
    Size maxDataLen, hoff;

    // Arrays for tuple data extraction
    bool toast_isnull[MaxHeapAttributeNumber];
    bool toast_oldisnull[MaxHeapAttributeNumber];
    Datum toast_values[MaxHeapAttributeNumber];
    Datum toast_oldvalues[MaxHeapAttributeNumber];
    ToastAttrInfo toast_attr[MaxHeapAttributeNumber];
    ToastTupleContext ttc;

    // Filter out speculative insertion flag
    options &= ~HEAP_INSERT_SPECULATIVE;

    // Extract tuple data into arrays
    heap_deform_tuple(newtup, tupleDesc, toast_values, toast_isnull);
    if (oldtup != NULL)
        heap_deform_tuple(oldtup, tupleDesc, toast_oldvalues, toast_oldisnull);

    // Initialize toast context
    toast_tuple_init(&ttc);

    // Calculate maximum data length target
    hoff = calculate_header_overhead(numAttrs, ttc.ttc_flags);
    maxDataLen = RelationGetToastTupleTarget(rel, TOAST_TUPLE_TARGET) - hoff;

    // Phase 1: Compress EXTENDED attributes, externalize very large ones
    while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen) {
        int biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, false);
        if (biggest_attno < 0) break;

        if (is_extended_storage(biggest_attno))
            toast_tuple_try_compression(&ttc, biggest_attno);

        if (is_oversized_and_has_toast_table(biggest_attno))
            toast_tuple_externalize(&ttc, biggest_attno, options);
    }

    // Phase 2: Externalize remaining EXTENDED/EXTERNAL attributes
    while (still_oversized_and_has_toast_table()) {
        int biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, false);
        if (biggest_attno < 0) break;
        toast_tuple_externalize(&ttc, biggest_attno, options);
    }

    // Phase 3: Compress MAIN storage attributes
    while (heap_compute_data_size(tupleDesc, toast_values, toast_isnull) > maxDataLen) {
        int biggest_attno = toast_tuple_find_biggest_attribute(&ttc, true, true);
        if (biggest_attno < 0) break;
        toast_tuple_try_compression(&ttc, biggest_attno);
    }

    // Phase 4: Externalize MAIN attributes (higher threshold)
    maxDataLen = TOAST_TUPLE_TARGET_MAIN - hoff;
    while (still_oversized_and_has_toast_table()) {
        int biggest_attno = toast_tuple_find_biggest_attribute(&ttc, false, true);
        if (biggest_attno < 0) break;
        toast_tuple_externalize(&ttc, biggest_attno, options);
    }

    // Build new tuple if any changes were made
    if ((ttc.ttc_flags & TOAST_NEEDS_CHANGE) != 0) {
        result_tuple = build_new_tuple_with_toasted_values(&ttc, newtup, tupleDesc);
    } else {
        result_tuple = newtup;
    }

    toast_tuple_cleanup(&ttc);
    return result_tuple;
}
```