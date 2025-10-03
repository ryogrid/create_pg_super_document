# heapam_relation_needs_toast_table

## Location
[src/backend/access/heap/heapam_handler.c:2040-2087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L2040-L2087)

## Overview
Determines whether a heap relation requires a TOAST table by analyzing tuple size and the presence of toastable attributes.

## Definition
```c
static bool heapam_relation_needs_toast_table(Relation rel)
```

## Detailed Description
This function evaluates whether a relation needs an associated TOAST (The Oversized-Attribute Storage Technique) table for storing large attribute values. It performs two key checks: first, it determines if the relation has any attributes that can be toasted (variable-length attributes with storage type other than TYPSTORAGE_PLAIN), and second, it calculates whether the maximum possible tuple length could exceed TOAST_TUPLE_THRESHOLD.

The function iterates through all non-dropped attributes, calculating the total data length by considering fixed-length attributes directly and determining maximum sizes for variable-length attributes. For variable-length types with unknown maximum size (indicated by type_maximum_size returning -1), it immediately returns true since such unlimited-length attributes require TOAST storage. The final calculation includes tuple header overhead and null bitmap space to determine if the total tuple size would exceed the TOAST threshold.

## Parameters / Member Variables
- `rel`: The relation being evaluated for TOAST table requirements

## Dependencies
- Functions called/Symbols referenced:
  - att_align_nominal
  - [type_maximum_size](../t/type_maximum_size.md)
  - TYPSTORAGE_PLAIN
  - SizeofHeapTupleHeader
  - BITMAPLEN
  - TOAST_TUPLE_THRESHOLD
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md)

## Notes and Other Information
This function implements the logic for PostgreSQLs automatic TOAST table creation. It avoids creating unnecessary TOAST tables for relations with only small variable-length attributes (like "varchar(20)") while ensuring that relations with potentially large tuples get proper TOAST support. The calculation includes proper alignment considerations and accounts for tuple header overhead, providing an accurate assessment of storage requirements.

## Simplified Source

```c
static bool heapam_relation_needs_toast_table(Relation rel) {
    int32 data_length = 0;
    bool maxlength_unknown = false;
    bool has_toastable_attrs = false;
    TupleDesc tupdesc = rel->rd_att;
    int32 tuple_length;

    // Examine each attribute to calculate total data length
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);

        if (att->attisdropped)
            continue;

        // Apply alignment padding
        data_length = att_align_nominal(data_length, att->attalign);

        if (att->attlen > 0) {
            // Fixed-length types are never toastable
            data_length += att->attlen;
        } else {
            // Variable-length type - check if toastable and get max size
            int32 maxlen = type_maximum_size(att->atttypid, att->atttypmod);

            if (maxlen < 0) {
                maxlength_unknown = true;  // Unlimited length type
            } else {
                data_length += maxlen;
            }

            if (att->attstorage != TYPSTORAGE_PLAIN)
                has_toastable_attrs = true;
        }
    }

    // No toastable attributes means no TOAST table needed
    if (!has_toastable_attrs)
        return false;

    // Any unlimited-length attributes require TOAST
    if (maxlength_unknown)
        return true;

    // Calculate total tuple size including header and null bitmap
    tuple_length = MAXALIGN(SizeofHeapTupleHeader + BITMAPLEN(tupdesc->natts)) +
                   MAXALIGN(data_length);

    return (tuple_length > TOAST_TUPLE_THRESHOLD);
}
```