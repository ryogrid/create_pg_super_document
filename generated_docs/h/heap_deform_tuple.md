# heap_deform_tuple

## Location
[src/backend/access/common/heaptuple.c:1345-1433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L1345-L1433)

## Overview
Extracts all attribute values and null indicators from a HeapTuple into caller-provided arrays, serving as the inverse operation to heap_form_tuple.

## Definition
```c
void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull)
```

## Detailed Description
This function efficiently extracts all attribute data from a HeapTuple into separate arrays. It performs several optimizations:

1. **Null handling**: Checks the tuple's null bitmap to identify null attributes and sets corresponding entries
2. **Attribute caching**: Uses and maintains `attcacheoff` values in the TupleDesc to speed up repeated access
3. **Alignment handling**: Properly handles data alignment requirements for different attribute types
4. **Variable-length attributes**: Handles varlena types with special alignment considerations
5. **Missing attributes**: For tuples with fewer attributes than expected (inheritance scenarios), fills remaining positions with missing values

The function is significantly more efficient than repeated `heap_getattr` calls, providing O(N) performance instead of O(N²) when accessing multiple attributes.

## Parameters / Member Variables
- `tuple`: The HeapTuple to extract data from
- `tupleDesc`: TupleDesc describing the expected tuple structure and types
- `values`: Caller-allocated array to receive Datum values (size = tupleDesc->natts)
- `isnull`: Caller-allocated array to receive null indicators (size = tupleDesc->natts)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHasNulls
  - HeapTupleHeaderGetNatts
  - TupleDescAttr
  - [att_isnull](../a/att_isnull.md)
  - att_align_nominal
  - att_align_pointer
  - fetchatt
  - att_addlength_pointer
  - [getmissingattr](../g/getmissingattr.md)
- Called from (representative examples):
  - [heap_modify_tuple](heap_modify_tuple.md)
  - [heap_modify_tuple_by_cols](heap_modify_tuple_by_cols.md)
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)
  - [SPI_modifytuple](../S/SPI_modifytuple.md)
  - [record_out](../r/record_out.md), record_cmp (tuple comparison functions)

## Notes and Other Information
- For pass-by-reference datatypes, returned Datum pointers point directly into the tuple data
- Handles inheritance scenarios where tuples may have more or fewer attributes than expected
- Maintains attribute offset cache (`attcacheoff`) when possible to optimize future accesses
- Sets `slow` flag when encountering null attributes or variable-length types that prevent caching
- Caller must ensure arrays are sized according to `tupleDesc->natts`
- Used extensively throughout PostgreSQL for tuple processing, type conversion, and data access
- Critical performance path in many query execution scenarios

## Simplified Source

```c
void heap_deform_tuple(HeapTuple tuple, TupleDesc tupleDesc, Datum *values, bool *isnull) {
    HeapTupleHeader tup = tuple->t_data;
    bool hasnulls = HeapTupleHasNulls(tuple);
    int tdesc_natts = tupleDesc->natts;
    int natts = HeapTupleHeaderGetNatts(tup);
    char *tp = (char *) tup + tup->t_hoff;  // Start of tuple data
    uint32 off = 0;  // Current offset
    bits8 *bp = tup->t_bits;  // Null bitmap
    bool slow = false;  // Can we use cached offsets?

    // Don't read more attributes than caller expects
    natts = Min(natts, tdesc_natts);

    // Extract each attribute
    for (int attnum = 0; attnum < natts; attnum++) {
        Form_pg_attribute thisatt = TupleDescAttr(tupleDesc, attnum);

        // Handle null attributes
        if (hasnulls && att_isnull(attnum, bp)) {
            values[attnum] = (Datum) 0;
            isnull[attnum] = true;
            slow = true;  // Nulls prevent offset caching
            continue;
        }

        isnull[attnum] = false;

        // Calculate or use cached attribute offset
        if (!slow && thisatt->attcacheoff >= 0) {
            off = thisatt->attcacheoff;
        } else {
            // Handle alignment for different attribute types
            if (thisatt->attlen == -1) {
                // Variable-length attribute: careful alignment
                if (!slow && off == att_align_nominal(off, thisatt->attalign)) {
                    thisatt->attcacheoff = off;
                } else {
                    off = att_align_pointer(off, thisatt->attalign, -1, tp + off);
                    slow = true;
                }
            } else {
                // Fixed-length attribute: use nominal alignment
                off = att_align_nominal(off, thisatt->attalign);
                if (!slow) {
                    thisatt->attcacheoff = off;
                }
            }
        }

        // Extract the attribute value
        values[attnum] = fetchatt(thisatt, tp + off);

        // Move to next attribute
        off = att_addlength_pointer(off, thisatt->attlen, tp + off);
        if (thisatt->attlen <= 0) {
            slow = true;  // Variable-length prevents caching
        }
    }

    // Handle missing attributes (inheritance scenarios)
    for (; attnum < tdesc_natts; attnum++) {
        values[attnum] = getmissingattr(tupleDesc, attnum + 1, &isnull[attnum]);
    }
}
```