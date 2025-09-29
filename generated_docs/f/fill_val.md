# fill_val

## Location
[src/backend/access/common/heaptuple.c:271-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L271-L399)

## Overview
The fill_val function is a per-attribute helper function used by heap_fill_tuple and other routines that build PostgreSQL heap tuples, responsible for filling in either a data value or a bit in the null bitmask for a single column attribute.

## Definition

```c
structed tuple doesn't depend on it
				 */
				ExpandedObjectHeader *eoh = DatumGetEOHP(datum);
```
## Detailed Description
fill_val is a core utility function that handles the serialization of individual column values into the PostgreSQL heap tuple format. The function manages both the null bitmap construction and the actual data storage, handling different data types including pass-by-value types, variable-length (varlena) types, C-strings, and fixed-length pass-by-reference types.

The function performs several critical tasks:
- Updates the null bitmap when building tuples with null values
- Handles proper data alignment based on the attribute's alignment requirements  
- Manages different storage formats for variable-length data (short varlena, external references, expanded objects)
- Converts between different varlena representations for optimal storage
- Updates tuple info mask flags to indicate presence of null values, variable-width data, or external references

## Parameters / Member Variables
- : Form_pg_attribute structure containing attribute metadata (alignment, length, pass-by-value flag, etc.)
- : Pointer to pointer to the current byte in the null bitmap being constructed
- : Pointer to the current bit mask for setting individual bits within the null bitmap byte
- : Pointer to pointer to the current position in the tuple's data area
- : Pointer to the tuple's info mask flags that indicate tuple characteristics
- : The actual data value to be stored (as a PostgreSQL Datum)
- : Boolean flag indicating whether this attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - att_align_nominal (data alignment)
  - [store_att_byval](../s/store_att_byval.md) (store pass-by-value data)
  - [DatumGetEOHP](../D/DatumGetEOHP.md) (expanded object handling)
  - [EOH_get_flat_size](../E/EOH_get_flat_size.md), EOH_flatten_into (expanded object flattening)
  - VARSIZE_EXTERNAL, VARSIZE_SHORT, VARSIZE (varlena size calculations)
  - [DatumGetCString](../D/DatumGetCString.md), DatumGetPointer (datum extraction)
  - SET_VARSIZE_SHORT, VARDATA (varlena manipulation)
- Called from (representative examples):
  - [heap_fill_tuple](../h/heap_fill_tuple.md)
  - [expand_tuple](../e/expand_tuple.md)

## Notes and Other Information
- This is a static inline function for performance optimization
- Handles the complex logic of PostgreSQL's variable-length attribute storage
- Critical for tuple construction performance as it's called for every non-dropped column
- Manages the conversion between different varlena formats (short vs. full header) for space efficiency
- Updates infomask flags (HEAP_HASNULL, HEAP_HASVARWIDTH, HEAP_HASEXTERNAL) that are essential for tuple interpretation
- The function uses pointer arithmetic and bit manipulation for efficient tuple layout construction

## Simplified Source

```c
static inline void fill_val(Form_pg_attribute att,
                           bits8 **bit,
                           int *bitmask,
                           char **dataP,
                           uint16 *infomask,
                           Datum datum,
                           bool isnull) {
    Size data_length;
    char *data = *dataP;

    // Handle null bitmap construction
    if (bit != NULL) {
        // Advance bitmap position
        if (*bitmask != HIGHBIT) {
            *bitmask <<= 1;
        } else {
            *bit += 1;
            **bit = 0x0;
            *bitmask = 1;
        }

        // Set null bit and return early if value is null
        if (isnull) {
            *infomask |= HEAP_HASNULL;
            return;
        }

        **bit |= *bitmask;
    }

    // Handle different data types
    if (att->attbyval) {
        // Pass-by-value: store value directly
        data = (char *) att_align_nominal(data, att->attalign);
        store_att_byval(data, datum, att->attlen);
        data_length = att->attlen;
    }
    else if (att->attlen == -1) {
        // Variable-length (varlena) data
        Pointer val = DatumGetPointer(datum);
        *infomask |= HEAP_HASVARWIDTH;

        if (VARATT_IS_EXTERNAL(val)) {
            if (VARATT_IS_EXTERNAL_EXPANDED(val)) {
                // Flatten expanded objects
                ExpandedObjectHeader *eoh = DatumGetEOHP(datum);
                data = (char *) att_align_nominal(data, att->attalign);
                data_length = EOH_get_flat_size(eoh);
                EOH_flatten_into(eoh, data, data_length);
            } else {
                // External reference
                *infomask |= HEAP_HASEXTERNAL;
                data_length = VARSIZE_EXTERNAL(val);
                memcpy(data, val, data_length);
            }
        }
        else if (VARATT_IS_SHORT(val)) {
            // Short varlena format
            data_length = VARSIZE_SHORT(val);
            memcpy(data, val, data_length);
        }
        else {
            // Full 4-byte header varlena
            data = (char *) att_align_nominal(data, att->attalign);
            data_length = VARSIZE(val);
            memcpy(data, val, data_length);
        }
    }
    else if (att->attlen == -2) {
        // C-string: null-terminated string
        *infomask |= HEAP_HASVARWIDTH;
        data_length = strlen(DatumGetCString(datum)) + 1;
        memcpy(data, DatumGetPointer(datum), data_length);
    }
    else {
        // Fixed-length pass-by-reference
        data = (char *) att_align_nominal(data, att->attalign);
        data_length = att->attlen;
        memcpy(data, DatumGetPointer(datum), data_length);
    }

    // Advance data pointer
    data += data_length;
    *dataP = data;
}
```