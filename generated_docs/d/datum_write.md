# datum_write

## Location
[src/backend/utils/adt/rangetypes.c:2709-2785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2709-L2785)

## Overview
A static function that writes a datum to a specified memory location with proper alignment and returns the updated pointer position.

## Definition

```c
static Pointer
datum_write(Pointer ptr, Datum datum, bool typbyval, char typalign,
			int16 typlen, char typstorage)
```
## Detailed Description
This function is part of PostgreSQL's range type serialization system and handles the physical writing of datum values to memory buffers. It supports multiple storage formats including pass-by-value types, variable-length arrays (varlena), C-strings, and fixed-length pass-by-reference types. The function optimizes storage by converting eligible varlena types to short format when possible and ensures proper memory alignment for each data type. It includes safety checks to prevent storing toast pointers within range objects.

## Parameters / Member Variables
- `ptr`: Memory pointer where the datum should be written
- `datum`: The datum value to write
- `typbyval`: Whether the type is passed by value
- `typalign`: Type alignment requirement ('c', 's', 'i', 'd')
- `typlen`: Type length (-1 for varlena, -2 for cstring, positive for fixed length)
- `typstorage`: Type storage strategy ('p', 'e', 'm', 'x')
## Dependencies
- Functions called/Symbols referenced:
  - att_align_nominal
  - [store_att_byval](../s/store_att_byval.md)
  - VARATT_IS_EXTERNAL
  - VARATT_IS_SHORT
  - VARSIZE_SHORT
  - TYPE_IS_PACKABLE
  - VARATT_CAN_MAKE_SHORT
  - VARATT_CONVERTED_SHORT_SIZE
  - SET_VARSIZE_SHORT
  - VARDATA
  - VARSIZE
  - [DatumGetCString](../D/DatumGetCString.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - TYPALIGN_CHAR
- Called from (representative examples):
  - [range_serialize](../r/range_serialize.md)

## Notes and Other Information
This function implements comprehensive datum serialization logic with several optimization strategies. It converts eligible varlena types to short format to save space, handles alignment requirements correctly for different data types, and includes error checking to prevent toast pointer storage. The function is critical for range type persistence and is called for both lower and upper bound values during range serialization. It advances the pointer by the actual data length written, making it suitable for sequential writing operations.

## Simplified Source

```c
static Pointer datum_write(Pointer ptr, Datum datum, bool typbyval,
                          char typalign, int16 typlen, char typstorage) {
    Size data_length;

    if (typbyval) {
        // Pass-by-value: align and store directly
        ptr = (char *) att_align_nominal(ptr, typalign);
        store_att_byval(ptr, datum, typlen);
        data_length = typlen;
    } else if (typlen == -1) {
        // Variable length (varlena)
        Pointer val = DatumGetPointer(datum);

        if (VARATT_IS_EXTERNAL(val)) {
            elog(ERROR, "cannot store a toast pointer inside a range");
        } else if (VARATT_IS_SHORT(val)) {
            // Already short varlena - copy directly (no alignment)
            data_length = VARSIZE_SHORT(val);
            memcpy(ptr, val, data_length);
        } else if (TYPE_IS_PACKABLE(typlen, typstorage) &&
                   VARATT_CAN_MAKE_SHORT(val)) {
            // Convert to short varlena format
            data_length = VARATT_CONVERTED_SHORT_SIZE(val);
            SET_VARSIZE_SHORT(ptr, data_length);
            memcpy(ptr + 1, VARDATA(val), data_length - 1);
        } else {
            // Full 4-byte header varlena
            ptr = (char *) att_align_nominal(ptr, typalign);
            data_length = VARSIZE(val);
            memcpy(ptr, val, data_length);
        }
    } else if (typlen == -2) {
        // C-string (null-terminated)
        data_length = strlen(DatumGetCString(datum)) + 1;
        memcpy(ptr, DatumGetPointer(datum), data_length);
    } else {
        // Fixed-length pass-by-reference
        ptr = (char *) att_align_nominal(ptr, typalign);
        data_length = typlen;
        memcpy(ptr, DatumGetPointer(datum), data_length);
    }

    return ptr + data_length;
}
```