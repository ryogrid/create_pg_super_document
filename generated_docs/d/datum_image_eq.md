# datum_image_eq

## Location
[src/backend/utils/adt/datum.c:266-337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datum.c#L266-L337)

## Overview
Compares two datums for identical byte-level contents, with special handling for TOAST decompression to ensure accurate comparison of variable-length data.

## Definition

```c
struct varlena *arg1val;
```
## Detailed Description
The  function performs exact byte-level comparison between two datums, with sophisticated handling for different storage types. Unlike , this function properly handles TOAST (The Oversized-Attribute Storage Technique) by detoasting variable-length data before comparison:

1. **Pass-by-value types**: Direct equality comparison using 
2. **Fixed-length pass-by-reference types**: Direct  of the specified type length
3. **Variable-length (varlena) types**: 
   - Uses  to get actual sizes
   - If sizes differ, returns false immediately
   - Detoasts both datums using 
   - Compares only the data portion (excluding varlena header) using 
   - Properly frees any temporary detoasted copies
4. **C-string types**: Calculates string lengths and performs byte-wise comparison including null terminators

This function provides true content equality checking, making it suitable for cases where exact data matching is required.

## Parameters / Member Variables
- : First datum to compare
- : Second datum to compare
- : Boolean indicating whether the type is passed by value (true) or by reference (false)
- : The declared type length (-1 for varlena, -2 for cstring, positive for fixed-length)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [toast_raw_datum_size](../t/toast_raw_datum_size.md)
  - PG_DETOAST_DATUM_PACKED
  - VARDATA_ANY
  - VARHDRSZ (constant for varlena header size)
  - [DatumGetCString](../D/DatumGetCString.md)
  - memcmp
  - strlen
  - [pfree](../p/pfree.md)
  - elog
- Called from (representative examples):
  - [_bt_keep_natts_fast](../b/_bt_keep_natts_fast.md)
  - [MemoizeHash_equal](../M/MemoizeHash_equal.md)
  - [ri_KeysEqual](../r/ri_KeysEqual.md)
  - [record_image_eq](../r/record_image_eq.md)

## Notes and Other Information
- This function properly handles TOAST, making it more reliable than  for variable-length data
- The function performs memory management for detoasted data, freeing temporary copies when necessary
- For varlena types, only the actual data is compared (header excluded), ensuring content-based equality
- C-string comparison includes the null terminator in the length calculation
- The function will throw an ERROR for invalid typLen values outside the expected range
- This is the preferred function when exact content matching is required, especially for TOAST-able data types
- Memory-efficient approach: early size comparison for varlena types avoids unnecessary detoasting when sizes differ

## Simplified Source

```c
bool
datum_image_eq(Datum value1, Datum value2, bool typByVal, int typLen)
{
    Size len1, len2;
    bool result = true;

    if (typByVal)
    {
        // Pass-by-value: direct comparison
        result = (value1 == value2);
    }
    else if (typLen > 0)
    {
        // Fixed-length pass-by-reference: direct memory comparison
        result = (memcmp(DatumGetPointer(value1),
                        DatumGetPointer(value2),
                        typLen) == 0);
    }
    else if (typLen == -1)
    {
        // Variable-length (varlena) type with TOAST handling
        len1 = toast_raw_datum_size(value1);
        len2 = toast_raw_datum_size(value2);

        if (len1 != len2)
            result = false;  // Different sizes, can't be equal
        else
        {
            struct varlena *arg1val;
            struct varlena *arg2val;

            // Detoast both datums for comparison
            arg1val = PG_DETOAST_DATUM_PACKED(value1);
            arg2val = PG_DETOAST_DATUM_PACKED(value2);

            // Compare only the data portion (excluding headers)
            result = (memcmp(VARDATA_ANY(arg1val),
                           VARDATA_ANY(arg2val),
                           len1 - VARHDRSZ) == 0);

            // Free any temporary detoasted copies
            if ((Pointer) arg1val != (Pointer) value1)
                pfree(arg1val);
            if ((Pointer) arg2val != (Pointer) value2)
                pfree(arg2val);
        }
    }
    else if (typLen == -2)
    {
        // C-string type
        char *s1 = DatumGetCString(value1);
        char *s2 = DatumGetCString(value2);
        len1 = strlen(s1) + 1;
        len2 = strlen(s2) + 1;

        if (len1 != len2)
            return false;
        result = (memcmp(s1, s2, len1) == 0);
    }
    else
        elog(ERROR, "unexpected typLen: %d", typLen);

    return result;
}
```