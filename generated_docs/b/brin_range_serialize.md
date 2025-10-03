# brin_range_serialize

## Location
[src/backend/access/brin/brin_minmax_multi.c:576-720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L576-L720)

## Overview
Serializes the in-memory representation of BRIN range data into a compact varlena value for storage or transmission.

## Definition

```c
static SerializedRanges *
brin_range_serialize(Ranges *range)
```
## Detailed Description
This function takes an in-memory Ranges structure and converts it into a serialized format (SerializedRanges) that can be stored efficiently. The serialization process involves copying the header information and then serializing the individual values according to their data type characteristics. The function handles different data types appropriately:

- **By-value types**: Uses proper alignment and endian-safe copying via store_att_byval
- **Fixed-length by-reference types**: Direct memory copy of the fixed size
- **Variable-length types (varlena)**: Copies the entire variable-length structure including its size header
- **C-string types**: Copies the string including the null terminator

Before serialization, the function deduplicates values and performs various sanity checks to ensure data integrity. The resulting serialized structure is a varlena object with a proper PostgreSQL varlena header.

## Parameters / Member Variables
- `*range`: Input Ranges structure containing the in-memory representation of range data to be serialized
## Dependencies
- Functions called/Symbols referenced:
  - [range_deduplicate_values](../r/range_deduplicate_values.md)
  - [get_typbyval](../g/get_typbyval.md)
  - [get_typlen](../g/get_typlen.md)
  - VARSIZE_ANY
  - [DatumGetCString](../D/DatumGetCString.md)
  - SET_VARSIZE
  - [store_att_byval](../s/store_att_byval.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [brin_minmax_multi_serialize](brin_minmax_multi_serialize.md)
  - [brin_minmax_multi_union](brin_minmax_multi_union.md)

## Notes and Other Information
- The function performs extensive sanity checks using Assert() to validate the input range structure
- The serialized output is designed to be compact and type-aware
- Memory layout is carefully managed to avoid buffer overflows
- The function handles endianness considerations for by-value types
- After serialization, the range data is compacted to the target maximum values size

## Simplified Source

```c
static SerializedRanges *
brin_range_serialize(Ranges *range)
{
    Size        len;
    int         nvalues;
    SerializedRanges *serialized;
    Oid         typid;
    int         typlen;
    bool        typbyval;
    char       *ptr;

    // Deduplicate values first
    range_deduplicate_values(range);

    // Calculate total values count (ranges + individual values)
    nvalues = 2 * range->nranges + range->nvalues;

    // Get type information
    typid = range->typid;
    typbyval = get_typbyval(typid);
    typlen = get_typlen(typid);

    // Calculate space needed for serialization
    len = offsetof(SerializedRanges, data);

    if (typlen == -1)  // varlena types
    {
        for (int i = 0; i < nvalues; i++)
            len += VARSIZE_ANY(range->values[i]);
    }
    else if (typlen == -2)  // cstring types
    {
        for (int i = 0; i < nvalues; i++)
            len += strlen(DatumGetCString(range->values[i])) + 1;
    }
    else  // fixed-length types
    {
        len += nvalues * typlen;
    }

    // Allocate and initialize serialized structure
    serialized = (SerializedRanges *) palloc0(len);
    SET_VARSIZE(serialized, len);

    serialized->typid = typid;
    serialized->nranges = range->nranges;
    serialized->nvalues = range->nvalues;
    serialized->maxvalues = range->target_maxvalues;

    // Copy values according to type
    ptr = serialized->data;
    for (int i = 0; i < nvalues; i++)
    {
        if (typbyval)
        {
            // Copy by-value types with proper alignment
            Datum tmp;
            store_att_byval(&tmp, range->values[i], typlen);
            memcpy(ptr, &tmp, typlen);
            ptr += typlen;
        }
        else if (typlen > 0)
        {
            // Fixed-length by-reference types
            memcpy(ptr, DatumGetPointer(range->values[i]), typlen);
            ptr += typlen;
        }
        else if (typlen == -1)
        {
            // Variable-length types
            int tmp = VARSIZE_ANY(DatumGetPointer(range->values[i]));
            memcpy(ptr, DatumGetPointer(range->values[i]), tmp);
            ptr += tmp;
        }
        else if (typlen == -2)
        {
            // C-string types
            int tmp = strlen(DatumGetCString(range->values[i])) + 1;
            memcpy(ptr, DatumGetCString(range->values[i]), tmp);
            ptr += tmp;
        }
    }

    return serialized;
}
```