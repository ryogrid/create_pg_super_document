# DecodeTextArrayToBitmapset

## Location
[src/backend/utils/cache/evtcache.c:222-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/evtcache.c#L222-L254)

## Overview
Converts a PostgreSQL text array containing command tag names into a Bitmapset representation for efficient command tag matching in event triggers.

## Definition
```c
static Bitmapset *DecodeTextArrayToBitmapset(Datum array)
```

## Detailed Description
DecodeTextArrayToBitmapset is a static utility function that processes a PostgreSQL text array (text[]) datum and converts it into a Bitmapset containing CommandTag enumeration values. This function is specifically used in the event trigger cache system to decode the evttags column from the pg_event_trigger system catalog. The function validates that the input is a proper 1-dimensional text array without null elements, then iterates through each text element, converts it to a command tag enum using GetCommandTagEnum(), and adds it to a bitmapset using bms_add_member().

The resulting bitmapset provides an efficient representation for testing whether specific command tags are included in an event trigger's filter set, enabling fast filtering during event trigger execution.

## Parameters / Member Variables
- `array`: A Datum representing a PostgreSQL text[] array containing command tag names that need to be converted to a bitmapset

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - TextDatumGetCString
  - [GetCommandTagEnum](../G/GetCommandTagEnum.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [BuildEventTriggerCache](../B/BuildEventTriggerCache.md) (src/backend/utils/cache/evtcache.c:183)

## Notes and Other Information
- Validates input array is 1-dimensional, contains no nulls, and has TEXTOID element type
- Uses deconstruct_array_builtin for efficient array element extraction
- Converts each text element to C string, maps to CommandTag enum, then frees the string
- Returns NULL bitmapset if the input array is empty
- Throws ERROR if array format validation fails
- Memory management includes proper cleanup of temporary strings and deconstructed array elements
- Used specifically for processing the evttags column in pg_event_trigger system catalog
- Part of the event trigger cache optimization system for fast command tag filtering

## Simplified Source

```c
static Bitmapset *DecodeTextArrayToBitmapset(Datum array)
{
    ArrayType *arr = DatumGetArrayTypeP(array);
    Datum *elems;
    Bitmapset *bms;
    int i;
    int nelems;

    // Validate array format: must be 1-D text array with no nulls
    if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "expected 1-D text array");

    // Deconstruct the array into individual text elements
    deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems);

    // Convert each text element to CommandTag enum and add to bitmapset
    for (bms = NULL, i = 0; i < nelems; ++i)
    {
        char *str = TextDatumGetCString(elems[i]);

        bms = bms_add_member(bms, GetCommandTagEnum(str));
        pfree(str);
    }

    // Clean up deconstructed array elements
    pfree(elems);

    return bms;
}
```