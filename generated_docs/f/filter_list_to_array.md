# filter_list_to_array

## Location
[src/backend/commands/event_trigger.c:356-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L356-L385)

## Overview
Transforms a List of String nodes representing a filter clause (like WHEN tag IN ('cmd1', 'cmd2')) into a text array format suitable for catalog storage.

## Definition

```c
struct_array_builtin(data, l, TEXTOID));
```
## Detailed Description
This function is part of PostgreSQL's event trigger system and serves as a utility to convert parser representations into catalog-storable format. In the parser, filter clauses like "WHEN tag IN ('cmd1', 'cmd2')" are represented by DefElem structures whose values are Lists of String nodes. However, in the catalog, these lists need to be stored as text arrays.

The function iterates through each string in the input list, converts each string to uppercase (for case-insensitive matching), converts it to PostgreSQL's text type, and constructs a PostgreSQL array from all the processed strings. The uppercase conversion ensures consistent storage format for command tags and prepares for potential case-insensitive filtering.

## Parameters / Member Variables
- `filterlist`: A List containing String nodes that represent filter values from a WHEN clause in event trigger definitions

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to get the number of elements)
  - [palloc](../p/palloc.md) (to allocate memory for the Datum array)
  - strVal (to extract string value from String nodes)
  - [pstrdup](../p/pstrdup.md) (to duplicate strings for modification)
  - [pg_ascii_toupper](../p/pg_ascii_toupper.md) (to convert characters to uppercase)
  - [cstring_to_text](../c/cstring_to_text.md) (to convert C strings to PostgreSQL text type)
  - [PointerGetDatum](../P/PointerGetDatum.md) (to convert pointers to Datum)
  - [construct_array_builtin](../c/construct_array_builtin.md) (to construct PostgreSQL array from Datum array)
  - [pfree](../p/pfree.md) (to free allocated memory)
- Called from (representative examples):
  - [insert_event_trigger_tuple](../i/insert_event_trigger_tuple.md)

## Notes and Other Information
- The function is static, meaning it's only used within the event_trigger.c file
- [Command](../C/Command.md) tags are stored in uppercase in the catalog for consistency
- The function handles memory management by allocating space for the Datum array and freeing temporary string duplicates
- The conversion to text arrays allows efficient storage and querying of filter conditions in the PostgreSQL catalog
- Future case-sensitive filter variables might require modifications to this function's uppercase conversion logic

## Simplified Source
```c
static Datum filter_list_to_array(List *filterlist) {
    // Allocate array to hold converted strings
    int len = list_length(filterlist);
    Datum *data = (Datum *) palloc(len * sizeof(Datum));

    int i = 0;
    ListCell *lc;
    foreach(lc, filterlist) {
        // Get string value and convert to uppercase
        const char *value = strVal(lfirst(lc));
        char *result = pstrdup(value);

        // Convert to uppercase for case-insensitive matching
        for (char *p = result; *p; p++) {
            *p = pg_ascii_toupper((unsigned char) *p);
        }

        // Convert to PostgreSQL text type and store in array
        data[i++] = PointerGetDatum(cstring_to_text(result));
        pfree(result);
    }

    // Construct and return PostgreSQL text array
    return PointerGetDatum(construct_array_builtin(data, len, TEXTOID));
}
```