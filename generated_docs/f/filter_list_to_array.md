# filter_list_to_array

## Location
src/backend/commands/event_trigger.c: 356 - 385

## Overview
Transforms a List of String nodes representing a filter clause (like WHEN tag IN ('cmd1', 'cmd2')) into a text array format suitable for catalog storage.

## Definition


## Detailed Description
This function is part of PostgreSQL's event trigger system and serves as a utility to convert parser representations into catalog-storable format. In the parser, filter clauses like "WHEN tag IN ('cmd1', 'cmd2')" are represented by DefElem structures whose values are Lists of String nodes. However, in the catalog, these lists need to be stored as text arrays.

The function iterates through each string in the input list, converts each string to uppercase (for case-insensitive matching), converts it to PostgreSQL's text type, and constructs a PostgreSQL array from all the processed strings. The uppercase conversion ensures consistent storage format for command tags and prepares for potential case-insensitive filtering.

## Parameters / Member Variables
- `filterlist`: A List containing String nodes that represent filter values from a WHEN clause in event trigger definitions

## Dependencies
- Functions called/Symbols referenced:
  - list_length (to get the number of elements)
  - palloc (to allocate memory for the Datum array)
  - strVal (to extract string value from String nodes)
  - pstrdup (to duplicate strings for modification)
  - pg_ascii_toupper (to convert characters to uppercase)
  - cstring_to_text (to convert C strings to PostgreSQL text type)
  - PointerGetDatum (to convert pointers to Datum)
  - construct_array_builtin (to construct PostgreSQL array from Datum array)
  - pfree (to free allocated memory)
- Called from (representative examples):
  - insert_event_trigger_tuple

## Notes and Other Information
- The function is static, meaning it's only used within the event_trigger.c file
- Command tags are stored in uppercase in the catalog for consistency
- The function handles memory management by allocating space for the Datum array and freeing temporary string duplicates
- The conversion to text arrays allows efficient storage and querying of filter conditions in the PostgreSQL catalog
- Future case-sensitive filter variables might require modifications to this function's uppercase conversion logic