# textToQualifiedNameList

## Location
[src/backend/utils/adt/varlena.c:3399-3456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3399-L3456)

## Overview
Converts a text object containing a qualified name into a list of String nodes, parsing dotted identifiers while handling quoting and case conversion rules.

## Definition

```c
List *
textToQualifiedNameList(text *textval)
```
## Detailed Description
The  function is a critical utility that parses text representations of qualified database object names (like 'schema.table' or 'database.schema.table') into a structured list format. This function is widely used throughout PostgreSQL for processing user-provided object names in SQL functions and commands.

The parsing process includes:
1. Converting the input text to a C string, handling possible TOAST decompression
2. Using  to split the name at dots while respecting SQL identifier quoting rules
3. Converting each parsed identifier into a String node for inclusion in the result list
4. Performing appropriate error checking for malformed names

The function handles PostgreSQL's identifier rules:
- Unquoted identifiers are converted to lowercase
- Double-quoted identifiers preserve case and can contain special characters
- Names are truncated if they exceed maximum identifier length
- Empty or malformed names generate appropriate error messages

## Parameters / Member Variables
- `*textval`: Input text datum containing the qualified name to be parsed (e.g., 'public.users' or '"MySchema"."MyTable"')
## Dependencies
- Functions called/Symbols referenced:
  - : Converts text datum to null-terminated C string
  - : Splits qualified identifier string at dots
  - : Creates a String node for list inclusion
  - : Frees the temporary name list
  - : List iteration macro
  - : Appends element to list
  - : Duplicates string in current memory context
  - : Frees allocated memory

- Called from (representative examples):
  - : Sequence value retrieval function
  - : Text to regclass conversion
  - : View definition retrieval
  - : Serial sequence name retrieval
  - : Current tuple ID by relation name
  - : Text search token type lookup
  - : ACL table name conversion

## Notes and Other Information
- This function is essential for PostgreSQL's SQL interface, enabling text-based object name resolution
- The function allocates memory for the result list and String nodes, which should be freed by the caller
- Error handling includes specific error codes (ERRCODE_INVALID_NAME) for malformed input
- The function is used extensively in system functions that accept qualified object names as text parameters
- Supports PostgreSQL's standard identifier syntax including schema qualification
- Location: src/backend/utils/adt/varlena.c:3399-3456