# textarray_to_strvaluelist

## Location
[src/backend/catalog/objectaddress.c:2074-2099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2074-L2099)

## Overview
Converts a PostgreSQL TEXT array into a List of string Values, transforming SQL array input into the format expected by PostgreSQL's object address resolution system.

## Definition
```c
static List *textarray_to_strvaluelist(ArrayType *arr)
```

## Detailed Description
This utility function serves as a bridge between SQL-level array inputs and PostgreSQL's internal object address resolution system. It takes a PostgreSQL ArrayType containing TEXT elements and converts each element into a string Value node, creating a List that matches the format expected by get_object_address and related functions.

The function performs element-by-element conversion, extracting each TEXT datum from the array, converting it to a C string, and wrapping it in a makeString Value node. It includes null checking to ensure that name or argument lists don't contain null values, which would be invalid for object address resolution.

## Parameters / Member Variables
- `arr`: PostgreSQL ArrayType containing TEXT elements to be converted to a string value list

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) (array element extraction)
  - TextDatumGetCString (TEXT datum to C string conversion) 
  - [makeString](../m/makeString.md) (creates string Value nodes)
  - [lappend](../l/lappend.md) (list append operation)
- Called from (representative examples):
  - [pg_get_object_address](../p/pg_get_object_address.md) (SQL function for object address resolution)

## Notes and Other Information
- Returns NIL (empty list) for empty arrays
- Validates that no array elements are NULL, raising an error if any null values are found
- Uses deconstruct_array_builtin for efficient built-in type array processing with TEXTOID
- The returned List contains Value nodes of type T_String, matching parser output format
- Part of the SQL interface for PostgreSQL's object address resolution system
- Error message specifically mentions "name or argument lists" to provide context about the restriction
- Memory for the returned list and string values is allocated in the current memory context