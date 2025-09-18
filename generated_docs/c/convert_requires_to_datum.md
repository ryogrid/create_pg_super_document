# convert_requires_to_datum

## Location
[src/backend/commands/extension.c:2313-2338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2313-L2338)

## Overview
This static helper function converts a PostgreSQL List of extension names (as C strings) into a PostgreSQL name[] array Datum for use in result sets.

## Definition


## Detailed Description
The convert_requires_to_datum function takes a List containing extension name strings and converts it into a PostgreSQL array datum of type name[]. This conversion is necessary when returning extension dependency information through the pg_available_extension_versions system view.

The function performs the following steps:
1. Determines the number of items in the input list
2. Allocates an array of Datum values to hold the converted names
3. Iterates through the list, converting each string to a name datum using the namein function
4. Constructs a PostgreSQL array of NAMEOID type containing all the converted names
5. Returns the array as a Datum

This function is essential for properly formatting extension dependency information in a way that can be returned through PostgreSQL's function result interface.

## Parameters / Member Variables
- : A PostgreSQL List containing char* pointers to extension names that are required dependencies

## Dependencies
- Functions called/Symbols referenced:
  - list_length (macro/function to get list length)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - lfirst (macro to get list cell content)
  - DirectFunctionCall1
  - namein
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [construct_array_builtin](construct_array_builtin.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
- Called from (representative examples):
  - [get_available_versions_for_extension](../g/get_available_versions_for_extension.md)

## Notes and Other Information
- This is a static (internal) function used only within extension.c
- The function is specifically designed to handle the 'requires' field from extension control files
- Uses PostgreSQL's NAMEOID type which is appropriate for extension names
- Memory allocation is handled through PostgreSQL's memory context system (palloc)
- The resulting array can be returned directly as part of a SQL result set
- The function assumes all strings in the input list are valid extension names
- Used exclusively by get_available_versions_for_extension to format dependency arrays in the pg_available_extension_versions view