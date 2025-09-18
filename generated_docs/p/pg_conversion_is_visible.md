# pg_conversion_is_visible

## Location
src/backend/catalog/namespace.c: 4992 - 5005

## Overview
Determines whether a given encoding conversion is visible in the current search path, returning NULL if the conversion does not exist.

## Definition
```c
Datum pg_conversion_is_visible(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that checks the visibility of an encoding conversion identified by its OID in the current search path. It serves as a wrapper around the internal ConversionIsVisibleExt function, providing a SQL interface for conversion visibility checks. The function returns a boolean value indicating whether the conversion is accessible from the current namespace context, or NULL if the conversion doesn't exist in the system catalogs.

The visibility check considers the current search path and ensures that the conversion would be found by name resolution. A conversion is considered visible if it exists in a namespace that's in the current search path and wouldn't be shadowed by another conversion with the same name in an earlier namespace.

## Parameters / Member Variables
- First argument (OID): The object identifier of the encoding conversion to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - ConversionIsVisibleExt: Internal function that performs the actual visibility check
  - PG_RETURN_NULL: Returns NULL value to SQL caller
  - PG_RETURN_BOOL: Returns boolean value to SQL caller
- Called from (representative examples):
  - Available as SQL function pg_conversion_is_visible()

## Notes and Other Information
- This is a system information function available in SQL as pg_conversion_is_visible(oid)
- Returns NULL rather than FALSE when the conversion doesn't exist, following PostgreSQL's convention for visibility functions
- The function uses the is_missing parameter of ConversionIsVisibleExt to distinguish between "not visible" and "doesn't exist"
- Part of PostgreSQL's namespace and visibility system for schema-qualified object resolution
- Encoding conversions are used for character set conversion between different encodings
- Located in src/backend/catalog/namespace.c:4992-5005