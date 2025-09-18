# deconstruct_array_builtin

## Location
[src/backend/utils/adt/arrayfuncs.c:3685-3754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L3685-L3754)

## Overview
A specialized version of deconstruct_array that automatically determines element type properties for PostgreSQL built-in data types, eliminating the need for the caller to provide type information.

## Definition


## Detailed Description
The  function is a convenience wrapper around  that handles built-in PostgreSQL data types. Instead of requiring the caller to provide element length, pass-by-value status, and alignment information, this function uses hardcoded knowledge about common built-in types to automatically determine these properties.

The function supports a predefined set of built-in types including CHAR, CSTRING, FLOAT8, INT2, OID, TEXT, and TID. For each supported type, it looks up the appropriate type characteristics (elmlen, elmbyval, elmalign) from hardcoded values and then delegates to the standard  function.

This function is particularly useful when working with system catalog arrays or other contexts where the element types are known to be built-in PostgreSQL types, simplifying the caller's code by eliminating type information management.

## Parameters / Member Variables
- : The PostgreSQL array object to deconstruct (must not be NULL)
- : The OID of the array element data type (must be a supported built-in type)
- : Output parameter, set to point to palloc'd array of Datum values
- : Output parameter, set to point to palloc'd array of null indicators (may be NULL)
- : Output parameter, set to the number of elements extracted

## Dependencies
- Functions called/Symbols referenced:
  - [deconstruct_array](deconstruct_array.md) (the main array deconstruction function)
  - TYPALIGN_CHAR (character alignment constant)
  - TYPALIGN_SHORT (short integer alignment constant)  
  - TYPALIGN_INT (integer alignment constant)
  - TYPALIGN_DOUBLE (double precision alignment constant)
  - FLOAT8PASSBYVAL (macro indicating if float8 is passed by value)
  - elog (error logging function)

- Called from (representative examples):
  - [transformRelOptions](../t/transformRelOptions.md) (relation options parsing)
  - [parseRelOptionsInternal](../p/parseRelOptionsInternal.md) (internal option parsing)
  - [textarray_to_strvaluelist](../t/textarray_to_strvaluelist.md) (text array to string list conversion)
  - [pg_get_object_address](../p/pg_get_object_address.md) (object address resolution)
  - [oid_array_to_list](../o/oid_array_to_list.md) (OID array to list conversion)
  - TidListEval (tuple ID list evaluation)
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md) (integer type modifier extraction)
  - [json_object](../j/json_object.md)/jsonb_object (JSON object construction)
  - [percentile_disc_multi_final](../p/percentile_disc_multi_final.md) (percentile aggregate finalization)

## Notes and Other Information
- Supports only a limited set of built-in types: CHAROID, CSTRINGOID, FLOAT8OID, INT2OID, OIDOID, TEXTOID, and TIDOID
- Throws an error for unsupported element types with message "type %u not supported by deconstruct_array_builtin()"
- The hardcoded type information includes:
  - CHAR: 1 byte, pass-by-value, character-aligned
  - CSTRING: variable length (-2), pass-by-reference, character-aligned
  - FLOAT8: 8 bytes, pass-by-value status depends on platform, double-aligned
  - INT2: 2 bytes, pass-by-value, short-aligned
  - OID: 4 bytes, pass-by-value, integer-aligned
  - TEXT: variable length (-1), pass-by-reference, integer-aligned
  - TID: ItemPointerData size, pass-by-reference, short-aligned
- This function is commonly used in system catalog manipulation and built-in function implementations
- The type characteristics are compile-time constants, making this function very efficient for supported types