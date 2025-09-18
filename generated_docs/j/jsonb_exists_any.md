# jsonb_exists_any

## Location
src/backend/utils/adt/jsonb_op.c: 46 - 78

## Overview
Tests whether any of the specified keys exist in a JSONB object or any of the specified string values exist as array elements.

## Definition


## Detailed Description
The jsonb_exists_any function implements the PostgreSQL '?|' operator for JSONB values. It checks whether any key from a provided array of text values exists at the top level of a JSONB object, or whether any of the specified string values exist as elements in a JSONB array. The function returns true as soon as it finds a match for any of the provided keys/values.

Like jsonb_exists, this function only performs top-level matching without recursion. For JSONB objects, it searches for object keys, and for JSONB arrays, it searches for string elements only.

## Parameters / Member Variables
-  (Jsonb *): The JSONB value to search in
-  (ArrayType *): Array of text values representing keys or string values to search for

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_ARRAYTYPE_P
  - deconstruct_array_builtin
  - findJsonbValueFromContainer
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - PG_RETURN_BOOL
- Types used:
  - Jsonb
  - ArrayType
  - JsonbValue
  - jbvString
- Constants used:
  - TEXTOID
  - JB_FOBJECT
  - JB_FARRAY

## Notes and Other Information
- Returns true immediately upon finding the first matching key/element (short-circuit evaluation)
- Skips null elements in the input array
- Only matches at the top level - no recursive search is performed
- For objects: matches against key names (which are always strings)
- For arrays: only matches string elements, not other data types
- Corresponds to the '?|' operator in PostgreSQL JSONB operations
- More efficient than checking each key individually when testing multiple possibilities