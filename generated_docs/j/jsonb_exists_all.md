# jsonb_exists_all

## Location
src/backend/utils/adt/jsonb_op.c: 79 - 111

## Overview
Tests whether all specified keys exist in a JSONB object or all specified string values exist as array elements.

## Definition


## Detailed Description
The jsonb_exists_all function implements the PostgreSQL '?&' operator for JSONB values. It checks whether all keys from a provided array of text values exist at the top level of a JSONB object, or whether all of the specified string values exist as elements in a JSONB array. The function returns false as soon as it fails to find a match for any of the provided keys/values.

Like other jsonb_exists variants, this function only performs top-level matching without recursion. For JSONB objects, it searches for object keys, and for JSONB arrays, it searches for string elements only.

## Parameters / Member Variables
-  (Jsonb *): The JSONB value to search in
-  (ArrayType *): Array of text values representing keys or string values that must all be present

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
- Returns false immediately upon failing to find any key/element (short-circuit evaluation)
- Skips null elements in the input array
- Only matches at the top level - no recursive search is performed
- For objects: matches against key names (which are always strings)
- For arrays: only matches string elements, not other data types
- Corresponds to the '?&' operator in PostgreSQL JSONB operations
- Requires all specified keys/values to be present for a true result
- More efficient than checking each key individually when all must be verified