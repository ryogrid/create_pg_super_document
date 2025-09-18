# convert_column_name

## Location
src/backend/utils/adt/acl.c: 2898 - 2955

## Overview
A static helper function that converts a column name (as text) to its corresponding attribute number for a given table, with special handling for dropped columns and error cases.

## Definition


## Detailed Description
This function serves as a support routine for the has_column_privilege family of functions. It takes a table OID and a column name as a text string and returns the corresponding attribute number (AttrNumber). The function performs a direct lookup in the system catalog (pg_attribute) using SearchSysCache2 rather than using get_attnum() because it needs to distinguish between dropped columns and nonexistent columns. For dropped columns (where attisdropped is true), it returns InvalidAttrNumber, allowing the caller to return NULL instead of failing. If the column doesn't exist but the table does, it throws an ERRCODE_UNDEFINED_COLUMN error. If the table itself doesn't exist (get_rel_name returns NULL), it returns InvalidAttrNumber to allow graceful handling by the caller.

## Parameters / Member Variables
- : Object identifier (OID) of the table containing the column
- : Text string representing the name of the column to resolve

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - SearchSysCache2
  - CStringGetDatum
  - ObjectIdGetDatum
  - GETSTRUCT
  - ReleaseSysCache
  - get_rel_name
  - ereport
  - pfree
  - InvalidAttrNumber
- Called from (representative examples):
  - has_column_privilege_name_name
  - has_column_privilege_id_name
  - Various other has_column_privilege variants

## Notes and Other Information
- Returns InvalidAttrNumber (rather than throwing an error) for dropped columns to allow privilege functions to return NULL
- Uses direct system catalog lookup instead of get_attnum() to handle dropped columns appropriately  
- Distinguishes between nonexistent columns (error) and dropped columns (return InvalidAttrNumber)
- If the table OID is invalid or the table has been dropped, returns InvalidAttrNumber rather than erroring
- Memory management: properly frees the converted C string using pfree()
- Part of the internal support infrastructure for PostgreSQL's column privilege checking system
- Located in src/backend/utils/adt/acl.c:2898-2955