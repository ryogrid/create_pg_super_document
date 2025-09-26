# convert_foreign_data_wrapper_name

## Location
[src/backend/utils/adt/acl.c:3356-3367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L3356-L3367)

## Overview
Converts a foreign data wrapper name (text) to its corresponding object identifier (Oid) for privilege checking operations.

## Definition
static Oid convert_foreign_data_wrapper_name(text *fdwname)

## Detailed Description
This internal helper function is part of the foreign data wrapper privilege checking infrastructure. It takes a text representation of a foreign data wrapper name and converts it to the corresponding Oid by performing a catalog lookup. The function serves as a bridge between the text-based privilege checking functions and the internal Oid-based representation used by PostgreSQL's catalog system.

The function is straightforward: it converts the text parameter to a C string and then uses the catalog lookup function to find the corresponding Oid. If the foreign data wrapper doesn't exist, the underlying catalog function will handle the error reporting.

## Parameters / Member Variables
- : A text pointer containing the name of the foreign data wrapper to look up

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md): Converts PostgreSQL text type to C string
  - [get_foreign_data_wrapper_oid](../g/get_foreign_data_wrapper_oid.md): Looks up foreign data wrapper Oid by name
- Called from (representative examples):
  - [has_foreign_data_wrapper_privilege_name_name](../h/has_foreign_data_wrapper_privilege_name_name.md): Privilege checking with role and FDW names
  - [has_foreign_data_wrapper_privilege_name](../h/has_foreign_data_wrapper_privilege_name.md): Privilege checking with role Oid and FDW name
  - [has_foreign_data_wrapper_privilege_id_name](../h/has_foreign_data_wrapper_privilege_id_name.md): Privilege checking with role Oid and FDW name

## Notes and Other Information
- This is a static function, meaning it's only accessible within the acl.c compilation unit
- Part of the support routines for the has_foreign_data_wrapper_privilege family of functions
- The function assumes the input text is valid and relies on get_foreign_data_wrapper_oid for error handling
- Uses the  parameter for get_foreign_data_wrapper_oid, meaning it will throw an error if the FDW doesn't exist rather than returning InvalidOid