# convert_schema_name

## Location
src/backend/utils/adt/acl.c: 3965 - 3976

## Overview
Converts a schema name expressed as text to its corresponding Object Identifier (OID).

## Definition
static Oid convert_schema_name(text *schemaname)

## Detailed Description
This function is a static helper utility used by the has_schema_privilege family of functions to convert schema names from text representation to their corresponding Object Identifiers (OIDs). The function takes a PostgreSQL text type parameter, converts it to a C string, and then uses the system catalog to look up the namespace OID. This is an essential step in privilege checking functions that need to work with schema names rather than OIDs.

The function will raise an error if the specified schema does not exist, as it calls get_namespace_oid with the error_on_missing parameter set to false, which actually means it WILL error if missing (contrary to the parameter name).

## Parameters / Member Variables
- schemaname (text*): PostgreSQL text type containing the name of the schema to look up

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring
  - get_namespace_oid
- Called from (representative examples):
  - has_schema_privilege_name_name
  - has_schema_privilege_name
  - has_schema_privilege_id_name

## Notes and Other Information
- This is a static function, only accessible within the acl.c compilation unit
- Part of the support routines for the has_schema_privilege family of functions
- Will raise an error if the schema does not exist rather than returning InvalidOid
- The conversion from text to C string is necessary for compatibility with catalog lookup functions
- Located in src/backend/utils/adt/acl.c:3965-3976