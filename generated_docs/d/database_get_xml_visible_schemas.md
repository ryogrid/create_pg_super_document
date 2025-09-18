# database_get_xml_visible_schemas

## Location
src/backend/utils/adt/xml.c: 2842 - 2848

## Overview
Retrieves a list of schema OIDs that are visible to the current user within the current database for XML mapping operations.

## Definition
```c
static List *database_get_xml_visible_schemas(void)
```

## Detailed Description
This function returns all schemas (namespaces) in the current database that are accessible to the current user and suitable for inclusion in XML output. It excludes system schemas (those starting with 'pg_') and the information_schema. The function uses a predefined SQL query via the XML_VISIBLE_SCHEMAS macro that checks for USAGE privileges on each schema and filters out internal PostgreSQL schemas to provide only user-accessible schemas.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [query_to_oid_list](../q/query_to_oid_list.md)
  - XML_VISIBLE_SCHEMAS (macro that expands to: "SELECT oid FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege (oid, 'USAGE') AND NOT (nspname ~ '^pg_' OR nspname = 'information_schema')")
- Called from (representative examples):
  - [database_to_xml_internal](database_to_xml_internal.md)
  - [database_to_xmlschema_internal](database_to_xmlschema_internal.md)

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for database-level XML mapping
- Excludes system schemas using regex pattern '^pg_' and explicit 'information_schema' check
- Uses PostgreSQL's privilege system (has_schema_privilege) with 'USAGE' permission
- Results are ordered by schema name (nspname) for consistent output
- Only includes schemas where the user has USAGE privileges