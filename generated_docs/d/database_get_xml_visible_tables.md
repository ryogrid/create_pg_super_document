# database_get_xml_visible_tables

## Location
src/backend/utils/adt/xml.c: 2849 - 2867

## Overview
Retrieves a list of table OIDs that are visible to the current user across all accessible schemas within the current database for XML mapping operations.

## Definition
```c
static List *database_get_xml_visible_tables(void)
```

## Detailed Description
This function returns all tables, materialized views, and views in the current database that are accessible to the current user and located within user-accessible schemas. It combines table-level privileges (SELECT permission) with schema-level visibility constraints by incorporating the XML_VISIBLE_SCHEMAS filter. The function constructs a comprehensive SQL query that ensures both the table itself and its containing schema are accessible to the current user, making it suitable for database-wide XML mapping operations.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - query_to_oid_list
  - RELKIND_RELATION
  - RELKIND_MATVIEW
  - RELKIND_VIEW
  - CppAsString2
  - XML_VISIBLE_SCHEMAS (macro for schema visibility filter)
- Called from (representative examples):
  - database_to_xmlschema_internal

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for database-level XML schema mapping
- Filters tables by three relation kinds: regular tables, materialized views, and views
- Uses PostgreSQL's privilege system (has_table_privilege) with 'SELECT' permission
- Restricts results to tables within schemas that meet XML visibility criteria
- Currently no specific ordering is applied to results (as noted in the comment)
- Combines table-level and schema-level access controls for comprehensive security