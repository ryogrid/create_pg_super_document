# schema_get_xml_visible_tables

## Location
[src/backend/utils/adt/xml.c:2815-2835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2815-L2835)

## Overview
Retrieves a list of table OIDs visible to the current user within a specific schema for XML mapping operations.

## Definition
```c
static List *schema_get_xml_visible_tables(Oid nspid)
```

## Detailed Description
This function constructs and executes a SQL query to find all tables, materialized views, and views within a specified schema that the current user has SELECT privileges on. It serves as part of the SQL to XML mapping infrastructure, ensuring that only accessible database objects are included in XML output generation. The function builds a dynamic SQL query using StringInfo utilities and leverages the query_to_oid_list helper function to convert the results into a List of OIDs.

## Parameters / Member Variables
- `nspid`: The OID of the namespace (schema) to query for visible tables

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - appendStringInfo
  - RELKIND_RELATION
  - RELKIND_MATVIEW
  - RELKIND_VIEW
  - CppAsString2
  - [query_to_oid_list](../q/query_to_oid_list.md)
- Called from (representative examples):
  - [schema_to_xml_internal](schema_to_xml_internal.md)
  - [schema_to_xmlschema_internal](schema_to_xmlschema_internal.md)

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for object visibility
- Filters tables by three relation kinds: regular tables, materialized views, and views
- Uses PostgreSQL's privilege system (has_table_privilege) to ensure security
- Results are ordered by relation name for consistent output
- Only includes objects where the user has SELECT privileges