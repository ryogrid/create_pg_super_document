# query_to_oid_list

## Location
[src/backend/utils/adt/xml.c:2786-2814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2786-L2814)

## Overview
Executes a SQL query that returns OID values and converts the results into a List of Oids for use in XML mapping operations.

## Definition

```c
static List *
query_to_oid_list(const char *query)
```
## Detailed Description
This function serves as a utility for XML mapping operations in PostgreSQL, specifically designed to execute SQL queries that return object identifiers (OIDs) as their first column. It uses the Server Programming Interface (SPI) to execute the provided query and collects all non-null OID values from the result set into a PostgreSQL List data structure. The function is part of the SQL to XML mapping infrastructure that supports visibility determination for database objects in XML output generation.

## Parameters / Member Variables
- : A SQL query string that must return type oid as the first column

## Dependencies
- Functions called/Symbols referenced:
  - [SPI_execute](../S/SPI_execute.md)
  - SPI_OK_SELECT
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - SPI_getbinval
  - lappend_oid
  - [DatumGetObjectId](../D/DatumGetObjectId.md)
- Called from (representative examples):
  - [schema_get_xml_visible_tables](../s/schema_get_xml_visible_tables.md)
  - [database_get_xml_visible_schemas](../d/database_get_xml_visible_schemas.md)
  - [database_get_xml_visible_tables](../d/database_get_xml_visible_tables.md)

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for object visibility
- Uses SPI (Server Programming Interface) to execute queries within the backend
- Filters out NULL values from the result set
- Returns NIL (empty list) if no valid OIDs are found
- Error handling includes logging with specific SPI result codes