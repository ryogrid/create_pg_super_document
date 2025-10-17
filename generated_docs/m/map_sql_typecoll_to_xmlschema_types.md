# map_sql_typecoll_to_xmlschema_types

## Location
[src/backend/utils/adt/xml.c:3856-3910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3856-L3910)

## Overview
Maps a collection of SQL data types to XML Schema data types according to SQL/XML:2008 section 9.7, generating XML Schema type definitions for all unique types used across multiple table descriptions.

## Definition
```c
static const char *
map_sql_typecoll_to_xmlschema_types(List *tupdesc_list)
```

## Detailed Description
This function analyzes a collection of tuple descriptors (table schemas) to extract all unique column types used across the tables, then generates XML Schema type definitions for each unique type. It performs deduplication to ensure each type appears only once in the output, and also includes base types for any domain types found.

The function operates in three phases:
1. Extracts all column types from the provided tuple descriptors, creating a list of unique type OIDs
2. For any domain types found, adds their base types to ensure complete type coverage
3. Converts each unique type to its corresponding XML Schema type definition

## Parameters / Member Variables
- `tupdesc_list`: List of TupleDesc structures representing table schemas to analyze for type extraction

## Dependencies
- Functions called/Symbols referenced:
  - [list_append_unique_oid](../l/list_append_unique_oid.md) (PostgreSQL list utility for maintaining unique OIDs)
  - [getBaseType](../g/getBaseType.md) (PostgreSQL function to resolve domain base types)
  - [map_sql_type_to_xmlschema_type](map_sql_type_to_xmlschema_type.md) (converts individual types to XML Schema)
  - TupleDescAttr (PostgreSQL macro to access tuple descriptor attributes)
- Called from (representative examples):
  - [schema_to_xmlschema_internal](../s/schema_to_xmlschema_internal.md)
  - [database_to_xmlschema_internal](../d/database_to_xmlschema_internal.md)
  - [map_sql_table_to_xmlschema](map_sql_table_to_xmlschema.md)

## Notes and Other Information
- Handles dropped columns by skipping them (checks att->attisdropped)
- Ensures comprehensive type coverage by including base types of domains
- Uses PostgreSQL's list_append_unique_oid to maintain type uniqueness automatically
- Each type definition is output on a separate line with newline separators
- The function is static and only used internally within the xml.c module
- Essential for generating complete XML Schema documents that cover all types used in a database structure
- Memory management relies on StringInfo for building the result string
- Processes tuple descriptors which are PostgreSQL's internal representation of table row structures

## Simplified Source

```c
static const char *map_sql_typecoll_to_xmlschema_types(List *tupdesc_list) {
    List *uniquetypes = NIL;
    StringInfoData result;
    ListCell *cell0;

    // Extract all column types from tuple descriptors
    foreach(cell0, tupdesc_list) {
        TupleDesc tupdesc = (TupleDesc) lfirst(cell0);

        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute att = TupleDescAttr(tupdesc, i);

            if (att->attisdropped) continue;
            uniquetypes = list_append_unique_oid(uniquetypes, att->atttypid);
        }
    }

    // Add base types for any domains found
    foreach(cell0, uniquetypes) {
        Oid typid = lfirst_oid(cell0);
        Oid basetypid = getBaseType(typid);

        if (basetypid != typid)
            uniquetypes = list_append_unique_oid(uniquetypes, basetypid);
    }

    // Convert each unique type to XML Schema type definition
    initStringInfo(&result);
    foreach(cell0, uniquetypes) {
        appendStringInfo(&result, "%s\n",
            map_sql_type_to_xmlschema_type(lfirst_oid(cell0), -1));
    }

    return result.data;
}
```