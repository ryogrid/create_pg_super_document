# database_to_xml_and_xmlschema

## Location
[src/backend/utils/adt/xml.c:3466-3484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3466-L3484)

## Overview
A PostgreSQL SQL-callable function that generates both XML data and an embedded XML Schema definition for all visible tables and schemas in the current database.

## Definition
```c
Datum
database_to_xml_and_xmlschema(PG_FUNCTION_ARGS)
```

## Detailed Description
This function combines XML data generation with XML Schema definition creation for the entire database. It first generates the XML Schema using the internal schema generation function, then uses that schema to produce XML data that includes both the schema definition and the actual database content. This provides a complete, self-describing XML document that contains both the structure definition and the data itself.

## Parameters / Member Variables
The function receives arguments through PG_FUNCTION_ARGS containing:
- Argument 0: `nulls` (boolean) - Whether to include nullable elements in both schema and data
- Argument 1: `tableforest` (boolean) - Controls XML structure format (table forest vs. single table)
- Argument 2: `targetns` (text) - Target namespace for both the XML Schema and XML data

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - [text_to_cstring](../t/text_to_cstring.md)
  - [database_to_xmlschema_internal](database_to_xmlschema_internal.md)
  - [database_to_xml_internal](database_to_xml_internal.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from:
  - No direct references (SQL-callable function)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL
- Combines schema generation and data export functionality
- The generated XML includes embedded schema information for validation
- More comprehensive than separate schema or data-only functions
- Useful for complete database export scenarios where both structure and data are needed
- The schema is generated first and passed to the XML data generation function

## Simplified Source

```c
Datum database_to_xml_and_xmlschema(PG_FUNCTION_ARGS) {
    // Extract function arguments
    bool nulls = PG_GETARG_BOOL(0);
    bool tableforest = PG_GETARG_BOOL(1);
    const char *targetns = text_to_cstring(PG_GETARG_TEXT_PP(2));

    // Generate XML schema for the database
    StringInfo xmlschema = database_to_xmlschema_internal(nulls, tableforest, targetns);

    // Generate XML data with embedded schema and return result
    return PG_RETURN_XML_P(stringinfo_to_xmltype(
        database_to_xml_internal(xmlschema->data, nulls, tableforest, targetns)));
}
```