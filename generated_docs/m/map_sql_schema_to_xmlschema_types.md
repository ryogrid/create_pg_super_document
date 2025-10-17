# map_sql_schema_to_xmlschema_types

## Location
[src/backend/utils/adt/xml.c:3621-3693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L3621-L3693)

## Overview
Maps an SQL schema to XML Schema data types according to the SQL/XML:2008 section 9.12 specification, generating XML Schema complex type definitions for database schemas.

## Definition

```c
static const char *
map_sql_schema_to_xmlschema_types(Oid nspid, List *relid_list, bool nulls,
								  bool tableforest, const char *targetns)
```
## Detailed Description
This function generates XML Schema definitions for a PostgreSQL schema by creating complex type definitions that represent the schema structure. It processes a list of relation IDs within the specified namespace and generates appropriate XML Schema elements and types. The function creates a schema-level complex type that contains elements for each table/relation in the schema.

The function supports two output formats controlled by the  parameter:
- When  is false: Uses  to allow elements in any order
- When  is true: Uses  with  and  for repeated elements

## Parameters / Member Variables
- `nspid`: The OID of the PostgreSQL namespace (schema) to map
- `*relid_list`: List of relation OIDs within the schema to include in the mapping
- `nulls`: Boolean flag indicating whether to handle NULL values (currently not used in implementation)
- `tableforest`: Boolean flag controlling output format - true for forest-style with sequences, false for all-style
- `*targetns`: Target namespace for the XML Schema (currently not actively used in the function body)
## Dependencies
- Functions called/Symbols referenced:
  - [get_database_name](../g/get_database_name.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [map_sql_identifier_to_xml_name](map_sql_identifier_to_xml_name.md)
  - [map_multipart_sql_identifier_to_xml_name](map_multipart_sql_identifier_to_xml_name.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [schema_to_xmlschema_internal](../s/schema_to_xmlschema_internal.md)

## Notes and Other Information
- The function is static and only used internally within the xml.c module
- Implements the SQL/XML:2008 standard for schema-to-XML mapping
- Creates both complex type definitions and root element declarations
- The generated XML Schema uses the "xsd" namespace prefix for XML Schema constructs
- Memory management relies on StringInfo for building the result string
- The function constructs hierarchical type names using the database name, schema name, and relation names

## Simplified Source

```c
static const char *map_sql_schema_to_xmlschema_types(Oid nspid, List *relid_list, bool nulls,
                                                   bool tableforest, const char *targetns) {
    StringInfoData result;
    initStringInfo(&result);

    // Get database and namespace names
    char *dbname = get_database_name(MyDatabaseId);
    char *nspname = get_namespace_name(nspid);
    char *xmlsn = map_sql_identifier_to_xml_name(nspname, true, false);
    char *schematypename = map_multipart_sql_identifier_to_xml_name("SchemaType", dbname, nspname, NULL);

    // Create complex type for schema
    appendStringInfo(&result, "<xsd:complexType name=\"%s\">\n", schematypename);

    // Choose container based on format
    if (!tableforest)
        appendStringInfoString(&result, "  <xsd:all>\n");
    else
        appendStringInfoString(&result, "  <xsd:sequence>\n");

    // Add elements for each relation in the schema
    ListCell *cell;
    foreach(cell, relid_list) {
        Oid relid = lfirst_oid(cell);
        char *relname = get_rel_name(relid);
        char *xmltn = map_sql_identifier_to_xml_name(relname, true, false);
        char *tabletypename = map_multipart_sql_identifier_to_xml_name(
            tableforest ? "RowType" : "TableType", dbname, nspname, relname);

        if (!tableforest)
            appendStringInfo(&result, "    <xsd:element name=\"%s\" type=\"%s\"/>\n", xmltn, tabletypename);
        else
            appendStringInfo(&result,
                "    <xsd:element name=\"%s\" type=\"%s\" minOccurs=\"0\" maxOccurs=\"unbounded\"/>\n",
                xmltn, tabletypename);
    }

    // Close container and complex type
    if (!tableforest)
        appendStringInfoString(&result, "  </xsd:all>\n");
    else
        appendStringInfoString(&result, "  </xsd:sequence>\n");
    appendStringInfoString(&result, "</xsd:complexType>\n\n");

    // Add root element declaration
    appendStringInfo(&result, "<xsd:element name=\"%s\" type=\"%s\"/>\n\n", xmlsn, schematypename);

    return result.data;
}
```