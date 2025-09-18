# table_to_xml_internal

## Location
src/backend/utils/adt/xml.c: 2868 - 2884

## Overview
Internal function that converts the contents of a PostgreSQL table to XML format according to SQL/XML:2008 section 9.11 specifications.

## Definition
```c
static StringInfo table_to_xml_internal(Oid relid, const char *xmlschema, bool nulls, bool tableforest, const char *targetns, bool top_level)
```

## Detailed Description
This function serves as the core table-to-XML conversion mechanism in PostgreSQL's SQL/XML implementation. It constructs a SELECT * query for the specified relation and delegates the actual XML generation to query_to_xml_internal. The function handles the mapping of SQL table data to XML documents with support for various XML formatting options including schema inclusion, null value handling, table forest format, and namespace targeting. It uses the relation's OID to build a proper SQL query and retrieves the relation name for XML element naming.

## Parameters / Member Variables
- `relid`: The OID of the relation (table/view) to convert to XML
- `xmlschema`: Optional XML Schema to include in the output (NULL if not needed)
- `nulls`: Boolean flag indicating whether to include NULL values in XML output
- `tableforest`: Boolean flag for table forest format (affects XML structure)
- `targetns`: Target namespace for the XML elements (NULL for default)
- `top_level`: Boolean flag indicating if this is a top-level conversion

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - appendStringInfo
  - regclassout
  - DirectFunctionCall1
  - DatumGetCString
  - ObjectIdGetDatum
  - query_to_xml_internal
  - get_rel_name
- Called from (representative examples):
  - table_to_xml
  - table_to_xml_and_xmlschema
  - schema_to_xml_internal

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for table data mapping
- Uses regclassout to safely convert relation OID to properly quoted relation name
- Constructs a simple "SELECT *" query to retrieve all table data
- Delegates actual XML formatting to query_to_xml_internal for consistency
- Supports both data-only and data-with-schema XML generation modes
- Handles various XML output formats through parameter configuration