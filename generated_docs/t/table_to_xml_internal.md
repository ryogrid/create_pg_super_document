# table_to_xml_internal

## Location
[src/backend/utils/adt/xml.c:2868-2884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2868-L2884)

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
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [regclassout](../r/regclassout.md)
  - DirectFunctionCall1
  - [DatumGetCString](../D/DatumGetCString.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [query_to_xml_internal](../q/query_to_xml_internal.md)
  - [get_rel_name](../g/get_rel_name.md)
- Called from (representative examples):
  - [table_to_xml](table_to_xml.md)
  - [table_to_xml_and_xmlschema](table_to_xml_and_xmlschema.md)
  - [schema_to_xml_internal](../s/schema_to_xml_internal.md)

## Notes and Other Information
- This is a static function, only accessible within the xml.c file
- Part of the SQL/XML:2008 standard implementation for table data mapping
- Uses regclassout to safely convert relation OID to properly quoted relation name
- Constructs a simple "SELECT *" query to retrieve all table data
- Delegates actual XML formatting to query_to_xml_internal for consistency
- Supports both data-only and data-with-schema XML generation modes
- Handles various XML output formats through parameter configuration