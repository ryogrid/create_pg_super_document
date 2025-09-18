# table_to_xml

## Location
[src/backend/utils/adt/xml.c:2885-2898](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L2885-L2898)

## Overview
SQL/XML function that converts a PostgreSQL table to XML format, providing a standardized way to export table data as XML documents.

## Definition


## Detailed Description
The  function implements the SQL/XML:2008 standard for converting table data to XML format. It serves as a wrapper around the internal  function, extracting function arguments and delegating the actual XML generation logic. The function constructs a SELECT * query for the specified table and transforms the result set into an XML document according to the provided formatting options.

The function supports various XML formatting options including null value handling, table forest format (multiple root elements), and XML namespace specification. It's part of PostgreSQL's SQL/XML compliance features.

## Parameters / Member Variables
-  (Oid): Object identifier of the table to convert to XML
-  (bool): Whether to include NULL values in the XML output
-  (bool): Whether to generate XML in table forest format (multiple root elements)
-  (text): Target XML namespace for the generated XML document

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID
  - PG_GETARG_BOOL  
  - PG_GETARG_TEXT_PP
  - text_to_cstring
  - [table_to_xml_internal](table_to_xml_internal.md)
  - [stringinfo_to_xmltype](../s/stringinfo_to_xmltype.md)
  - PG_RETURN_XML_P
- Called from (representative examples):
  - No direct callers found (SQL function interface)

## Notes and Other Information
- Implements SQL/XML:2008 section 9.11 specification for table-to-XML mapping
- The function is exposed as a SQL function for direct use in queries
- Internally delegates to  which constructs a SELECT * query and uses  for actual XML generation
- Part of PostgreSQL's XML data type support and SQL/XML standard compliance
- Returns XML data type that can be further processed or exported