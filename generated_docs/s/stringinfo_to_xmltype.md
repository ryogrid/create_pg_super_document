# stringinfo_to_xmltype

## Location
[src/backend/utils/adt/xml.c:467-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L467-L473)

## Overview
Converts a StringInfo buffer containing XML data into a PostgreSQL xmltype value.

## Definition

```c
static xmltype *
stringinfo_to_xmltype(StringInfo buf)
```
## Detailed Description
The stringinfo_to_xmltype function is a utility function that converts the contents of a StringInfo buffer into a PostgreSQL xmltype value. This function serves as a bridge between PostgreSQL's string building infrastructure and the XML data type system.

The function works by extracting both the data pointer and length from the StringInfo structure and passing them to cstring_to_text_with_len, which creates a text value. Since xmltype is internally represented as text in PostgreSQL, this cast operation is safe and efficient. The function preserves the exact length of the buffer contents, including any embedded null bytes that might be present in the XML data.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the XML data to be converted to xmltype

## Dependencies
- Functions called/Symbols referenced:
  - cstring_to_text_with_len (creates text value from C string with specified length)
  - [xmltype](../x/xmltype.md) (PostgreSQL XML data type)
- Called from:
  - [xmlcomment](../x/xmlcomment.md) (XML comment creation)
  - [xmlconcat](../x/xmlconcat.md) (XML concatenation)
  - [xmlpi](../x/xmlpi.md) (XML processing instruction creation)
  - [xmlroot](../x/xmlroot.md) (XML root element creation)
  - [table_to_xml](../t/table_to_xml.md) (table to XML conversion)
  - [query_to_xml](../q/query_to_xml.md) (query result to XML conversion)
  - [cursor_to_xml](../c/cursor_to_xml.md) (cursor to XML conversion)
  - [table_to_xml_and_xmlschema](../t/table_to_xml_and_xmlschema.md) (table to XML with schema)
  - [query_to_xml_and_xmlschema](../q/query_to_xml_and_xmlschema.md) (query to XML with schema)
  - [schema_to_xml](schema_to_xml.md) (schema to XML conversion)
  - [schema_to_xmlschema](schema_to_xmlschema.md) (schema to XML schema)
  - [schema_to_xml_and_xmlschema](schema_to_xml_and_xmlschema.md) (schema to XML and schema)
  - [database_to_xml](../d/database_to_xml.md) (database to XML conversion)
  - [database_to_xmlschema](../d/database_to_xmlschema.md) (database to XML schema)
  - [database_to_xml_and_xmlschema](../d/database_to_xml_and_xmlschema.md) (database to XML and schema)

## Notes and Other Information
- This is a static function, only available within the xml.c compilation unit
- The function assumes the StringInfo buffer contains valid XML data
- No validation of XML content is performed by this function
- The conversion preserves binary data exactly, including the length
- Widely used throughout PostgreSQL's XML generation functions
- The function is essentially a type cast from StringInfo to xmltype