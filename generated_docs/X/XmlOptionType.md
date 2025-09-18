# XmlOptionType

## Location
src/include/nodes/primnodes.h: 1594 - 1595

## Overview
XmlOptionType is an enumeration that defines the parsing modes for XML content, distinguishing between document and content parsing options in PostgreSQL's XML functionality.

## Definition
```c
typedef enum XmlOptionType
{
    XMLOPTION_DOCUMENT,
    XMLOPTION_CONTENT,
} XmlOptionType;
```

## Detailed Description
XmlOptionType specifies the XML parsing mode that determines how XML text should be interpreted during parsing operations. The enumeration provides two fundamental modes: DOCUMENT mode requires the XML to be a well-formed complete document with a single root element, while CONTENT mode allows XML fragments that may contain multiple top-level elements or text nodes. This distinction is important for XML parsing functions and determines validation rules and processing behavior.

## Parameters / Member Variables
- `XMLOPTION_DOCUMENT`: Specifies document parsing mode, requiring well-formed complete XML documents with a single root element
- `XMLOPTION_CONTENT`: Specifies content parsing mode, allowing XML fragments with multiple top-level elements or mixed content

## Dependencies
- Functions called/Symbols referenced:
  - None (this is an enumeration)
- Called from (representative examples):
  - [PgXmlErrorContext](../P/PgXmlErrorContext.md) (XML error handling context)
  - [xmltotext_with_options](../x/xmltotext_with_options.md) (XML to text conversion with options)
  - [xmlparse](../x/xmlparse.md) (XML parsing function)
  - xml_parse (Core XML parsing functionality)
  - [wellformed_xml](../w/wellformed_xml.md) (XML well-formedness checking)
  - [XmlSerialize](XmlSerialize.md) (XML serialization structure)
  - XmlExpr (XML expression structure)
  - PG_RETURN_XML_P (XML return macro)

## Notes and Other Information
- The enumeration is fundamental to PostgreSQL's XML processing and affects parsing validation
- DOCUMENT mode enforces stricter XML standards requiring a single root element
- CONTENT mode is more permissive and allows XML fragments commonly used in data exchange
- The option type is used throughout the XML processing pipeline from parsing to serialization
- Different XML functions may have different default behaviors based on their intended use case
- The choice between DOCUMENT and CONTENT modes affects both parsing performance and validation strictness